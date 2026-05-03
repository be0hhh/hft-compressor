#include "codecs/depth_ladder_offset/DepthLadderOffsetV2.hpp"

#include <algorithm>
#include <charconv>
#include <fstream>
#include <numeric>
#include <sstream>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <vector>

#include "common/CompressionInternals.hpp"
#include "common/timing.hpp"
#include "hft_compressor/metrics.hpp"

namespace hft_compressor::codecs::depth_ladder_offset_v2 {
namespace {

constexpr std::uint32_t kMagic = 0x32454443u; // CDE2
constexpr std::uint16_t kVersion = 2u;
constexpr std::size_t kHeaderBytes = 176u;
constexpr std::uint32_t kHotQtyCount = 64u;
constexpr std::uint32_t kInspectBatchLimit = 128u;

struct Level { std::int64_t price{0}, qty{0}, side{0}; };
struct Batch { std::int64_t ts{0}; std::vector<Level> levels; };
struct Header {
    std::uint32_t magic{kMagic}; std::uint16_t version{kVersion}; std::uint16_t bidHotCount{0};
    std::uint64_t inputBytes{0}, outputBytes{0}, batchCount{0}, levelCount{0};
    std::int64_t timeScale{1}, priceScale{1}, qtyScale{1}, baseTsUnit{0};
    std::uint32_t hotQtyCount{0}, hotQtyBytes{0};
    std::uint32_t batchBytes{0}, sideBytes{0}, priceModeBytes{0}, deleteBytes{0}, priceBytes{0}, qtyCodeBytes{0}, qtyEscapeBytes{0};
    std::uint32_t deleteCount{0}, qtyEscapeCount{0}, runModeBatchCount{0}, fallbackBatchCount{0};
    std::uint32_t offsetPriceCount{0}, absolutePriceCount{0};
};

struct Cursor {
    std::string_view text{}; std::size_t pos{0};
    void ws() noexcept { while (pos < text.size() && (text[pos] == ' ' || text[pos] == '\t' || text[pos] == '\r')) ++pos; }
    bool ch(char c) noexcept { ws(); if (pos >= text.size() || text[pos] != c) return false; ++pos; return true; }
    bool peek(char c) noexcept { ws(); return pos < text.size() && text[pos] == c; }
    bool i64(std::int64_t& out) noexcept { ws(); const char* b = text.data() + pos; const char* e = text.data() + text.size(); const auto [p, ec] = std::from_chars(b, e, out); if (ec != std::errc{} || p == b) return false; pos = static_cast<std::size_t>(p - text.data()); return true; }
    bool end() noexcept { ws(); return pos == text.size(); }
};

bool validSide(std::int64_t side) noexcept { return side == 0 || side == 1; }

bool parseLine(std::string_view line, Batch& out) noexcept {
    Cursor p{line};
    if (!p.ch('[') || !p.peek('[')) return false;
    while (p.peek('[')) {
        Level level{};
        if (!p.ch('[') || !p.i64(level.price) || !p.ch(',') || !p.i64(level.qty) || !p.ch(',') || !p.i64(level.side) || !validSide(level.side) || !p.ch(']') || !p.ch(',')) return false;
        out.levels.push_back(level);
    }
    return p.i64(out.ts) && p.ch(']') && p.end() && !out.levels.empty();
}

bool parseBatches(std::span<const std::uint8_t> input, std::vector<Batch>& out) {
    std::int64_t previousTs = 0; bool havePrevious = false; std::size_t start = 0;
    while (start < input.size()) {
        std::size_t end = start;
        while (end < input.size() && input[end] != static_cast<std::uint8_t>('\n')) ++end;
        std::string_view line{reinterpret_cast<const char*>(input.data() + start), end - start};
        if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
        if (line.empty()) return false;
        Batch batch{};
        if (!parseLine(line, batch)) return false;
        if (havePrevious && batch.ts < previousTs) return false;
        previousTs = batch.ts; havePrevious = true; out.push_back(std::move(batch));
        start = end + (end < input.size() ? 1u : 0u);
    }
    return !out.empty();
}

std::int64_t gcdAbs(std::int64_t a, std::int64_t b) noexcept { a = a < 0 ? -a : a; b = b < 0 ? -b : b; return std::gcd(a, b); }
std::int64_t safeScale(std::int64_t value) noexcept { return value == 0 ? 1 : (value < 0 ? -value : value); }
std::uint64_t zigzag(std::int64_t value) noexcept { return value < 0 ? static_cast<std::uint64_t>(-value) * 2u - 1u : static_cast<std::uint64_t>(value) * 2u; }
std::int64_t unzigzag(std::uint64_t value) noexcept { return (value & 1u) != 0u ? -static_cast<std::int64_t>((value + 1u) / 2u) : static_cast<std::int64_t>(value / 2u); }

void writeVarint(std::vector<std::uint8_t>& out, std::uint64_t value) {
    while (value >= 0x80u) { out.push_back(static_cast<std::uint8_t>(value | 0x80u)); value >>= 7u; }
    out.push_back(static_cast<std::uint8_t>(value));
}

bool readVarint(const std::uint8_t*& p, const std::uint8_t* end, std::uint64_t& out) noexcept {
    out = 0; unsigned shift = 0;
    while (p < end && shift <= 63u) {
        const auto byte = *p++;
        out |= static_cast<std::uint64_t>(byte & 0x7fu) << shift;
        if ((byte & 0x80u) == 0u) return true;
        shift += 7u;
    }
    return false;
}

template <class T> void writeLe(std::vector<std::uint8_t>& out, T value) {
    using U = std::make_unsigned_t<T>; U raw = static_cast<U>(value);
    for (std::size_t i = 0; i < sizeof(T); ++i) out.push_back(static_cast<std::uint8_t>((raw >> (i * 8u)) & 0xffu));
}

template <class T> bool readLe(const std::uint8_t*& p, const std::uint8_t* end, T& out) noexcept {
    if (static_cast<std::size_t>(end - p) < sizeof(T)) return false;
    using U = std::make_unsigned_t<T>; U raw = 0;
    for (std::size_t i = 0; i < sizeof(T); ++i) raw |= static_cast<U>(*p++) << (i * 8u);
    out = static_cast<T>(raw); return true;
}
struct BitWriter { std::vector<std::uint8_t> bytes; std::uint8_t current{0}; unsigned used{0}; void bit(bool value) { if (value) current |= static_cast<std::uint8_t>(1u << used); if (++used == 8u) { bytes.push_back(current); current = 0; used = 0; } } std::vector<std::uint8_t> finish() { if (used != 0u) bytes.push_back(current); return bytes; } };
struct BitReader { const std::uint8_t* p{}; const std::uint8_t* end{}; std::uint8_t current{0}; unsigned used{8}; bool bit(bool& out) noexcept { if (used == 8u) { if (p >= end) return false; current = *p++; used = 0; } out = ((current >> used) & 1u) != 0u; ++used; return true; } };

struct BookState {
    std::unordered_map<std::int64_t, std::int64_t> bid, ask;
    bool haveBid{false}, haveAsk{false}; std::int64_t bestBid{0}, bestAsk{0};
    void recompute() { haveBid = !bid.empty(); haveAsk = !ask.empty(); if (haveBid) { bestBid = bid.begin()->first; for (const auto& item : bid) bestBid = std::max(bestBid, item.first); } if (haveAsk) { bestAsk = ask.begin()->first; for (const auto& item : ask) bestAsk = std::min(bestAsk, item.first); } }
    void apply(std::int64_t side, std::int64_t price, std::int64_t qty) { auto& book = side == 0 ? bid : ask; if (qty == 0) book.erase(price); else book[price] = qty; }
};

std::vector<std::uint8_t> serializeHeader(const Header& h, const std::vector<std::int64_t>& hot) {
    std::vector<std::uint8_t> out; out.reserve(kHeaderBytes + hot.size() * sizeof(std::int64_t));
    writeLe(out, h.magic); writeLe(out, h.version); writeLe(out, h.bidHotCount);
    writeLe(out, h.inputBytes); writeLe(out, h.outputBytes); writeLe(out, h.batchCount); writeLe(out, h.levelCount);
    writeLe(out, h.timeScale); writeLe(out, h.priceScale); writeLe(out, h.qtyScale); writeLe(out, h.baseTsUnit);
    writeLe(out, h.hotQtyCount); writeLe(out, h.hotQtyBytes); writeLe(out, h.batchBytes); writeLe(out, h.sideBytes); writeLe(out, h.priceModeBytes); writeLe(out, h.deleteBytes);
    writeLe(out, h.priceBytes); writeLe(out, h.qtyCodeBytes); writeLe(out, h.qtyEscapeBytes);
    writeLe(out, h.deleteCount); writeLe(out, h.qtyEscapeCount); writeLe(out, h.runModeBatchCount); writeLe(out, h.fallbackBatchCount); writeLe(out, h.offsetPriceCount); writeLe(out, h.absolutePriceCount);
    out.resize(kHeaderBytes, 0);
    for (const auto qty : hot) writeLe(out, qty);
    return out;
}

bool readHeader(std::span<const std::uint8_t> data, Header& h) noexcept {
    if (data.size() < kHeaderBytes) return false;
    const auto* p = data.data(); const auto* end = data.data() + kHeaderBytes;
    return readLe(p, end, h.magic) && readLe(p, end, h.version) && readLe(p, end, h.bidHotCount)
        && readLe(p, end, h.inputBytes) && readLe(p, end, h.outputBytes) && readLe(p, end, h.batchCount) && readLe(p, end, h.levelCount)
        && readLe(p, end, h.timeScale) && readLe(p, end, h.priceScale) && readLe(p, end, h.qtyScale) && readLe(p, end, h.baseTsUnit)
        && readLe(p, end, h.hotQtyCount) && readLe(p, end, h.hotQtyBytes) && readLe(p, end, h.batchBytes) && readLe(p, end, h.sideBytes) && readLe(p, end, h.priceModeBytes) && readLe(p, end, h.deleteBytes)
        && readLe(p, end, h.priceBytes) && readLe(p, end, h.qtyCodeBytes) && readLe(p, end, h.qtyEscapeBytes)
        && readLe(p, end, h.deleteCount) && readLe(p, end, h.qtyEscapeCount) && readLe(p, end, h.runModeBatchCount) && readLe(p, end, h.fallbackBatchCount) && readLe(p, end, h.offsetPriceCount) && readLe(p, end, h.absolutePriceCount)
        && h.magic == kMagic && h.version == kVersion && h.hotQtyCount <= kHotQtyCount * 2u && h.bidHotCount <= h.hotQtyCount && h.hotQtyBytes == h.hotQtyCount * sizeof(std::int64_t);
}

bool take(std::span<const std::uint8_t> data, std::size_t& offset, std::uint32_t size, std::span<const std::uint8_t>& out) noexcept { if (offset + size > data.size()) return false; out = data.subspan(offset, size); offset += size; return true; }

std::vector<std::int64_t> buildHotQty(const std::unordered_map<std::int64_t, std::uint32_t>& counts, std::int64_t qtyScale) {
    std::vector<std::pair<std::int64_t, std::uint32_t>> values(counts.begin(), counts.end());
    std::sort(values.begin(), values.end(), [](const auto& lhs, const auto& rhs) { return lhs.second == rhs.second ? lhs.first < rhs.first : lhs.second > rhs.second; });
    std::vector<std::int64_t> out;
    for (std::size_t i = 0; i < values.size() && i < kHotQtyCount; ++i) out.push_back(values[i].first / qtyScale);
    return out;
}

std::uint64_t qtyCode(std::span<const std::int64_t> hot, std::int64_t qty) noexcept { for (std::size_t i = 0; i < hot.size(); ++i) if (hot[i] == qty) return i; return hot.size(); }

void writeQty(std::int64_t side, std::int64_t qty, std::span<const std::int64_t> bidHot, std::span<const std::int64_t> askHot, std::vector<std::uint8_t>& codeStream, std::vector<std::uint8_t>& escapeStream, Header& h) {
    const auto hot = side == 0 ? bidHot : askHot; const auto code = qtyCode(hot, qty); writeVarint(codeStream, code);
    if (code == hot.size()) { writeVarint(escapeStream, static_cast<std::uint64_t>(qty)); ++h.qtyEscapeCount; }
}

bool readQty(std::int64_t side, std::span<const std::int64_t> bidHot, std::span<const std::int64_t> askHot, const std::uint8_t*& code, const std::uint8_t* codeEnd, const std::uint8_t*& escape, const std::uint8_t* escapeEnd, std::int64_t& qty) noexcept {
    const auto hot = side == 0 ? bidHot : askHot; std::uint64_t rawCode = 0; if (!readVarint(code, codeEnd, rawCode)) return false;
    if (rawCode < hot.size()) { qty = hot[static_cast<std::size_t>(rawCode)]; return true; }
    std::uint64_t rawQty = 0; if (rawCode != hot.size() || !readVarint(escape, escapeEnd, rawQty)) return false; qty = static_cast<std::int64_t>(rawQty); return true;
}

bool runModeCandidate(const Batch& batch, const BookState& state, std::int64_t priceScale, std::uint64_t& bidCount, std::uint64_t& askCount) noexcept {
    bidCount = 0; askCount = 0; bool seenAsk = false; std::int64_t prevBid = -1; std::int64_t prevAsk = -1;
    for (const auto& raw : batch.levels) {
        const auto price = raw.price / priceScale;
        if (raw.side == 0) { if (seenAsk || !state.haveBid) return false; const auto off = state.bestBid - price; if (off < 0 || off < prevBid) return false; prevBid = off; ++bidCount; }
        else { seenAsk = true; if (!state.haveAsk) return false; const auto off = price - state.bestAsk; if (off < 0 || off < prevAsk) return false; prevAsk = off; ++askCount; }
    }
    return bidCount != 0u || askCount != 0u;
}

bool readFile(const std::filesystem::path& path, std::vector<std::uint8_t>& out) noexcept { return internal::readFileBytes(path, out); }
Status emitText(const std::string& text, const DecodedBlockCallback& onBlock) noexcept { return onBlock(std::span<const std::uint8_t>{reinterpret_cast<const std::uint8_t*>(text.data()), text.size()}) ? Status::Ok : Status::CallbackStopped; }
Status decodeBytes(std::span<const std::uint8_t> data, std::string* jsonl, std::ostream* encoded) noexcept {
    Header h{}; if (!readHeader(data, h)) return Status::CorruptData;
    std::size_t offset = kHeaderBytes; std::vector<std::int64_t> hot(h.hotQtyCount);
    const auto* hp = data.data() + offset; const auto* he = hp + h.hotQtyBytes; for (auto& qty : hot) if (!readLe(hp, he, qty)) return Status::CorruptData; offset += h.hotQtyBytes;
    std::span<const std::int64_t> bidHot{hot.data(), h.bidHotCount}; std::span<const std::int64_t> askHot{hot.data() + h.bidHotCount, hot.size() - h.bidHotCount};
    std::span<const std::uint8_t> batchS, sideS, priceModeS, deleteS, priceS, codeS, escapeS;
    if (!take(data, offset, h.batchBytes, batchS) || !take(data, offset, h.sideBytes, sideS) || !take(data, offset, h.priceModeBytes, priceModeS) || !take(data, offset, h.deleteBytes, deleteS) || !take(data, offset, h.priceBytes, priceS) || !take(data, offset, h.qtyCodeBytes, codeS) || !take(data, offset, h.qtyEscapeBytes, escapeS) || offset != data.size()) return Status::CorruptData;
    const auto* batch = batchS.data(); const auto* batchEnd = batchS.data() + batchS.size(); const auto* price = priceS.data(); const auto* priceEnd = priceS.data() + priceS.size(); const auto* code = codeS.data(); const auto* codeEnd = codeS.data() + codeS.size(); const auto* escape = escapeS.data(); const auto* escapeEnd = escapeS.data() + escapeS.size();
    BitReader sideBits{sideS.data(), sideS.data() + sideS.size()}; BitReader deleteBits{deleteS.data(), deleteS.data() + deleteS.size()}; BookState state; std::int64_t ts = h.baseTsUnit;
    if (encoded) { *encoded << "{\n  \"pipeline_id\": \"hftmac.depth_ladder_offset_v2\",\n  \"version\": 2,\n  \"batch_count\": " << h.batchCount << ",\n  \"hot_qty\": {\"bid\": ["; for (std::size_t i = 0; i < bidHot.size(); ++i) *encoded << (i ? ", " : "") << bidHot[i]; *encoded << "], \"ask\": ["; for (std::size_t i = 0; i < askHot.size(); ++i) *encoded << (i ? ", " : "") << askHot[i]; *encoded << "]},\n  \"batches\": [\n"; }
    for (std::uint64_t bi = 0; bi < h.batchCount; ++bi) {
        std::uint64_t dt = 0, levelCount = 0, mode = 0; if (!readVarint(batch, batchEnd, dt) || !readVarint(batch, batchEnd, levelCount) || !readVarint(batch, batchEnd, mode)) return Status::CorruptData; ts += static_cast<std::int64_t>(dt);
        const bool runMode = mode != 0u; std::uint64_t bidCount = 0, askCount = 0; if (runMode && (!readVarint(batch, batchEnd, bidCount) || !readVarint(batch, batchEnd, askCount) || bidCount + askCount != levelCount)) return Status::CorruptData;
        const auto bestBid = state.bestBid; const auto bestAsk = state.bestAsk; std::vector<Level> levels; levels.reserve(static_cast<std::size_t>(levelCount));
        if (encoded && bi < kInspectBatchLimit) *encoded << (bi ? ",\n" : "") << "    {\"dt\": " << dt << ", \"level_count\": " << levelCount << ", \"mode\": \"" << (runMode ? "side_runs_offset_gaps" : "explicit") << "\", \"best_before\": {\"bid\": " << bestBid << ", \"ask\": " << bestAsk << "}, \"levels\": [";
        auto readOne = [&](std::int64_t side, bool offsetMode, std::int64_t& prevOffset, std::uint64_t localIndex) -> bool {
            if (!offsetMode) { bool sb = false; if (!sideBits.bit(sb)) return false; side = sb ? 1 : 0; }
            std::uint64_t rawPrice = 0; if (!readVarint(price, priceEnd, rawPrice)) return false;
            std::int64_t offsetValue = 0; std::int64_t priceTick = 0;
            if (offsetMode) { offsetValue = localIndex == 0 ? static_cast<std::int64_t>(rawPrice) : prevOffset + static_cast<std::int64_t>(rawPrice); prevOffset = offsetValue; priceTick = side == 0 ? bestBid - offsetValue : bestAsk + offsetValue; }
            else {
                const bool haveSide = side == 0 ? state.haveBid : state.haveAsk;
                if (haveSide) { offsetValue = unzigzag(rawPrice); priceTick = side == 0 ? bestBid - offsetValue : bestAsk + offsetValue; }
                else { priceTick = unzigzag(rawPrice); }
            }
            bool deleted = false; if (!deleteBits.bit(deleted)) return false; std::int64_t qty = 0; if (!deleted && !readQty(side, bidHot, askHot, code, codeEnd, escape, escapeEnd, qty)) return false;
            levels.push_back(Level{priceTick, qty, side});
            if (encoded && bi < kInspectBatchLimit) *encoded << (levels.size() > 1 ? ", " : "") << "{\"side\": " << side << ", \"price\": " << priceTick << ", \"offset\": " << (offsetMode ? offsetValue : 0) << ", \"delete\": " << (deleted ? "true" : "false") << ", \"qty\": " << qty << "}";
            return true;
        };
        if (runMode) { std::int64_t prev = 0; for (std::uint64_t i = 0; i < bidCount; ++i) if (!readOne(0, true, prev, i)) return Status::CorruptData; prev = 0; for (std::uint64_t i = 0; i < askCount; ++i) if (!readOne(1, true, prev, i)) return Status::CorruptData; }
        else { std::int64_t prev = 0; for (std::uint64_t i = 0; i < levelCount; ++i) if (!readOne(0, false, prev, 0)) return Status::CorruptData; }
        if (encoded && bi < kInspectBatchLimit) *encoded << "]}";
        if (jsonl) { *jsonl += "["; for (std::size_t i = 0; i < levels.size(); ++i) { if (i) *jsonl += ","; const auto& l = levels[i]; *jsonl += "[" + std::to_string(l.price * h.priceScale) + "," + std::to_string(l.qty * h.qtyScale) + "," + std::to_string(l.side) + "]"; } *jsonl += "," + std::to_string(ts * h.timeScale) + "]\n"; }
        for (const auto& level : levels) state.apply(level.side, level.price, level.qty); state.recompute();
    }
    if (batch != batchEnd || price != priceEnd || code != codeEnd || escape != escapeEnd) return Status::CorruptData;
    if (encoded) { if (h.batchCount > kInspectBatchLimit) *encoded << "\n  ],\n  \"truncated\": true,\n  \"shown_batches\": " << kInspectBatchLimit << "\n}\n"; else *encoded << "\n  ],\n  \"truncated\": false\n}\n"; }
    return Status::Ok;
}

std::string statsJson(const Header& h) {
    std::ostringstream out;
    out << "{\n  \"pipeline_id\": \"hftmac.depth_ladder_offset_v2\",\n  \"version\": 2,\n  \"batch_count\": " << h.batchCount << ",\n  \"total_level_count\": " << h.levelCount << ",\n  \"raw_runtime_bytes\": " << (h.batchCount * 8u + h.levelCount * 32u) << ",\n  \"encoded_bytes\": " << h.outputBytes << ",\n  \"bytes_per_level\": " << (h.levelCount ? static_cast<double>(h.outputBytes) / static_cast<double>(h.levelCount) : 0.0) << ",\n  \"delete_count\": " << h.deleteCount << ",\n  \"qty_escape_count\": " << h.qtyEscapeCount << ",\n  \"run_mode_batch_count\": " << h.runModeBatchCount << ",\n  \"fallback_batch_count\": " << h.fallbackBatchCount << ",\n  \"offset_price_count\": " << h.offsetPriceCount << ",\n  \"absolute_price_count\": " << h.absolutePriceCount << ",\n  \"batch_stream_bytes\": " << h.batchBytes << ",\n  \"side_stream_bytes\": " << h.sideBytes << ",\n  \"delete_bit_stream_bytes\": " << h.deleteBytes << ",\n  \"price_stream_bytes\": " << h.priceBytes << ",\n  \"qty_code_stream_bytes\": " << h.qtyCodeBytes << ",\n  \"qty_escape_stream_bytes\": " << h.qtyEscapeBytes << "\n}\n";
    return out.str();
}

}  // namespace

CompressionResult compress(const CompressionRequest& request, const PipelineDescriptor& pipeline) noexcept {
    if (request.inputPath.empty()) { auto result = internal::fail(Status::InvalidArgument, request, &pipeline, "input path is empty"); metrics::recordRun(result); return result; }
    if (inferStreamTypeFromPath(request.inputPath) != StreamType::Depth) { auto result = internal::fail(Status::UnsupportedStream, request, &pipeline, "expected depth.jsonl"); metrics::recordRun(result); return result; }
    CompressionResult result{}; internal::applyPipeline(result, &pipeline); result.streamType = StreamType::Depth; result.inputPath = request.inputPath; const auto totalStart = timing::nowNs();
    std::vector<std::uint8_t> input; const auto readStart = timing::nowNs(); if (!internal::readFileBytes(request.inputPath, input)) { auto failed = internal::fail(Status::IoError, request, &pipeline, "failed to read input file"); metrics::recordRun(failed); return failed; } result.readNs = timing::nowNs() - readStart; result.inputBytes = input.size();
    std::vector<Batch> batches; const auto parseStart = timing::nowNs(); batches.reserve(std::count(input.begin(), input.end(), static_cast<std::uint8_t>('\n')) + 1u); if (!parseBatches(input, batches)) { auto failed = internal::fail(Status::CorruptData, request, &pipeline, "input is not canonical depth jsonl"); metrics::recordRun(failed); return failed; } result.parseNs = timing::nowNs() - parseStart;
    Header h{}; h.inputBytes = result.inputBytes; h.batchCount = batches.size(); std::int64_t timeScale = batches.front().ts, priceScale = 0, qtyScale = 0; std::unordered_map<std::int64_t, std::uint32_t> bidQtyCounts, askQtyCounts;
    for (const auto& batch : batches) { timeScale = gcdAbs(timeScale, batch.ts - batches.front().ts); h.levelCount += batch.levels.size(); for (const auto& level : batch.levels) { priceScale = gcdAbs(priceScale, level.price); qtyScale = gcdAbs(qtyScale, level.qty); if (level.qty != 0) { if (level.side == 0) ++bidQtyCounts[level.qty]; else ++askQtyCounts[level.qty]; } } }
    h.timeScale = safeScale(timeScale); h.priceScale = safeScale(priceScale); h.qtyScale = safeScale(qtyScale); h.baseTsUnit = batches.front().ts / h.timeScale;
    auto bidHotVec = buildHotQty(bidQtyCounts, h.qtyScale); auto askHotVec = buildHotQty(askQtyCounts, h.qtyScale); h.bidHotCount = static_cast<std::uint16_t>(bidHotVec.size()); std::vector<std::int64_t> hot; hot.insert(hot.end(), bidHotVec.begin(), bidHotVec.end()); hot.insert(hot.end(), askHotVec.begin(), askHotVec.end()); h.hotQtyCount = static_cast<std::uint32_t>(hot.size()); h.hotQtyBytes = static_cast<std::uint32_t>(hot.size() * sizeof(std::int64_t)); std::span<const std::int64_t> bidHot{hot.data(), h.bidHotCount}; std::span<const std::int64_t> askHot{hot.data() + h.bidHotCount, hot.size() - h.bidHotCount};
    std::vector<std::uint8_t> batchStream, priceStream, qtyCodeStream, qtyEscapeStream; BitWriter sideBits, deleteBits; BookState state; std::int64_t previousTs = h.baseTsUnit; const auto encodeStart = timing::nowNs(); const auto cycleStart = timing::readCycles();
    for (const auto& batch : batches) {
        const auto ts = batch.ts / h.timeScale; writeVarint(batchStream, static_cast<std::uint64_t>(ts - previousTs)); writeVarint(batchStream, static_cast<std::uint64_t>(batch.levels.size())); previousTs = ts;
        std::uint64_t bidCount = 0, askCount = 0; const bool runMode = runModeCandidate(batch, state, h.priceScale, bidCount, askCount); writeVarint(batchStream, runMode ? 1u : 0u);
        const auto bestBid = state.bestBid; const auto bestAsk = state.bestAsk;
        if (runMode) {
            ++h.runModeBatchCount; writeVarint(batchStream, bidCount); writeVarint(batchStream, askCount); std::int64_t previousOffset = 0; bool first = true;
            for (const auto& raw : batch.levels) if (raw.side == 0) { const auto price = raw.price / h.priceScale; const auto qty = raw.qty / h.qtyScale; const auto offset = bestBid - price; writeVarint(priceStream, static_cast<std::uint64_t>(first ? offset : offset - previousOffset)); previousOffset = offset; first = false; ++h.offsetPriceCount; const bool deleted = qty == 0; deleteBits.bit(deleted); if (deleted) ++h.deleteCount; else writeQty(0, qty, bidHot, askHot, qtyCodeStream, qtyEscapeStream, h); }
            previousOffset = 0; first = true;
            for (const auto& raw : batch.levels) if (raw.side == 1) { const auto price = raw.price / h.priceScale; const auto qty = raw.qty / h.qtyScale; const auto offset = price - bestAsk; writeVarint(priceStream, static_cast<std::uint64_t>(first ? offset : offset - previousOffset)); previousOffset = offset; first = false; ++h.offsetPriceCount; const bool deleted = qty == 0; deleteBits.bit(deleted); if (deleted) ++h.deleteCount; else writeQty(1, qty, bidHot, askHot, qtyCodeStream, qtyEscapeStream, h); }
        } else {
            ++h.fallbackBatchCount;
            for (const auto& raw : batch.levels) { const auto side = raw.side; const auto price = raw.price / h.priceScale; const auto qty = raw.qty / h.qtyScale; sideBits.bit(side != 0); const bool haveSide = side == 0 ? state.haveBid : state.haveAsk; if (haveSide) { writeVarint(priceStream, zigzag(side == 0 ? bestBid - price : price - bestAsk)); ++h.offsetPriceCount; } else { writeVarint(priceStream, zigzag(price)); ++h.absolutePriceCount; } const bool deleted = qty == 0; deleteBits.bit(deleted); if (deleted) ++h.deleteCount; else writeQty(side, qty, bidHot, askHot, qtyCodeStream, qtyEscapeStream, h); }
        }
        for (const auto& raw : batch.levels) state.apply(raw.side, raw.price / h.priceScale, raw.qty / h.qtyScale); state.recompute();
    }
    result.encodeCycles = timing::readCycles() - cycleStart; result.encodeCoreNs = timing::nowNs() - encodeStart; const auto sideStream = sideBits.finish(); const auto deleteStream = deleteBits.finish(); h.batchBytes = static_cast<std::uint32_t>(batchStream.size()); h.sideBytes = static_cast<std::uint32_t>(sideStream.size()); h.priceModeBytes = 0; h.deleteBytes = static_cast<std::uint32_t>(deleteStream.size()); h.priceBytes = static_cast<std::uint32_t>(priceStream.size()); h.qtyCodeBytes = static_cast<std::uint32_t>(qtyCodeStream.size()); h.qtyEscapeBytes = static_cast<std::uint32_t>(qtyEscapeStream.size()); h.outputBytes = kHeaderBytes + h.hotQtyBytes + h.batchBytes + h.sideBytes + h.priceModeBytes + h.deleteBytes + h.priceBytes + h.qtyCodeBytes + h.qtyEscapeBytes;
    const auto outputPath = internal::outputPathFor(request, pipeline, StreamType::Depth); std::error_code dirEc; std::filesystem::create_directories(outputPath.parent_path(), dirEc); if (dirEc) { auto failed = internal::fail(Status::IoError, request, &pipeline, "failed to create output directory"); metrics::recordRun(failed); return failed; }
    result.outputPath = outputPath; result.metricsPath = outputPath.parent_path() / (outputPath.stem().string() + ".metrics.json"); const auto writeStart = timing::nowNs(); std::ofstream out(outputPath, std::ios::binary | std::ios::trunc); const auto header = serializeHeader(h, hot); out.write(reinterpret_cast<const char*>(header.data()), static_cast<std::streamsize>(header.size())); out.write(reinterpret_cast<const char*>(batchStream.data()), static_cast<std::streamsize>(batchStream.size())); out.write(reinterpret_cast<const char*>(sideStream.data()), static_cast<std::streamsize>(sideStream.size())); out.write(reinterpret_cast<const char*>(deleteStream.data()), static_cast<std::streamsize>(deleteStream.size())); out.write(reinterpret_cast<const char*>(priceStream.data()), static_cast<std::streamsize>(priceStream.size())); out.write(reinterpret_cast<const char*>(qtyCodeStream.data()), static_cast<std::streamsize>(qtyCodeStream.size())); out.write(reinterpret_cast<const char*>(qtyEscapeStream.data()), static_cast<std::streamsize>(qtyEscapeStream.size())); out.close(); result.writeNs = timing::nowNs() - writeStart; result.outputBytes = h.outputBytes; result.lineCount = h.batchCount; result.blockCount = 1; result.encodeNs = timing::nowNs() - totalStart;
    std::vector<std::uint8_t> file; (void)readFile(outputPath, file); std::string decoded; const auto decodeStart = timing::nowNs(); const auto decodeCycles = timing::readCycles(); const auto decodeStatus = decodeBytes(file, &decoded, nullptr); result.decodeCycles = timing::readCycles() - decodeCycles; result.decodeNs = timing::nowNs() - decodeStart; result.decodeCoreNs = result.decodeNs; result.roundtripOk = isOk(decodeStatus) && decoded.size() == input.size() && std::equal(decoded.begin(), decoded.end(), reinterpret_cast<const char*>(input.data())); result.status = result.roundtripOk ? Status::Ok : Status::DecodeError; if (!result.roundtripOk) result.error = "roundtrip check failed"; (void)internal::writeTextFile(result.metricsPath, toMetricsJson(result)); metrics::recordRun(result); return result;
}

ReplayArtifactInfo inspectArtifact(const std::filesystem::path& path, const PipelineDescriptor& pipeline) noexcept { std::vector<std::uint8_t> data; ReplayArtifactInfo info{}; info.path = path; if (!readFile(path, data)) { info.status = Status::IoError; info.error = "failed to read artifact"; return info; } Header h{}; if (!readHeader(data, h)) { info.status = Status::CorruptData; info.error = "invalid depth v2 artifact"; return info; } info.status = Status::Ok; info.found = true; info.formatId = "hftmac.depth_ladder_offset.v2"; info.pipelineId = std::string{pipeline.id}; info.transform = std::string{pipeline.transform}; info.entropy = std::string{pipeline.entropy}; info.streamType = StreamType::Depth; info.version = h.version; info.inputBytes = h.inputBytes; info.outputBytes = h.outputBytes; info.lineCount = h.batchCount; info.blockCount = 1; return info; }
Status decode(std::span<const std::uint8_t> bytes, const DecodedBlockCallback& onBlock) noexcept { std::string out; const auto status = decodeBytes(bytes, &out, nullptr); if (!isOk(status)) return status; return emitText(out, onBlock); }
Status decodeFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept { std::vector<std::uint8_t> data; if (!readFile(path, data)) return Status::IoError; return decode(data, onBlock); }
Status inspectEncodedJsonFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept { std::vector<std::uint8_t> data; if (!readFile(path, data)) return Status::IoError; std::ostringstream out; const auto status = decodeBytes(data, nullptr, &out); if (!isOk(status)) return status; return emitText(out.str(), onBlock); }
Status inspectEncodedBinaryFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept { std::vector<std::uint8_t> data; if (!readFile(path, data)) return Status::IoError; Header h{}; if (!readHeader(data, h)) return Status::CorruptData; std::ostringstream out; out << "depth_ladder_offset_v2 bytes=" << data.size() << " header=" << kHeaderBytes << " hot_qty=" << h.hotQtyBytes << " batch=" << h.batchBytes << " side=" << h.sideBytes << " price_mode=" << h.priceModeBytes << " delete=" << h.deleteBytes << " price=" << h.priceBytes << " qty_code=" << h.qtyCodeBytes << " qty_escape=" << h.qtyEscapeBytes << "\n"; return emitText(out.str(), onBlock); }
Status inspectStatsJsonFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept { std::vector<std::uint8_t> data; if (!readFile(path, data)) return Status::IoError; Header h{}; if (!readHeader(data, h)) return Status::CorruptData; return emitText(statsJson(h), onBlock); }

}  // namespace hft_compressor::codecs::depth_ladder_offset_v2
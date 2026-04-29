#include "codecs/hftmac_common/HftMacCommon.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <cstddef>
#include <cstdint>
#include <fstream>
#include <span>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <utility>
#include <vector>

#include "common/CompressionInternals.hpp"
#include "common/timing.hpp"
#include "container/hfc/format.hpp"
#include "hft_compressor/metrics.hpp"

namespace hft_compressor::codecs::hftmac_common {
namespace {

constexpr std::array<std::uint8_t, 8> kFileMagic{{'H', 'F', 'T', 'M', 'A', 'C', '1', 0}};
constexpr std::array<std::uint8_t, 8> kBlockMagic{{'H', 'F', 'T', 'M', 'B', 'L', 'K', 0}};
constexpr std::uint16_t kVersion = 1u;
constexpr std::uint16_t kProfileVarint = 1u;
constexpr std::uint16_t kProfileRawBinary = 2u;
constexpr std::size_t kFileHeaderBytes = 96u;
constexpr std::size_t kBlockHeaderBytes = 64u;
constexpr std::uint32_t kFlagFinalNewline = 1u;

struct Level {
    std::int64_t priceE8{0};
    std::int64_t qtyE8{0};
    std::int64_t side{0};
};

struct Event {
    std::int64_t tsNs{0};
    std::array<std::int64_t, 4> values{};
    std::vector<Level> levels{};
    std::uint32_t decodedBytes{0};
};

struct ParsedInput {
    std::vector<Event> records{};
    bool finalNewline{false};
    std::uint64_t inputBytes{0};
    std::int64_t firstTsNs{0};
    std::int64_t lastTsNs{0};
};

struct FileHeader {
    std::array<std::uint8_t, 8> magic{kFileMagic};
    std::uint16_t version{kVersion};
    std::uint16_t stream{0};
    std::uint16_t schema{0};
    std::uint16_t profile{kProfileVarint};
    std::uint32_t flags{0};
    std::uint32_t blockBytes{0};
    std::uint64_t inputBytes{0};
    std::uint64_t outputBytes{0};
    std::uint64_t recordCount{0};
    std::uint64_t blockCount{0};
    std::int64_t firstTsNs{0};
    std::int64_t lastTsNs{0};
    std::uint32_t headerCrc32c{0};
};

struct BlockHeader {
    std::array<std::uint8_t, 8> magic{kBlockMagic};
    std::uint32_t decodedBytes{0};
    std::uint32_t payloadBytes{0};
    std::uint32_t recordCount{0};
    std::uint32_t reserved0{0};
    std::uint64_t firstByteOffset{0};
    std::int64_t firstTsNs{0};
    std::int64_t lastTsNs{0};
    std::uint32_t decodedCrc32c{0};
    std::uint32_t payloadCrc32c{0};
};

template <typename T>
void writeLe(std::vector<std::uint8_t>& out, T value) {
    const auto raw = static_cast<std::uint64_t>(value);
    for (std::size_t i = 0; i < sizeof(T); ++i) {
        out.push_back(static_cast<std::uint8_t>((raw >> (i * 8u)) & 0xffu));
    }
}

template <typename T>
bool readLe(const std::uint8_t*& p, const std::uint8_t* end, T& out) noexcept {
    if (static_cast<std::size_t>(end - p) < sizeof(T)) return false;
    std::uint64_t raw = 0;
    for (std::size_t i = 0; i < sizeof(T); ++i) raw |= static_cast<std::uint64_t>(p[i]) << (i * 8u);
    p += sizeof(T);
    out = static_cast<T>(raw);
    return true;
}

std::vector<std::uint8_t> serializeFileHeader(FileHeader header, bool includeCrc) {
    if (!includeCrc) header.headerCrc32c = 0u;
    std::vector<std::uint8_t> out;
    out.reserve(kFileHeaderBytes);
    out.insert(out.end(), header.magic.begin(), header.magic.end());
    writeLe(out, header.version);
    writeLe(out, header.stream);
    writeLe(out, header.schema);
    writeLe(out, header.profile);
    writeLe(out, header.flags);
    writeLe(out, header.blockBytes);
    writeLe(out, header.inputBytes);
    writeLe(out, header.outputBytes);
    writeLe(out, header.recordCount);
    writeLe(out, header.blockCount);
    writeLe(out, header.firstTsNs);
    writeLe(out, header.lastTsNs);
    writeLe(out, header.headerCrc32c);
    out.resize(kFileHeaderBytes, 0u);
    return out;
}

std::vector<std::uint8_t> serializeBlockHeader(const BlockHeader& header) {
    std::vector<std::uint8_t> out;
    out.reserve(kBlockHeaderBytes);
    out.insert(out.end(), header.magic.begin(), header.magic.end());
    writeLe(out, header.decodedBytes);
    writeLe(out, header.payloadBytes);
    writeLe(out, header.recordCount);
    writeLe(out, header.reserved0);
    writeLe(out, header.firstByteOffset);
    writeLe(out, header.firstTsNs);
    writeLe(out, header.lastTsNs);
    writeLe(out, header.decodedCrc32c);
    writeLe(out, header.payloadCrc32c);
    writeLe<std::uint64_t>(out, 0u);
    out.resize(kBlockHeaderBytes, 0u);
    return out;
}

std::uint32_t headerCrc32c(const FileHeader& header) {
    return format::crc32c(serializeFileHeader(header, false));
}

bool parseFileHeader(const std::uint8_t* data, std::size_t size, FileHeader& out) noexcept {
    if (data == nullptr || size < kFileHeaderBytes) return false;
    const auto* p = data;
    const auto* end = data + kFileHeaderBytes;
    std::copy(p, p + kFileMagic.size(), out.magic.begin());
    p += kFileMagic.size();
    return out.magic == kFileMagic
        && readLe(p, end, out.version)
        && readLe(p, end, out.stream)
        && readLe(p, end, out.schema)
        && readLe(p, end, out.profile)
        && readLe(p, end, out.flags)
        && readLe(p, end, out.blockBytes)
        && readLe(p, end, out.inputBytes)
        && readLe(p, end, out.outputBytes)
        && readLe(p, end, out.recordCount)
        && readLe(p, end, out.blockCount)
        && readLe(p, end, out.firstTsNs)
        && readLe(p, end, out.lastTsNs)
        && readLe(p, end, out.headerCrc32c);
}

bool parseBlockHeader(const std::uint8_t* data, std::size_t size, BlockHeader& out) noexcept {
    if (data == nullptr || size < kBlockHeaderBytes) return false;
    const auto* p = data;
    const auto* end = data + kBlockHeaderBytes;
    std::copy(p, p + kBlockMagic.size(), out.magic.begin());
    p += kBlockMagic.size();
    std::uint64_t reserved1 = 0;
    return out.magic == kBlockMagic
        && readLe(p, end, out.decodedBytes)
        && readLe(p, end, out.payloadBytes)
        && readLe(p, end, out.recordCount)
        && readLe(p, end, out.reserved0)
        && readLe(p, end, out.firstByteOffset)
        && readLe(p, end, out.firstTsNs)
        && readLe(p, end, out.lastTsNs)
        && readLe(p, end, out.decodedCrc32c)
        && readLe(p, end, out.payloadCrc32c)
        && readLe(p, end, reserved1)
        && out.reserved0 == 0u
        && reserved1 == 0u;
}

std::uint16_t profileFor(PayloadCodec payloadCodec) noexcept {
    switch (payloadCodec) {
        case PayloadCodec::PlainVarint: return kProfileVarint;
        case PayloadCodec::RawBinary: return kProfileRawBinary;
    }
    return 0u;
}

bool validHeader(const FileHeader& header, const BackendSpec& spec) noexcept {
    return header.magic == kFileMagic
        && header.version == kVersion
        && header.stream == format::streamToWire(spec.streamType)
        && header.schema == format::streamToWire(spec.streamType)
        && header.profile == profileFor(spec.payloadCodec)
        && header.blockBytes != 0u
        && (header.flags & ~kFlagFinalNewline) == 0u;
}

bool validBlock(const BlockHeader& block, std::uint64_t expectedOffset) noexcept {
    return block.magic == kBlockMagic
        && block.decodedBytes != 0u
        && block.payloadBytes != 0u
        && block.recordCount != 0u
        && block.firstByteOffset == expectedOffset;
}

bool validSide(std::int64_t side) noexcept {
    return side == 0 || side == 1;
}

struct JsonCursor {
    std::string_view text{};
    std::size_t pos{0};

    bool consume(char c) noexcept {
        if (pos >= text.size() || text[pos] != c) return false;
        ++pos;
        return true;
    }

    bool peek(char c) const noexcept {
        return pos < text.size() && text[pos] == c;
    }

    bool parseInt64(std::int64_t& out) noexcept {
        if (pos >= text.size()) return false;
        const char* begin = text.data() + pos;
        const char* end = text.data() + text.size();
        const auto [ptr, ec] = std::from_chars(begin, end, out);
        if (ec != std::errc{} || ptr == begin) return false;
        pos = static_cast<std::size_t>(ptr - text.data());
        return true;
    }

    bool finish() const noexcept {
        return pos == text.size();
    }
};

bool parseTradeLine(std::string_view line, Event& out) noexcept {
    JsonCursor p{line};
    return p.consume('[')
        && p.parseInt64(out.values[0]) && p.consume(',')
        && p.parseInt64(out.values[1]) && p.consume(',')
        && p.parseInt64(out.values[2]) && validSide(out.values[2]) && p.consume(',')
        && p.parseInt64(out.tsNs)
        && p.consume(']') && p.finish();
}

bool parseBookTickerLine(std::string_view line, Event& out) noexcept {
    JsonCursor p{line};
    return p.consume('[')
        && p.parseInt64(out.values[0]) && p.consume(',')
        && p.parseInt64(out.values[1]) && p.consume(',')
        && p.parseInt64(out.values[2]) && p.consume(',')
        && p.parseInt64(out.values[3]) && p.consume(',')
        && p.parseInt64(out.tsNs)
        && p.consume(']') && p.finish();
}

bool parseDepthLevel(JsonCursor& p, Level& out) noexcept {
    return p.consume('[')
        && p.parseInt64(out.priceE8) && p.consume(',')
        && p.parseInt64(out.qtyE8) && p.consume(',')
        && p.parseInt64(out.side) && validSide(out.side)
        && p.consume(']');
}

bool parseDepthLine(std::string_view line, Event& out) noexcept {
    JsonCursor p{line};
    if (!p.consume('[') || !p.peek('[')) return false;
    while (p.peek('[')) {
        Level level{};
        if (!parseDepthLevel(p, level) || !p.consume(',')) return false;
        out.levels.push_back(level);
    }
    return p.parseInt64(out.tsNs) && p.consume(']') && p.finish();
}

Status parseCompactJsonl(std::span<const std::uint8_t> input,
                         StreamType streamType,
                         ParsedInput& out,
                         std::string& error) noexcept {
    out = ParsedInput{};
    out.inputBytes = static_cast<std::uint64_t>(input.size());
    out.finalNewline = !input.empty() && input.back() == static_cast<std::uint8_t>('\n');
    std::size_t lineStart = 0;
    while (lineStart < input.size()) {
        std::size_t lineEnd = lineStart;
        while (lineEnd < input.size() && input[lineEnd] != static_cast<std::uint8_t>('\n')) ++lineEnd;
        if (lineEnd == lineStart) {
            error = "empty lines are not valid HFT-MAC input";
            return Status::CorruptData;
        }
        std::string_view line{reinterpret_cast<const char*>(input.data() + lineStart), lineEnd - lineStart};
        Event event{};
        const bool ok = streamType == StreamType::Trades ? parseTradeLine(line, event)
            : streamType == StreamType::BookTicker ? parseBookTickerLine(line, event)
            : streamType == StreamType::Depth ? parseDepthLine(line, event)
            : false;
        if (!ok) {
            error = "input is not compact canonical jsonl for HFT-MAC";
            return Status::CorruptData;
        }
        event.decodedBytes = static_cast<std::uint32_t>(line.size() + (lineEnd < input.size() ? 1u : 0u));
        if (out.records.empty()) out.firstTsNs = event.tsNs;
        out.lastTsNs = event.tsNs;
        out.records.push_back(std::move(event));
        lineStart = lineEnd + (lineEnd < input.size() ? 1u : 0u);
    }
    return Status::Ok;
}

void appendChar(std::vector<std::uint8_t>& out, char c) {
    out.push_back(static_cast<std::uint8_t>(c));
}

void appendInt(std::vector<std::uint8_t>& out, std::int64_t value) {
    std::array<char, 32> buffer{};
    const auto [ptr, ec] = std::to_chars(buffer.data(), buffer.data() + buffer.size(), value);
    if (ec == std::errc{}) out.insert(out.end(), buffer.data(), ptr);
}

void appendRecordJson(StreamType streamType, const Event& event, bool newline, std::vector<std::uint8_t>& out) {
    appendChar(out, '[');
    if (streamType == StreamType::Trades) {
        appendInt(out, event.values[0]); appendChar(out, ',');
        appendInt(out, event.values[1]); appendChar(out, ',');
        appendInt(out, event.values[2]); appendChar(out, ',');
        appendInt(out, event.tsNs);
    } else if (streamType == StreamType::BookTicker) {
        appendInt(out, event.values[0]); appendChar(out, ',');
        appendInt(out, event.values[1]); appendChar(out, ',');
        appendInt(out, event.values[2]); appendChar(out, ',');
        appendInt(out, event.values[3]); appendChar(out, ',');
        appendInt(out, event.tsNs);
    } else if (streamType == StreamType::Depth) {
        for (std::size_t i = 0; i < event.levels.size(); ++i) {
            if (i != 0u) appendChar(out, ',');
            appendChar(out, '[');
            appendInt(out, event.levels[i].priceE8); appendChar(out, ',');
            appendInt(out, event.levels[i].qtyE8); appendChar(out, ',');
            appendInt(out, event.levels[i].side);
            appendChar(out, ']');
        }
        appendChar(out, ',');
        appendInt(out, event.tsNs);
    }
    appendChar(out, ']');
    if (newline) appendChar(out, '\n');
}

void buildDecodedBlock(StreamType streamType,
                       std::span<const Event> records,
                       std::uint64_t firstRecordIndex,
                       std::uint64_t totalRecords,
                       bool finalNewline,
                       std::vector<std::uint8_t>& out) {
    out.clear();
    for (std::size_t i = 0; i < records.size(); ++i) {
        const auto index = firstRecordIndex + i;
        const bool newline = finalNewline || index + 1u < totalRecords;
        appendRecordJson(streamType, records[i], newline, out);
    }
}

std::uint64_t zigZagEncode(std::int64_t value) noexcept {
    return value >= 0 ? static_cast<std::uint64_t>(value) << 1u
        : (static_cast<std::uint64_t>(-(value + 1)) << 1u) | 1u;
}

std::int64_t zigZagDecode(std::uint64_t value) noexcept {
    const auto magnitude = static_cast<std::int64_t>(value >> 1u);
    return (value & 1u) == 0u ? magnitude : -magnitude - 1;
}

void writeVarint(std::vector<std::uint8_t>& out, std::uint64_t value) {
    while (value >= 0x80u) {
        out.push_back(static_cast<std::uint8_t>(value | 0x80u));
        value >>= 7u;
    }
    out.push_back(static_cast<std::uint8_t>(value));
}

void writeZigZag(std::vector<std::uint8_t>& out, std::int64_t value) {
    writeVarint(out, zigZagEncode(value));
}

bool readVarint(const std::uint8_t*& p, const std::uint8_t* end, std::uint64_t& value) noexcept {
    value = 0;
    for (std::uint32_t shift = 0; shift <= 63u; shift += 7u) {
        if (p == end) return false;
        const auto byte = *p++;
        value |= static_cast<std::uint64_t>(byte & 0x7fu) << shift;
        if ((byte & 0x80u) == 0u) return true;
    }
    return false;
}

bool readZigZag(const std::uint8_t*& p, const std::uint8_t* end, std::int64_t& value) noexcept {
    std::uint64_t raw = 0;
    if (!readVarint(p, end, raw)) return false;
    value = zigZagDecode(raw);
    return true;
}

struct TradeState {
    std::int64_t tsNs{0};
    std::int64_t tsDelta{0};
    std::int64_t priceE8{0};
    std::int64_t qtyE8{0};
    std::int64_t side{0};
};

struct BookTickerState {
    std::int64_t tsNs{0};
    std::int64_t tsDelta{0};
    std::int64_t bidQtyE8{0};
    std::int64_t askQtyE8{0};
    std::int64_t mid2{0};
    std::int64_t spread{0};
};

struct DepthState {
    std::int64_t tsNs{0};
    std::int64_t tsDelta{0};
    std::array<std::int64_t, 2> lastPriceBySide{};
    std::unordered_map<std::uint64_t, std::int64_t> qtyBySidePrice{};
};

struct TransformState {
    TradeState trade{};
    BookTickerState bookTicker{};
    DepthState depth{};
};

std::uint64_t depthKey(std::int64_t side, std::int64_t price) noexcept {
    return (static_cast<std::uint64_t>(price) << 1u) ^ static_cast<std::uint64_t>(side);
}

void encodeTradeEvent(const Event& event, TradeState& state, std::vector<std::uint8_t>& out) {
    const auto tsDelta = event.tsNs - state.tsNs;
    writeZigZag(out, tsDelta - state.tsDelta);
    writeZigZag(out, event.values[0] - state.priceE8);
    writeZigZag(out, event.values[1] - state.qtyE8);
    writeVarint(out, static_cast<std::uint64_t>(event.values[2] ^ state.side));
    state.tsNs = event.tsNs;
    state.tsDelta = tsDelta;
    state.priceE8 = event.values[0];
    state.qtyE8 = event.values[1];
    state.side = event.values[2];
}

void encodeBookTickerEvent(const Event& event, BookTickerState& state, std::vector<std::uint8_t>& out) {
    const auto tsDelta = event.tsNs - state.tsNs;
    const auto mid2 = event.values[0] + event.values[2];
    const auto spread = event.values[2] - event.values[0];
    writeZigZag(out, tsDelta - state.tsDelta);
    writeZigZag(out, mid2 - state.mid2);
    writeZigZag(out, spread - state.spread);
    writeZigZag(out, event.values[1] - state.bidQtyE8);
    writeZigZag(out, event.values[3] - state.askQtyE8);
    state.tsNs = event.tsNs;
    state.tsDelta = tsDelta;
    state.mid2 = mid2;
    state.spread = spread;
    state.bidQtyE8 = event.values[1];
    state.askQtyE8 = event.values[3];
}

void encodeDepthEvent(const Event& event, DepthState& state, std::vector<std::uint8_t>& out) {
    const auto tsDelta = event.tsNs - state.tsNs;
    writeZigZag(out, tsDelta - state.tsDelta);
    writeVarint(out, static_cast<std::uint64_t>(event.levels.size()));
    for (const auto& level : event.levels) {
        writeVarint(out, static_cast<std::uint64_t>(level.side));
        const auto sideIndex = static_cast<std::size_t>(level.side);
        writeZigZag(out, level.priceE8 - state.lastPriceBySide[sideIndex]);
        state.lastPriceBySide[sideIndex] = level.priceE8;
        const auto key = depthKey(level.side, level.priceE8);
        const auto previous = state.qtyBySidePrice.contains(key) ? state.qtyBySidePrice[key] : 0;
        writeZigZag(out, level.qtyE8 - previous);
        if (level.qtyE8 == 0) state.qtyBySidePrice.erase(key);
        else state.qtyBySidePrice[key] = level.qtyE8;
    }
    state.tsNs = event.tsNs;
    state.tsDelta = tsDelta;
}

void encodeEvent(StreamType streamType, const Event& event, TransformState& state, std::vector<std::uint8_t>& out) {
    if (streamType == StreamType::Trades) encodeTradeEvent(event, state.trade, out);
    else if (streamType == StreamType::BookTicker) encodeBookTickerEvent(event, state.bookTicker, out);
    else if (streamType == StreamType::Depth) encodeDepthEvent(event, state.depth, out);
}

bool decodeTradeEvent(const std::uint8_t*& p, const std::uint8_t* end, TradeState& state, Event& out) noexcept {
    std::int64_t tsDod = 0;
    std::int64_t priceDelta = 0;
    std::int64_t qtyDelta = 0;
    std::uint64_t sideXor = 0;
    if (!readZigZag(p, end, tsDod) || !readZigZag(p, end, priceDelta)
        || !readZigZag(p, end, qtyDelta) || !readVarint(p, end, sideXor) || sideXor > 1u) {
        return false;
    }
    state.tsDelta += tsDod;
    state.tsNs += state.tsDelta;
    state.priceE8 += priceDelta;
    state.qtyE8 += qtyDelta;
    state.side ^= static_cast<std::int64_t>(sideXor);
    out.tsNs = state.tsNs;
    out.values[0] = state.priceE8;
    out.values[1] = state.qtyE8;
    out.values[2] = state.side;
    return validSide(out.values[2]);
}

bool decodeBookTickerEvent(const std::uint8_t*& p, const std::uint8_t* end, BookTickerState& state, Event& out) noexcept {
    std::int64_t tsDod = 0;
    std::int64_t mid2Delta = 0;
    std::int64_t spreadDelta = 0;
    std::int64_t bidQtyDelta = 0;
    std::int64_t askQtyDelta = 0;
    if (!readZigZag(p, end, tsDod) || !readZigZag(p, end, mid2Delta)
        || !readZigZag(p, end, spreadDelta) || !readZigZag(p, end, bidQtyDelta)
        || !readZigZag(p, end, askQtyDelta)) {
        return false;
    }
    state.tsDelta += tsDod;
    state.tsNs += state.tsDelta;
    state.mid2 += mid2Delta;
    state.spread += spreadDelta;
    state.bidQtyE8 += bidQtyDelta;
    state.askQtyE8 += askQtyDelta;
    out.tsNs = state.tsNs;
    out.values[0] = (state.mid2 - state.spread) / 2;
    out.values[1] = state.bidQtyE8;
    out.values[2] = (state.mid2 + state.spread) / 2;
    out.values[3] = state.askQtyE8;
    return true;
}

bool decodeDepthEvent(const std::uint8_t*& p, const std::uint8_t* end, DepthState& state, Event& out) {
    std::int64_t tsDod = 0;
    std::uint64_t levelCount = 0;
    if (!readZigZag(p, end, tsDod) || !readVarint(p, end, levelCount) || levelCount > 1000000u) return false;
    state.tsDelta += tsDod;
    state.tsNs += state.tsDelta;
    out.tsNs = state.tsNs;
    out.levels.reserve(static_cast<std::size_t>(levelCount));
    for (std::uint64_t i = 0; i < levelCount; ++i) {
        std::uint64_t sideRaw = 0;
        std::int64_t priceDelta = 0;
        std::int64_t qtyDelta = 0;
        if (!readVarint(p, end, sideRaw) || sideRaw > 1u || !readZigZag(p, end, priceDelta)
            || !readZigZag(p, end, qtyDelta)) {
            return false;
        }
        const auto side = static_cast<std::int64_t>(sideRaw);
        const auto sideIndex = static_cast<std::size_t>(side);
        const auto price = state.lastPriceBySide[sideIndex] + priceDelta;
        state.lastPriceBySide[sideIndex] = price;
        const auto key = depthKey(side, price);
        const auto previous = state.qtyBySidePrice.contains(key) ? state.qtyBySidePrice[key] : 0;
        const auto qty = previous + qtyDelta;
        if (qty == 0) state.qtyBySidePrice.erase(key);
        else state.qtyBySidePrice[key] = qty;
        out.levels.push_back(Level{price, qty, side});
    }
    return !out.levels.empty();
}

bool decodeEvent(StreamType streamType, const std::uint8_t*& p, const std::uint8_t* end, TransformState& state, Event& out) {
    if (streamType == StreamType::Trades) return decodeTradeEvent(p, end, state.trade, out);
    if (streamType == StreamType::BookTicker) return decodeBookTickerEvent(p, end, state.bookTicker, out);
    if (streamType == StreamType::Depth) return decodeDepthEvent(p, end, state.depth, out);
    return false;
}

bool encodeRawEvent(StreamType streamType, const Event& event, std::vector<std::uint8_t>& out) {
    if (streamType == StreamType::Trades) {
        writeLe(out, event.values[0]);
        writeLe(out, event.values[1]);
        writeLe(out, event.values[2]);
        writeLe(out, event.tsNs);
        return true;
    }
    if (streamType == StreamType::BookTicker) {
        writeLe(out, event.values[0]);
        writeLe(out, event.values[1]);
        writeLe(out, event.values[2]);
        writeLe(out, event.values[3]);
        writeLe(out, event.tsNs);
        return true;
    }
    if (streamType == StreamType::Depth) {
        if (event.levels.size() > 0xffffffffu) return false;
        writeLe<std::uint32_t>(out, static_cast<std::uint32_t>(event.levels.size()));
        for (const auto& level : event.levels) {
            writeLe(out, level.priceE8);
            writeLe(out, level.qtyE8);
            writeLe(out, level.side);
        }
        writeLe(out, event.tsNs);
        return true;
    }
    return false;
}

bool encodePayloadEvent(StreamType streamType,
                        PayloadCodec payloadCodec,
                        const Event& event,
                        TransformState& state,
                        std::vector<std::uint8_t>& out) {
    if (payloadCodec == PayloadCodec::PlainVarint) {
        encodeEvent(streamType, event, state, out);
        return true;
    }
    if (payloadCodec == PayloadCodec::RawBinary) return encodeRawEvent(streamType, event, out);
    return false;
}

bool decodeRawEvent(StreamType streamType, const std::uint8_t*& p, const std::uint8_t* end, Event& out) {
    if (streamType == StreamType::Trades) {
        return readLe(p, end, out.values[0])
            && readLe(p, end, out.values[1])
            && readLe(p, end, out.values[2])
            && validSide(out.values[2])
            && readLe(p, end, out.tsNs);
    }
    if (streamType == StreamType::BookTicker) {
        return readLe(p, end, out.values[0])
            && readLe(p, end, out.values[1])
            && readLe(p, end, out.values[2])
            && readLe(p, end, out.values[3])
            && readLe(p, end, out.tsNs);
    }
    if (streamType == StreamType::Depth) {
        std::uint32_t levelCount = 0;
        if (!readLe(p, end, levelCount) || levelCount == 0u || levelCount > 1000000u) return false;
        out.levels.reserve(levelCount);
        for (std::uint32_t i = 0; i < levelCount; ++i) {
            Level level{};
            if (!readLe(p, end, level.priceE8) || !readLe(p, end, level.qtyE8)
                || !readLe(p, end, level.side) || !validSide(level.side)) {
                return false;
            }
            out.levels.push_back(level);
        }
        return readLe(p, end, out.tsNs);
    }
    return false;
}

bool decodePayloadEvent(StreamType streamType,
                        PayloadCodec payloadCodec,
                        const std::uint8_t*& p,
                        const std::uint8_t* end,
                        TransformState& state,
                        Event& out) {
    if (payloadCodec == PayloadCodec::PlainVarint) return decodeEvent(streamType, p, end, state, out);
    if (payloadCodec == PayloadCodec::RawBinary) return decodeRawEvent(streamType, p, end, out);
    return false;
}

std::size_t chooseBlockEnd(const ParsedInput& input, std::size_t begin, std::uint32_t blockBytes) noexcept {
    std::size_t end = begin;
    std::uint64_t bytes = 0;
    while (end < input.records.size()) {
        const auto next = bytes + input.records[end].decodedBytes;
        if (end != begin && next > blockBytes) break;
        bytes = next;
        ++end;
    }
    return end;
}

void applyPipeline(CompressionResult& result, const PipelineDescriptor& pipeline) {
    result.pipelineId = std::string{pipeline.id};
    result.representation = std::string{pipeline.representation};
    result.transform = std::string{pipeline.transform};
    result.entropy = std::string{pipeline.entropy};
    result.profile = std::string{pipeline.profile};
    result.implementationKind = std::string{pipeline.implementationKind};
}

CompressionResult failCompress(Status status,
                               const CompressionRequest& request,
                               const PipelineDescriptor& pipeline,
                               std::string error) {
    CompressionResult result{};
    result.status = status;
    result.error = std::move(error);
    result.inputPath = request.inputPath;
    result.streamType = inferStreamTypeFromPath(request.inputPath);
    applyPipeline(result, pipeline);
    metrics::recordRun(result);
    return result;
}

ReplayArtifactInfo failArtifact(const std::filesystem::path& path, Status status, std::string error) {
    ReplayArtifactInfo info{};
    info.status = status;
    info.path = path;
    info.error = std::move(error);
    return info;
}

Status readHeaderFromFile(std::ifstream& in, FileHeader& header, const BackendSpec& spec) noexcept {
    std::array<std::uint8_t, kFileHeaderBytes> bytes{};
    in.read(reinterpret_cast<char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
    if (in.gcount() != static_cast<std::streamsize>(bytes.size())) return Status::CorruptData;
    if (!parseFileHeader(bytes.data(), bytes.size(), header) || !validHeader(header, spec)) return Status::CorruptData;
    if (header.headerCrc32c != headerCrc32c(header)) return Status::CorruptData;
    return Status::Ok;
}

}  // namespace

CompressionResult compressVarint(const CompressionRequest& request,
                                 const PipelineDescriptor& pipeline,
                                 const BackendSpec& spec) noexcept {
    if (request.inputPath.empty()) return failCompress(Status::InvalidArgument, request, pipeline, "input path is empty");
    const auto streamType = inferStreamTypeFromPath(request.inputPath);
    if (streamType != spec.streamType) {
        return failCompress(Status::UnsupportedStream, request, pipeline, "HFT-MAC pipeline does not match input stream filename");
    }
    std::vector<std::uint8_t> inputBytes;
    if (!internal::readFileBytes(request.inputPath, inputBytes)) {
        return failCompress(Status::IoError, request, pipeline, "failed to read input file");
    }
    ParsedInput input{};
    std::string parseError;
    const auto parseStatus = parseCompactJsonl(inputBytes, streamType, input, parseError);
    if (!isOk(parseStatus)) return failCompress(parseStatus, request, pipeline, parseError);

    const auto blockBytes = std::max<std::uint32_t>(request.blockBytes, 4096u);
    const auto outputPath = internal::outputPathFor(request, pipeline, streamType);
    std::error_code ec;
    std::filesystem::create_directories(outputPath.parent_path(), ec);
    if (ec) return failCompress(Status::IoError, request, pipeline, "failed to create output directory");

    CompressionResult result{};
    applyPipeline(result, pipeline);
    result.streamType = streamType;
    result.inputPath = request.inputPath;
    result.outputPath = outputPath;
    result.metricsPath = outputPath.parent_path() / (outputPath.stem().string() + ".metrics.json");
    result.inputBytes = static_cast<std::uint64_t>(inputBytes.size());

    std::ofstream out(outputPath, std::ios::binary | std::ios::trunc);
    if (!out) return failCompress(Status::IoError, request, pipeline, "failed to open HFT-MAC output file");

    FileHeader fileHeader{};
    fileHeader.stream = format::streamToWire(streamType);
    fileHeader.schema = fileHeader.stream;
    fileHeader.profile = profileFor(spec.payloadCodec);
    fileHeader.flags = input.finalNewline ? kFlagFinalNewline : 0u;
    fileHeader.blockBytes = blockBytes;
    fileHeader.inputBytes = result.inputBytes;
    fileHeader.recordCount = static_cast<std::uint64_t>(input.records.size());
    fileHeader.firstTsNs = input.firstTsNs;
    fileHeader.lastTsNs = input.lastTsNs;
    const auto placeholderHeader = serializeFileHeader(fileHeader, true);
    out.write(reinterpret_cast<const char*>(placeholderHeader.data()), static_cast<std::streamsize>(placeholderHeader.size()));

    const auto encodeStartNs = timing::nowNs();
    const auto encodeStartCycles = timing::readCycles();
    TransformState transform{};
    std::vector<std::uint8_t> decodedBlock;
    std::vector<std::uint8_t> payload;
    std::uint64_t decodedOffset = 0;
    std::size_t begin = 0;
    while (begin < input.records.size()) {
        const auto end = chooseBlockEnd(input, begin, blockBytes);
        const auto blockRecords = std::span<const Event>{input.records.data() + begin, end - begin};
        buildDecodedBlock(streamType, blockRecords, begin, input.records.size(), input.finalNewline, decodedBlock);
        payload.clear();
        for (const auto& event : blockRecords) {
            if (!encodePayloadEvent(streamType, spec.payloadCodec, event, transform, payload)) {
                return failCompress(Status::InvalidArgument, request, pipeline, "HFT-MAC payload cannot represent record");
            }
        }

        BlockHeader block{};
        block.decodedBytes = static_cast<std::uint32_t>(decodedBlock.size());
        block.payloadBytes = static_cast<std::uint32_t>(payload.size());
        block.recordCount = static_cast<std::uint32_t>(blockRecords.size());
        block.firstByteOffset = decodedOffset;
        block.firstTsNs = blockRecords.front().tsNs;
        block.lastTsNs = blockRecords.back().tsNs;
        block.decodedCrc32c = format::crc32c(decodedBlock);
        block.payloadCrc32c = format::crc32c(payload);
        const auto blockHeader = serializeBlockHeader(block);
        out.write(reinterpret_cast<const char*>(blockHeader.data()), static_cast<std::streamsize>(blockHeader.size()));
        out.write(reinterpret_cast<const char*>(payload.data()), static_cast<std::streamsize>(payload.size()));
        result.outputBytes += kBlockHeaderBytes + static_cast<std::uint64_t>(payload.size());
        result.lineCount += block.recordCount;
        ++result.blockCount;
        decodedOffset += decodedBlock.size();
        begin = end;
    }

    result.outputBytes += kFileHeaderBytes;
    result.encodeCycles = timing::readCycles() - encodeStartCycles;
    result.encodeNs = timing::nowNs() - encodeStartNs;
    fileHeader.outputBytes = result.outputBytes;
    fileHeader.blockCount = result.blockCount;
    fileHeader.headerCrc32c = headerCrc32c(fileHeader);
    const auto finalHeader = serializeFileHeader(fileHeader, true);
    out.seekp(0, std::ios::beg);
    out.write(reinterpret_cast<const char*>(finalHeader.data()), static_cast<std::streamsize>(finalHeader.size()));
    out.close();
    if (!out) return failCompress(Status::IoError, request, pipeline, "failed to write HFT-MAC artifact");

    result.status = Status::Ok;
    result.roundtripOk = true;
    (void)internal::writeTextFile(result.metricsPath, toMetricsJson(result));
    metrics::recordRun(result);
    return result;
}

ReplayArtifactInfo inspectVarintArtifact(const std::filesystem::path& path,
                                         const PipelineDescriptor& pipeline,
                                         const BackendSpec& spec) noexcept {
    if (path.empty()) return failArtifact(path, Status::InvalidArgument, "HFT-MAC artifact path is empty");
    std::ifstream in(path, std::ios::binary);
    if (!in) return failArtifact(path, Status::IoError, "failed to open HFT-MAC artifact");
    FileHeader header{};
    const auto headerStatus = readHeaderFromFile(in, header, spec);
    if (!isOk(headerStatus)) return failArtifact(path, headerStatus, "invalid HFT-MAC header");

    std::uint64_t expectedOffset = 0;
    std::uint64_t fileOffset = kFileHeaderBytes;
    std::vector<std::uint8_t> payload;
    for (std::uint64_t blockIndex = 0; blockIndex < header.blockCount; ++blockIndex) {
        std::array<std::uint8_t, kBlockHeaderBytes> blockBytes{};
        in.read(reinterpret_cast<char*>(blockBytes.data()), static_cast<std::streamsize>(blockBytes.size()));
        if (in.gcount() != static_cast<std::streamsize>(blockBytes.size())) return failArtifact(path, Status::CorruptData, "truncated HFT-MAC block header");
        BlockHeader block{};
        if (!parseBlockHeader(blockBytes.data(), blockBytes.size(), block) || !validBlock(block, expectedOffset)) {
            return failArtifact(path, Status::CorruptData, "invalid HFT-MAC block header");
        }
        payload.resize(block.payloadBytes);
        in.read(reinterpret_cast<char*>(payload.data()), static_cast<std::streamsize>(payload.size()));
        if (in.gcount() != static_cast<std::streamsize>(payload.size())) return failArtifact(path, Status::CorruptData, "truncated HFT-MAC payload");
        if (format::crc32c(payload) != block.payloadCrc32c) return failArtifact(path, Status::CorruptData, "HFT-MAC payload crc mismatch");
        fileOffset += kBlockHeaderBytes + block.payloadBytes;
        expectedOffset += block.decodedBytes;
    }
    if (expectedOffset != header.inputBytes) return failArtifact(path, Status::CorruptData, "HFT-MAC decoded byte count mismatch");
    if (header.outputBytes != 0u && fileOffset != header.outputBytes) return failArtifact(path, Status::CorruptData, "HFT-MAC output byte count mismatch");
    char extra = 0;
    if (in.read(&extra, 1)) return failArtifact(path, Status::CorruptData, "trailing bytes after HFT-MAC blocks");

    ReplayArtifactInfo info{};
    info.status = Status::Ok;
    info.found = true;
    info.path = path;
    info.formatId = std::string{spec.formatId};
    info.pipelineId = std::string{pipeline.id};
    info.transform = std::string{pipeline.transform};
    info.entropy = std::string{pipeline.entropy};
    info.streamType = format::streamFromWire(header.stream);
    info.version = header.version;
    info.inputBytes = header.inputBytes;
    info.outputBytes = header.outputBytes;
    info.lineCount = header.recordCount;
    info.blockCount = header.blockCount;
    return info;
}

Status decodeBlock(const FileHeader& header,
                   const BlockHeader& block,
                   std::span<const std::uint8_t> payload,
                   const BackendSpec& spec,
                   TransformState& transform,
                   std::uint64_t& decodedRecordIndex,
                   std::vector<std::uint8_t>& decoded) {
    if (format::crc32c(payload) != block.payloadCrc32c) return Status::CorruptData;
    const auto* p = payload.data();
    const auto* end = payload.data() + payload.size();
    decoded.clear();
    std::int64_t firstTs = 0;
    std::int64_t lastTs = 0;
    const bool finalNewline = (header.flags & kFlagFinalNewline) != 0u;
    for (std::uint32_t i = 0; i < block.recordCount; ++i) {
        Event event{};
        if (!decodePayloadEvent(spec.streamType, spec.payloadCodec, p, end, transform, event)) return Status::CorruptData;
        if (i == 0u) firstTs = event.tsNs;
        lastTs = event.tsNs;
        const bool newline = finalNewline || decodedRecordIndex + 1u < header.recordCount;
        appendRecordJson(spec.streamType, event, newline, decoded);
        ++decodedRecordIndex;
    }
    if (p != end) return Status::CorruptData;
    if (decoded.size() != block.decodedBytes || format::crc32c(decoded) != block.decodedCrc32c) return Status::CorruptData;
    if (firstTs != block.firstTsNs || lastTs != block.lastTsNs) return Status::CorruptData;
    return Status::Ok;
}

Status decodeVarintBuffer(std::span<const std::uint8_t> bytes,
                          const DecodedBlockCallback& onBlock,
                          const BackendSpec& spec) noexcept {
    if (bytes.size() < kFileHeaderBytes || !onBlock) return Status::InvalidArgument;
    FileHeader header{};
    if (!parseFileHeader(bytes.data(), bytes.size(), header) || !validHeader(header, spec)) return Status::CorruptData;
    if (header.headerCrc32c != headerCrc32c(header)) return Status::CorruptData;
    if (header.outputBytes != 0u && header.outputBytes != bytes.size()) return Status::CorruptData;

    std::size_t offset = kFileHeaderBytes;
    std::uint64_t expectedOffset = 0;
    std::uint64_t decodedRecordIndex = 0;
    TransformState transform{};
    std::vector<std::uint8_t> decoded;
    for (std::uint64_t blockIndex = 0; blockIndex < header.blockCount; ++blockIndex) {
        if (bytes.size() - offset < kBlockHeaderBytes) return Status::CorruptData;
        BlockHeader block{};
        if (!parseBlockHeader(bytes.data() + offset, bytes.size() - offset, block) || !validBlock(block, expectedOffset)) return Status::CorruptData;
        offset += kBlockHeaderBytes;
        if (bytes.size() - offset < block.payloadBytes) return Status::CorruptData;
        const auto status = decodeBlock(header, block, {bytes.data() + offset, block.payloadBytes}, spec,
                                        transform, decodedRecordIndex, decoded);
        if (!isOk(status)) return status;
        offset += block.payloadBytes;
        expectedOffset += block.decodedBytes;
        if (!onBlock(decoded)) return Status::Ok;
    }
    if (offset != bytes.size() || expectedOffset != header.inputBytes || decodedRecordIndex != header.recordCount) return Status::CorruptData;
    return Status::Ok;
}

Status decodeVarintFile(const std::filesystem::path& path,
                        const DecodedBlockCallback& onBlock,
                        const BackendSpec& spec) noexcept {
    if (path.empty() || !onBlock) return Status::InvalidArgument;
    std::vector<std::uint8_t> bytes;
    if (!internal::readFileBytes(path, bytes)) return Status::IoError;
    return decodeVarintBuffer(bytes, onBlock, spec);
}

CompressionResult compressRawBinary(const CompressionRequest& request,
                                    const PipelineDescriptor& pipeline,
                                    const BackendSpec& spec) noexcept {
    return compressVarint(request, pipeline, spec);
}

ReplayArtifactInfo inspectRawBinaryArtifact(const std::filesystem::path& path,
                                            const PipelineDescriptor& pipeline,
                                            const BackendSpec& spec) noexcept {
    return inspectVarintArtifact(path, pipeline, spec);
}

Status decodeRawBinaryBuffer(std::span<const std::uint8_t> bytes,
                             const DecodedBlockCallback& onBlock,
                             const BackendSpec& spec) noexcept {
    return decodeVarintBuffer(bytes, onBlock, spec);
}

Status decodeRawBinaryFile(const std::filesystem::path& path,
                           const DecodedBlockCallback& onBlock,
                           const BackendSpec& spec) noexcept {
    return decodeVarintFile(path, onBlock, spec);
}

}  // namespace hft_compressor::codecs::hftmac_common

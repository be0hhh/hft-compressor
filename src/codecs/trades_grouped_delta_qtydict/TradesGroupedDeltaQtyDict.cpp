#include "codecs/trades_grouped_delta_qtydict/TradesGroupedDeltaQtyDict.hpp"

#include <algorithm>
#include <array>
#include <charconv>
#include <fstream>
#include <numeric>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <unordered_map>
#include <vector>

#include <simdjson.h>

#include "common/CompressionInternals.hpp"
#include "common/timing.hpp"
#include "container/hfc/format.hpp"
#include "hft_compressor/metrics.hpp"

namespace hft_compressor::codecs::trades_grouped_delta_qtydict {
namespace {

constexpr std::uint32_t kFileMagic = 0x46435843u;  // CXCF
constexpr std::uint32_t kChunkMagic = 0x4b484354u; // TCHK
constexpr std::uint16_t kVersionV3 = 3u;
constexpr std::size_t kFileHeaderBytes = 96u;
constexpr std::size_t kChunkHeaderBytes = 160u;

bool isSimdjsonEmpty(simdjson::error_code error) noexcept {
    return error == simdjson::EMPTY || static_cast<int>(error) == 12;
}
constexpr std::uint32_t kDefaultChunkRecords = 16u * 1024u;
constexpr std::uint32_t kHotQtyCount = 64u;
constexpr std::uint32_t kMaxHotQtyCount = 128u;

struct Trade {
    std::int64_t price{0};
    std::int64_t qty{0};
    std::int64_t side{0};
    std::int64_t tsNs{0};
};

struct EncodedChunk {
    std::uint32_t recordCount{0};
    std::uint32_t timeGroupCount{0};
    std::uint32_t priceGroupCount{0};
    std::uint32_t qtyEscapeCount{0};
    std::int64_t firstTsNs{0};
    std::int64_t lastTsNs{0};
    std::int64_t baseTsNs{0};
    std::int64_t basePrice{0};
    std::int64_t baseTsUnit{0};
    std::int64_t basePriceTick{0};
    std::int64_t priceScale{1};
    std::int64_t qtyScale{1};
    std::int64_t timeScale{1};
    std::uint32_t hotQtyCount{0};
    std::uint32_t hotQtyBits{7};
    std::uint32_t hotQtyCapacity{64};
    std::uint32_t dpZeroCount{0};
    std::uint32_t countOneCount{0};
    std::uint32_t priceGroupCountOneCount{0};
    std::vector<std::int64_t> hotQty;
    std::vector<std::uint8_t> hotQtyTableStream;
    std::vector<std::uint8_t> timeStream;
    std::vector<std::uint8_t> priceStream;
    std::vector<std::uint8_t> sideStream;
    std::vector<std::uint8_t> dpZeroStream;
    std::vector<std::uint8_t> countOneStream;
    std::vector<std::uint8_t> priceGroupCountOneStream;
    std::vector<std::uint8_t> qtyCodeStream;
    std::vector<std::uint8_t> qtyEscapeStream;
    std::uint32_t payloadCrc32c{0};
};

struct FileHeader {
    std::uint32_t magic{kFileMagic};
    std::uint16_t version{kVersionV3};
    std::uint16_t stream{0};
    std::uint16_t lineEnding{1};
    std::uint16_t reserved{0};
    std::uint32_t chunkRecords{kDefaultChunkRecords};
    std::uint64_t inputBytes{0};
    std::uint64_t outputBytes{0};
    std::uint64_t recordCount{0};
    std::uint64_t chunkCount{0};
    std::uint64_t timeGroupCount{0};
    std::uint64_t priceGroupCount{0};
    std::uint64_t qtyEscapeCount{0};
    std::uint32_t headerCrc32c{0};
};

struct ChunkHeader {
    std::uint32_t magic{kChunkMagic};
    std::uint32_t recordCount{0};
    std::uint32_t timeGroupCount{0};
    std::uint32_t priceGroupCount{0};
    std::uint32_t qtyEscapeCount{0};
    std::int64_t firstTsNs{0};
    std::int64_t lastTsNs{0};
    std::int64_t baseTsNs{0};
    std::int64_t basePrice{0};
    std::int64_t baseTsUnit{0};
    std::int64_t basePriceTick{0};
    std::int64_t priceScale{1};
    std::int64_t qtyScale{1};
    std::int64_t timeScale{1};
    std::uint32_t hotQtyCount{0};
    std::uint32_t hotQtyBits{7};
    std::uint32_t timeStreamBytes{0};
    std::uint32_t priceStreamBytes{0};
    std::uint32_t sideStreamBytes{0};
    std::uint32_t dpZeroStreamBytes{0};
    std::uint32_t countOneStreamBytes{0};
    std::uint32_t priceGroupCountOneStreamBytes{0};
    std::uint32_t qtyCodeStreamBytes{0};
    std::uint32_t qtyEscapeStreamBytes{0};
    std::uint32_t hotQtyTableBytes{0};
    std::uint32_t dpZeroCount{0};
    std::uint32_t countOneCount{0};
    std::uint32_t priceGroupCountOneCount{0};
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
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < sizeof(T); ++i) value |= static_cast<std::uint64_t>(p[i]) << (i * 8u);
    p += sizeof(T);
    out = static_cast<T>(value);
    return true;
}

void writeVarint(std::vector<std::uint8_t>& out, std::uint64_t value) {
    while (value >= 0x80u) {
        out.push_back(static_cast<std::uint8_t>(value | 0x80u));
        value >>= 7u;
    }
    out.push_back(static_cast<std::uint8_t>(value));
}

bool readVarint(const std::uint8_t*& p, const std::uint8_t* end, std::uint64_t& out) noexcept {
    std::uint64_t value = 0;
    unsigned shift = 0;
    while (p < end && shift <= 63u) {
        const auto byte = *p++;
        value |= static_cast<std::uint64_t>(byte & 0x7fu) << shift;
        if ((byte & 0x80u) == 0u) {
            out = value;
            return true;
        }
        shift += 7u;
    }
    return false;
}

struct BitWriter {
    std::vector<std::uint8_t> bytes;
    std::uint8_t current{0};
    unsigned bitCount{0};

    void writeBits(std::uint64_t value, unsigned bits) {
        for (unsigned i = 0; i < bits; ++i) {
            current |= static_cast<std::uint8_t>(((value >> i) & 1u) << bitCount);
            ++bitCount;
            if (bitCount == 8u) {
                bytes.push_back(current);
                current = 0;
                bitCount = 0;
            }
        }
    }

    std::vector<std::uint8_t> finish() {
        if (bitCount != 0u) {
            bytes.push_back(current);
            current = 0;
            bitCount = 0;
        }
        return std::move(bytes);
    }
};

struct BitReader {
    const std::uint8_t* data{nullptr};
    std::size_t size{0};
    std::size_t byteOffset{0};
    unsigned bitOffset{0};

    bool readBits(unsigned bits, std::uint64_t& out) noexcept {
        std::uint64_t value = 0;
        for (unsigned i = 0; i < bits; ++i) {
            if (byteOffset >= size) return false;
            value |= static_cast<std::uint64_t>((data[byteOffset] >> bitOffset) & 1u) << i;
            ++bitOffset;
            if (bitOffset == 8u) {
                bitOffset = 0;
                ++byteOffset;
            }
        }
        out = value;
        return true;
    }
};

std::uint64_t zigzag(std::int64_t value) noexcept {
    return (static_cast<std::uint64_t>(value) << 1u) ^ static_cast<std::uint64_t>(value >> 63u);
}

std::int64_t unzigzag(std::uint64_t value) noexcept {
    return static_cast<std::int64_t>((value >> 1u) ^ (~(value & 1u) + 1u));
}


std::int64_t gcdPositive(std::int64_t a, std::int64_t b) noexcept {
    auto ua = static_cast<std::uint64_t>(a < 0 ? -a : a);
    auto ub = static_cast<std::uint64_t>(b < 0 ? -b : b);
    return static_cast<std::int64_t>(std::gcd(ua, ub));
}

unsigned bitsForHotCapacity(std::uint32_t capacity) noexcept {
    unsigned bits = 0;
    std::uint32_t value = capacity;
    while (value != 0u) {
        ++bits;
        value >>= 1u;
    }
    return bits == 0u ? 1u : bits;
}

std::int64_t safeScale(std::int64_t value) noexcept {
    return value <= 0 ? 1 : value;
}

std::vector<std::uint8_t> serializeFileHeader(FileHeader header, bool includeCrc) {
    if (!includeCrc) header.headerCrc32c = 0u;
    std::vector<std::uint8_t> out;
    out.reserve(kFileHeaderBytes);
    writeLe(out, header.magic);
    writeLe(out, header.version);
    writeLe(out, header.stream);
    writeLe(out, header.lineEnding);
    writeLe(out, header.reserved);
    writeLe(out, header.chunkRecords);
    writeLe(out, header.inputBytes);
    writeLe(out, header.outputBytes);
    writeLe(out, header.recordCount);
    writeLe(out, header.chunkCount);
    writeLe(out, header.timeGroupCount);
    writeLe(out, header.priceGroupCount);
    writeLe(out, header.qtyEscapeCount);
    writeLe(out, header.headerCrc32c);
    out.resize(kFileHeaderBytes, 0u);
    return out;
}

std::vector<std::uint8_t> serializeChunkHeader(const ChunkHeader& header) {
    std::vector<std::uint8_t> out;
    out.reserve(kChunkHeaderBytes);
    writeLe(out, header.magic);
    writeLe(out, header.recordCount);
    writeLe(out, header.timeGroupCount);
    writeLe(out, header.priceGroupCount);
    writeLe(out, header.qtyEscapeCount);
    writeLe(out, header.firstTsNs);
    writeLe(out, header.lastTsNs);
    writeLe(out, header.baseTsNs);
    writeLe(out, header.basePrice);
    writeLe(out, header.baseTsUnit);
    writeLe(out, header.basePriceTick);
    writeLe(out, header.priceScale);
    writeLe(out, header.qtyScale);
    writeLe(out, header.timeScale);
    writeLe(out, header.hotQtyCount);
    writeLe(out, header.hotQtyBits);
    writeLe(out, header.timeStreamBytes);
    writeLe(out, header.priceStreamBytes);
    writeLe(out, header.sideStreamBytes);
    writeLe(out, header.dpZeroStreamBytes);
    writeLe(out, header.countOneStreamBytes);
    writeLe(out, header.priceGroupCountOneStreamBytes);
    writeLe(out, header.qtyCodeStreamBytes);
    writeLe(out, header.qtyEscapeStreamBytes);
    writeLe(out, header.hotQtyTableBytes);
    writeLe(out, header.dpZeroCount);
    writeLe(out, header.countOneCount);
    writeLe(out, header.priceGroupCountOneCount);
    writeLe(out, header.payloadCrc32c);
    out.resize(kChunkHeaderBytes, 0u);
    return out;
}

bool parseFileHeader(const std::uint8_t* data, std::size_t size, FileHeader& out) noexcept {
    if (data == nullptr || size < kFileHeaderBytes) return false;
    const auto* p = data;
    const auto* end = data + kFileHeaderBytes;
    return readLe(p, end, out.magic)
        && readLe(p, end, out.version)
        && readLe(p, end, out.stream)
        && readLe(p, end, out.lineEnding)
        && readLe(p, end, out.reserved)
        && readLe(p, end, out.chunkRecords)
        && readLe(p, end, out.inputBytes)
        && readLe(p, end, out.outputBytes)
        && readLe(p, end, out.recordCount)
        && readLe(p, end, out.chunkCount)
        && readLe(p, end, out.timeGroupCount)
        && readLe(p, end, out.priceGroupCount)
        && readLe(p, end, out.qtyEscapeCount)
        && readLe(p, end, out.headerCrc32c);
}

bool parseChunkHeader(const std::uint8_t* data, std::size_t size, ChunkHeader& out) noexcept {
    if (data == nullptr || size < kChunkHeaderBytes) return false;
    const auto* p = data;
    const auto* end = data + kChunkHeaderBytes;
    return readLe(p, end, out.magic)
        && readLe(p, end, out.recordCount)
        && readLe(p, end, out.timeGroupCount)
        && readLe(p, end, out.priceGroupCount)
        && readLe(p, end, out.qtyEscapeCount)
        && readLe(p, end, out.firstTsNs)
        && readLe(p, end, out.lastTsNs)
        && readLe(p, end, out.baseTsNs)
        && readLe(p, end, out.basePrice)
        && readLe(p, end, out.baseTsUnit)
        && readLe(p, end, out.basePriceTick)
        && readLe(p, end, out.priceScale)
        && readLe(p, end, out.qtyScale)
        && readLe(p, end, out.timeScale)
        && readLe(p, end, out.hotQtyCount)
        && readLe(p, end, out.hotQtyBits)
        && readLe(p, end, out.timeStreamBytes)
        && readLe(p, end, out.priceStreamBytes)
        && readLe(p, end, out.sideStreamBytes)
        && readLe(p, end, out.dpZeroStreamBytes)
        && readLe(p, end, out.countOneStreamBytes)
        && readLe(p, end, out.priceGroupCountOneStreamBytes)
        && readLe(p, end, out.qtyCodeStreamBytes)
        && readLe(p, end, out.qtyEscapeStreamBytes)
        && readLe(p, end, out.hotQtyTableBytes)
        && readLe(p, end, out.dpZeroCount)
        && readLe(p, end, out.countOneCount)
        && readLe(p, end, out.priceGroupCountOneCount)
        && readLe(p, end, out.payloadCrc32c);
}


std::uint32_t headerCrc32c(const FileHeader& header) {
    return format::crc32c(serializeFileHeader(header, false));
}

bool validHeader(const FileHeader& header) noexcept {
    return header.magic == kFileMagic
        && header.version == kVersionV3
        && format::streamFromWire(header.stream) == StreamType::Trades
        && (header.lineEnding == 1u || header.lineEnding == 2u)
        && header.reserved == 0u
        && header.chunkRecords != 0u;
}

std::uint16_t detectLineEnding(std::span<const std::uint8_t> input) noexcept {
    for (std::size_t i = 0; i < input.size(); ++i) {
        if (input[i] != static_cast<std::uint8_t>('\n')) continue;
        return i > 0u && input[i - 1u] == static_cast<std::uint8_t>('\r') ? 2u : 1u;
    }
    return 1u;
}

bool validChunkHeader(const ChunkHeader& header) noexcept {
    return header.magic == kChunkMagic
        && header.recordCount != 0u
        && header.timeGroupCount != 0u
        && header.priceGroupCount != 0u
        && header.hotQtyCount <= kMaxHotQtyCount
        && header.hotQtyBits <= 8u;
}

struct JsonCursor {
    std::string_view text{};
    std::size_t pos{0};

    void skipSpaces() noexcept {
        while (pos < text.size()) {
            const char c = text[pos];
            if (c != ' ' && c != '\t' && c != '\r') break;
            ++pos;
        }
    }

    bool consume(char c) noexcept {
        skipSpaces();
        if (pos >= text.size() || text[pos] != c) return false;
        ++pos;
        return true;
    }

    bool parseInt64(std::int64_t& out) noexcept {
        skipSpaces();
        if (pos >= text.size()) return false;
        const char* begin = text.data() + pos;
        const char* end = text.data() + text.size();
        const auto [ptr, ec] = std::from_chars(begin, end, out);
        if (ec != std::errc{} || ptr == begin) return false;
        pos = static_cast<std::size_t>(ptr - text.data());
        return true;
    }

    bool finish() noexcept {
        skipSpaces();
        return pos == text.size();
    }
};

bool parseTradeLine(std::string_view line, Trade& out) noexcept {
    JsonCursor p{line};
    return p.consume('[')
        && p.parseInt64(out.price) && p.consume(',')
        && p.parseInt64(out.qty) && p.consume(',')
        && p.parseInt64(out.side) && p.consume(',')
        && p.parseInt64(out.tsNs)
        && p.consume(']') && p.finish()
        && out.price > 0 && out.qty > 0 && (out.side == 0 || out.side == 1);
}

bool parseTrades(std::span<const std::uint8_t> input, std::vector<Trade>& out) {
    simdjson::dom::parser parser;
    simdjson::padded_string padded{reinterpret_cast<const char*>(input.data()), input.size()};
    auto docs = parser.parse_many(padded.data(), padded.size(), padded.size());
    std::int64_t previousTs = 0;
    bool havePrevious = false;
    for (auto docResult : docs) {
        simdjson::dom::element doc;
        const auto docError = docResult.get(doc);
        if (isSimdjsonEmpty(docError)) continue;
        if (docError != simdjson::SUCCESS || !doc.is_array()) return false;
        simdjson::dom::array values;
        if (doc.get_array().get(values) != simdjson::SUCCESS || values.size() != 4u) return false;
        Trade trade{};
        std::size_t index = 0;
        for (auto value : values) {
            std::int64_t parsed = 0;
            if (value.get_int64().get(parsed) != simdjson::SUCCESS) return false;
            if (index == 0u) trade.price = parsed;
            else if (index == 1u) trade.qty = parsed;
            else if (index == 2u) trade.side = parsed;
            else trade.tsNs = parsed;
            ++index;
        }
        if (trade.price <= 0 || trade.qty <= 0 || (trade.side != 0 && trade.side != 1)) return false;
        if (havePrevious && trade.tsNs < previousTs) return false;
        previousTs = trade.tsNs;
        havePrevious = true;
        out.push_back(trade);
    }
    return !out.empty();
}

void buildHotQty(const std::vector<Trade>& trades, std::size_t begin, std::size_t end, EncodedChunk& chunk, bool useV2) {
    std::unordered_map<std::int64_t, std::uint32_t> counts;
    for (std::size_t i = begin; i < end; ++i) ++counts[useV2 ? trades[i].qty / chunk.qtyScale : trades[i].qty];
    std::vector<std::pair<std::int64_t, std::uint32_t>> values;
    values.reserve(counts.size());
    for (const auto& item : counts) values.push_back(item);
    std::sort(values.begin(), values.end(), [](const auto& a, const auto& b) {
        if (a.second != b.second) return a.second > b.second;
        return a.first < b.first;
    });

    std::array<std::uint32_t, 4> capacities{16u, 32u, 64u, 128u};
    std::uint64_t bestCost = UINT64_MAX;
    std::uint32_t bestCapacity = useV2 ? 16u : kHotQtyCount;
    if (!useV2) {
        chunk.hotQtyCapacity = kHotQtyCount;
        chunk.hotQtyBits = 7u;
        chunk.hotQtyCount = static_cast<std::uint32_t>(std::min<std::size_t>(values.size(), kHotQtyCount));
        chunk.hotQty.reserve(chunk.hotQtyCount);
        for (std::uint32_t i = 0; i < chunk.hotQtyCount; ++i) chunk.hotQty.push_back(values[i].first);
        return;
    }

    for (const auto capacity : capacities) {
        const auto hotCount = std::min<std::size_t>(values.size(), capacity);
        std::uint64_t hotHits = 0;
        for (std::size_t i = 0; i < hotCount; ++i) hotHits += values[i].second;
        const auto escapes = static_cast<std::uint64_t>(end - begin) - hotHits;
        const auto bits = bitsForHotCapacity(capacity);
        std::uint64_t escapeBytes = 0;
        for (std::size_t i = hotCount; i < values.size(); ++i) {
            std::vector<std::uint8_t> tmp;
            writeVarint(tmp, static_cast<std::uint64_t>(values[i].first));
            escapeBytes += static_cast<std::uint64_t>(tmp.size()) * values[i].second;
        }
        const auto tableBytesEstimate = hotCount * 2u;
        const auto codeBytes = ((static_cast<std::uint64_t>(end - begin) * bits) + 7u) / 8u;
        const auto cost = codeBytes + escapeBytes + tableBytesEstimate + escapes;
        if (cost < bestCost) {
            bestCost = cost;
            bestCapacity = capacity;
        }
    }

    chunk.hotQtyCapacity = bestCapacity;
    chunk.hotQtyBits = bitsForHotCapacity(bestCapacity);
    chunk.hotQtyCount = static_cast<std::uint32_t>(std::min<std::size_t>(values.size(), bestCapacity));
    chunk.hotQty.reserve(chunk.hotQtyCount);
    for (std::uint32_t i = 0; i < chunk.hotQtyCount; ++i) chunk.hotQty.push_back(values[i].first);
}

std::uint64_t qtyCode(const EncodedChunk& chunk, std::int64_t qty) noexcept {
    for (std::uint32_t i = 0; i < chunk.hotQtyCount; ++i) {
        if (chunk.hotQty[i] == qty) return i;
    }
    return chunk.hotQtyCapacity;
}

EncodedChunk encodeChunk(const std::vector<Trade>& trades, std::size_t begin, std::size_t end) {
    EncodedChunk chunk{};
    chunk.recordCount = static_cast<std::uint32_t>(end - begin);
    chunk.firstTsNs = trades[begin].tsNs;
    chunk.lastTsNs = trades[end - 1u].tsNs;
    chunk.baseTsNs = trades[begin].tsNs;
    chunk.basePrice = trades[begin].price;

    {
        std::int64_t priceScale = 0;
        std::int64_t qtyScale = 0;
        std::int64_t timeScale = chunk.baseTsNs;
        for (std::size_t i = begin; i < end; ++i) {
            priceScale = gcdPositive(priceScale, trades[i].price);
            qtyScale = gcdPositive(qtyScale, trades[i].qty);
            timeScale = gcdPositive(timeScale, trades[i].tsNs - chunk.baseTsNs);
        }
        chunk.priceScale = safeScale(priceScale);
        chunk.qtyScale = safeScale(qtyScale);
        chunk.timeScale = safeScale(timeScale);
        chunk.baseTsUnit = chunk.baseTsNs / chunk.timeScale;
        chunk.basePriceTick = chunk.basePrice / chunk.priceScale;
    }

    buildHotQty(trades, begin, end, chunk, true);
    for (const auto qty : chunk.hotQty) writeVarint(chunk.hotQtyTableStream, static_cast<std::uint64_t>(qty));

    std::int64_t previousGroupTs = chunk.baseTsUnit;
    std::int64_t previousPrice = chunk.basePriceTick;
    BitWriter sideBits;
    BitWriter dpZeroBits;
    BitWriter countOneBits;
    BitWriter priceGroupCountOneBits;
    BitWriter qtyCodeBits;
    for (std::size_t i = begin; i < end;) {
        const auto rawTs = trades[i].tsNs;
        const auto ts = rawTs / chunk.timeScale;
        std::uint32_t priceGroups = 0;
        while (i < end && trades[i].tsNs == rawTs) {
            const auto rawPrice = trades[i].price;
            const auto price = rawPrice / chunk.priceScale;
            const auto side = trades[i].side;
            std::uint32_t tradeCount = 0;
            while (i < end && trades[i].tsNs == rawTs && trades[i].price == rawPrice && trades[i].side == side) {
                ++tradeCount;
                ++i;
            }
            ++priceGroups;
            ++chunk.priceGroupCount;
            const auto dp = price - previousPrice;
            const bool dpZero = dp == 0;
            const bool countOne = tradeCount == 1u;
            dpZeroBits.writeBits(dpZero ? 1u : 0u, 1u);
            countOneBits.writeBits(countOne ? 1u : 0u, 1u);
            if (dpZero) ++chunk.dpZeroCount;
            else writeVarint(chunk.priceStream, zigzag(dp));
            if (countOne) {
                ++chunk.countOneCount;
            } else {
                writeVarint(chunk.priceStream, static_cast<std::uint64_t>(tradeCount - 2u));
            }
            sideBits.writeBits(static_cast<std::uint64_t>(side), 1u);
            previousPrice = price;
            for (std::size_t q = i - tradeCount; q < i; ++q) {
                const auto qty = trades[q].qty / chunk.qtyScale;
                const auto code = qtyCode(chunk, qty);
                qtyCodeBits.writeBits(code, chunk.hotQtyBits);
                if (code == chunk.hotQtyCapacity) {
                    writeVarint(chunk.qtyEscapeStream, static_cast<std::uint64_t>(qty));
                    ++chunk.qtyEscapeCount;
                }
            }
        }
        ++chunk.timeGroupCount;
        writeVarint(chunk.timeStream, static_cast<std::uint64_t>(ts - previousGroupTs));
        const bool onePriceGroup = priceGroups == 1u;
        priceGroupCountOneBits.writeBits(onePriceGroup ? 1u : 0u, 1u);
        if (onePriceGroup) ++chunk.priceGroupCountOneCount;
        else writeVarint(chunk.timeStream, priceGroups);
        previousGroupTs = ts;
    }
    chunk.sideStream = sideBits.finish();
    chunk.dpZeroStream = dpZeroBits.finish();
    chunk.countOneStream = countOneBits.finish();
    chunk.priceGroupCountOneStream = priceGroupCountOneBits.finish();
    chunk.qtyCodeStream = qtyCodeBits.finish();

    std::vector<std::uint8_t> payload;
    payload.reserve(chunk.hotQtyTableStream.size() + chunk.timeStream.size() + chunk.priceStream.size() + chunk.sideStream.size()
        + chunk.dpZeroStream.size() + chunk.countOneStream.size() + chunk.priceGroupCountOneStream.size()
        + chunk.qtyCodeStream.size() + chunk.qtyEscapeStream.size());
    payload.insert(payload.end(), chunk.hotQtyTableStream.begin(), chunk.hotQtyTableStream.end());
    payload.insert(payload.end(), chunk.timeStream.begin(), chunk.timeStream.end());
    payload.insert(payload.end(), chunk.priceStream.begin(), chunk.priceStream.end());
    payload.insert(payload.end(), chunk.sideStream.begin(), chunk.sideStream.end());
    payload.insert(payload.end(), chunk.dpZeroStream.begin(), chunk.dpZeroStream.end());
    payload.insert(payload.end(), chunk.countOneStream.begin(), chunk.countOneStream.end());
    payload.insert(payload.end(), chunk.priceGroupCountOneStream.begin(), chunk.priceGroupCountOneStream.end());
    payload.insert(payload.end(), chunk.qtyCodeStream.begin(), chunk.qtyCodeStream.end());
    payload.insert(payload.end(), chunk.qtyEscapeStream.begin(), chunk.qtyEscapeStream.end());
    chunk.payloadCrc32c = format::crc32c(payload);
    return chunk;
}

void writeChunk(std::ofstream& out, const EncodedChunk& chunk) {
    ChunkHeader header{};
    header.recordCount = chunk.recordCount;
    header.timeGroupCount = chunk.timeGroupCount;
    header.priceGroupCount = chunk.priceGroupCount;
    header.qtyEscapeCount = chunk.qtyEscapeCount;
    header.firstTsNs = chunk.firstTsNs;
    header.lastTsNs = chunk.lastTsNs;
    header.baseTsNs = chunk.baseTsNs;
    header.basePrice = chunk.basePrice;
    header.baseTsUnit = chunk.baseTsUnit;
    header.basePriceTick = chunk.basePriceTick;
    header.priceScale = chunk.priceScale;
    header.qtyScale = chunk.qtyScale;
    header.timeScale = chunk.timeScale;
    header.hotQtyCount = chunk.hotQtyCount;
    header.hotQtyBits = chunk.hotQtyBits;
    header.timeStreamBytes = static_cast<std::uint32_t>(chunk.timeStream.size());
    header.priceStreamBytes = static_cast<std::uint32_t>(chunk.priceStream.size());
    header.sideStreamBytes = static_cast<std::uint32_t>(chunk.sideStream.size());
    header.dpZeroStreamBytes = static_cast<std::uint32_t>(chunk.dpZeroStream.size());
    header.countOneStreamBytes = static_cast<std::uint32_t>(chunk.countOneStream.size());
    header.priceGroupCountOneStreamBytes = static_cast<std::uint32_t>(chunk.priceGroupCountOneStream.size());
    header.qtyCodeStreamBytes = static_cast<std::uint32_t>(chunk.qtyCodeStream.size());
    header.qtyEscapeStreamBytes = static_cast<std::uint32_t>(chunk.qtyEscapeStream.size());
    header.hotQtyTableBytes = static_cast<std::uint32_t>(chunk.hotQtyTableStream.size());
    header.dpZeroCount = chunk.dpZeroCount;
    header.countOneCount = chunk.countOneCount;
    header.priceGroupCountOneCount = chunk.priceGroupCountOneCount;
    header.payloadCrc32c = chunk.payloadCrc32c;
    const auto headerBytes = serializeChunkHeader(header);
    out.write(reinterpret_cast<const char*>(headerBytes.data()), static_cast<std::streamsize>(headerBytes.size()));
    out.write(reinterpret_cast<const char*>(chunk.hotQtyTableStream.data()), static_cast<std::streamsize>(chunk.hotQtyTableStream.size()));
    out.write(reinterpret_cast<const char*>(chunk.timeStream.data()), static_cast<std::streamsize>(chunk.timeStream.size()));
    out.write(reinterpret_cast<const char*>(chunk.priceStream.data()), static_cast<std::streamsize>(chunk.priceStream.size()));
    out.write(reinterpret_cast<const char*>(chunk.sideStream.data()), static_cast<std::streamsize>(chunk.sideStream.size()));
    out.write(reinterpret_cast<const char*>(chunk.dpZeroStream.data()), static_cast<std::streamsize>(chunk.dpZeroStream.size()));
    out.write(reinterpret_cast<const char*>(chunk.countOneStream.data()), static_cast<std::streamsize>(chunk.countOneStream.size()));
    out.write(reinterpret_cast<const char*>(chunk.priceGroupCountOneStream.data()), static_cast<std::streamsize>(chunk.priceGroupCountOneStream.size()));
    out.write(reinterpret_cast<const char*>(chunk.qtyCodeStream.data()), static_cast<std::streamsize>(chunk.qtyCodeStream.size()));
    out.write(reinterpret_cast<const char*>(chunk.qtyEscapeStream.data()), static_cast<std::streamsize>(chunk.qtyEscapeStream.size()));
}

ReplayArtifactInfo failArtifact(const std::filesystem::path& path, Status status, std::string error) {
    ReplayArtifactInfo info{};
    info.status = status;
    info.path = path;
    info.error = std::move(error);
    return info;
}

bool readFile(const std::filesystem::path& path, std::vector<std::uint8_t>& out) noexcept {
    return internal::readFileBytes(path, out);
}

Status decodeChunk(
                   const ChunkHeader& header,
                   std::span<const std::int64_t> hotQty,
                   std::span<const std::uint8_t> timeStream,
                   std::span<const std::uint8_t> priceStream,
                   std::span<const std::uint8_t> sideStream,
                   std::span<const std::uint8_t> dpZeroStream,
                   std::span<const std::uint8_t> countOneStream,
                   std::span<const std::uint8_t> priceGroupCountOneStream,
                   std::span<const std::uint8_t> qtyCodeStream,
                   std::span<const std::uint8_t> qtyEscapeStream,
                   std::string_view lineEnding,
                   std::string* jsonlOut,
                   std::ostream* encodedJsonOut) noexcept {
    const auto* time = timeStream.data();
    const auto* timeEnd = timeStream.data() + timeStream.size();
    const auto* price = priceStream.data();
    const auto* priceEnd = priceStream.data() + priceStream.size();
    const auto* qtyEscape = qtyEscapeStream.data();
    const auto* qtyEscapeEnd = qtyEscapeStream.data() + qtyEscapeStream.size();
    BitReader sideBits{sideStream.data(), sideStream.size()};
    BitReader dpZeroBits{dpZeroStream.data(), dpZeroStream.size()};
    BitReader countOneBits{countOneStream.data(), countOneStream.size()};
    BitReader priceGroupCountOneBits{priceGroupCountOneStream.data(), priceGroupCountOneStream.size()};
    BitReader qtyCodeBits{qtyCodeStream.data(), qtyCodeStream.size()};

    const auto priceScale = header.priceScale;
    const auto qtyScale = header.qtyScale;
    const auto timeScale = header.timeScale;
    std::int64_t ts = header.baseTsUnit;
    std::int64_t previousPrice = header.basePriceTick;
    std::uint32_t records = 0;
    std::uint32_t priceGroups = 0;
    std::uint32_t qtyEscapes = 0;
    if (encodedJsonOut != nullptr) {
        *encodedJsonOut << "[\n      [" << header.baseTsNs << ", " << header.basePrice << ", [";
        for (std::size_t i = 0; i < hotQty.size(); ++i) {
            if (i != 0u) *encodedJsonOut << ", ";
            *encodedJsonOut << hotQty[i];
        }
        *encodedJsonOut << "]],\n      [\n";
    }
    for (std::uint32_t tg = 0; tg < header.timeGroupCount; ++tg) {
        std::uint64_t dt = 0;
        std::uint64_t groupCount = 0;
        if (!readVarint(time, timeEnd, dt)) return Status::CorruptData;
        std::uint64_t oneGroup = 0;
        if (!priceGroupCountOneBits.readBits(1u, oneGroup)) return Status::CorruptData;
        if (oneGroup != 0u) groupCount = 1u;
        else if (!readVarint(time, timeEnd, groupCount)) return Status::CorruptData;
        ts += static_cast<std::int64_t>(dt);
        if (encodedJsonOut != nullptr) {
            if (tg != 0u) *encodedJsonOut << ",\n";
            *encodedJsonOut << "        [" << dt << ", [";
        }
        for (std::uint64_t pg = 0; pg < groupCount; ++pg) {
            std::uint64_t zz = 0;
            std::uint64_t side = 0;
            std::uint64_t tradeCount = 0;
            std::int64_t dp = 0;
            if (!sideBits.readBits(1u, side)) return Status::CorruptData;
            std::uint64_t dpZero = 0;
            std::uint64_t countOne = 0;
            if (!dpZeroBits.readBits(1u, dpZero) || !countOneBits.readBits(1u, countOne)) return Status::CorruptData;
            if (dpZero == 0u) {
                if (!readVarint(price, priceEnd, zz)) return Status::CorruptData;
                dp = unzigzag(zz);
            }
            if (countOne != 0u) {
                tradeCount = 1u;
            } else if (!readVarint(price, priceEnd, tradeCount)) {
                return Status::CorruptData;
            } else {
                tradeCount += 2u;
            }
            if (side > 1u || tradeCount == 0u) return Status::CorruptData;
            const auto currentPrice = previousPrice + dp;
            previousPrice = currentPrice;
            if (encodedJsonOut != nullptr) {
                if (pg != 0u) *encodedJsonOut << ", ";
                *encodedJsonOut << '[' << dp << ", " << side << ", " << tradeCount << ", [";
            }
            for (std::uint64_t i = 0; i < tradeCount; ++i) {
                std::uint64_t code = 0;
                if (!qtyCodeBits.readBits(header.hotQtyBits, code)) return Status::CorruptData;
                std::int64_t qty = 0;
                if (code < hotQty.size()) {
                    qty = hotQty[static_cast<std::size_t>(code)];
                } else if (code == ((1u << header.hotQtyBits) >> 1u)) {
                    std::uint64_t rawQty = 0;
                    if (!readVarint(qtyEscape, qtyEscapeEnd, rawQty)) return Status::CorruptData;
                    qty = static_cast<std::int64_t>(rawQty);
                    ++qtyEscapes;
                } else {
                    return Status::CorruptData;
                }
                if (encodedJsonOut != nullptr) {
                    if (i != 0u) *encodedJsonOut << ", ";
                    *encodedJsonOut << code;
                }
                if (jsonlOut != nullptr) {
                    jsonlOut->append("[");
                    jsonlOut->append(std::to_string(currentPrice * priceScale));
                    jsonlOut->append(",");
                    jsonlOut->append(std::to_string(qty * qtyScale));
                    jsonlOut->append(",");
                    jsonlOut->append(std::to_string(side));
                    jsonlOut->append(",");
                    jsonlOut->append(std::to_string(ts * timeScale));
                    jsonlOut->append("]");
                    jsonlOut->append(lineEnding);
                }
                ++records;
            }
            if (encodedJsonOut != nullptr) {
                *encodedJsonOut << "]]";
            }
            ++priceGroups;
        }
        if (encodedJsonOut != nullptr) *encodedJsonOut << "]]";
    }
    if (encodedJsonOut != nullptr) *encodedJsonOut << "\n      ]\n    ]";
    if (time != timeEnd || price != priceEnd || qtyEscape != qtyEscapeEnd) return Status::CorruptData;
    if (records != header.recordCount || priceGroups != header.priceGroupCount || qtyEscapes != header.qtyEscapeCount) return Status::CorruptData;
    return Status::Ok;
}

Status walkFile(std::span<const std::uint8_t> file,
                const DecodedBlockCallback* onJsonl,
                std::ostream* encodedJsonOut,
                std::ostream* binaryDumpOut,
                FileHeader* parsedHeader = nullptr) noexcept {
    if (file.size() < kFileHeaderBytes) return Status::InvalidArgument;
    FileHeader header{};
    if (!parseFileHeader(file.data(), file.size(), header) || !validHeader(header)) return Status::CorruptData;
    if (header.headerCrc32c != headerCrc32c(header)) return Status::CorruptData;
    if (header.outputBytes != 0u && header.outputBytes != file.size()) return Status::CorruptData;
    if (parsedHeader != nullptr) *parsedHeader = header;
    if (encodedJsonOut != nullptr) {
        *encodedJsonOut << "{\n"
                        << "  \"schema\": {\n"
                        << "    \"chunk\": \"[chunk_index, record_count, encoded]\",\n"
                        << "    \"encoded\": \"[[base_ts, base_price, hot_qty], [time_group...]]\",\n"
                        << "    \"time_group\": \"[dt, [[dp, side, count, qty_codes]...]]\"\n"
                        << "  },\n"
                        << "  \"chunks\": [\n";
    }
    if (binaryDumpOut != nullptr) {
        *binaryDumpOut << "{\"file_header_bytes\":" << kFileHeaderBytes << ",\"chunks\":[";
    }
    std::size_t offset = kFileHeaderBytes;
    std::uint64_t records = 0;
    for (std::uint64_t chunkIndex = 0; chunkIndex < header.chunkCount; ++chunkIndex) {
        if (file.size() - offset < kChunkHeaderBytes) return Status::CorruptData;
        ChunkHeader chunk{};
        if (!parseChunkHeader(file.data() + offset, file.size() - offset, chunk) || !validChunkHeader(chunk)) return Status::CorruptData;
        const auto chunkStart = offset;
        offset += kChunkHeaderBytes;
        std::size_t payloadStart = offset;
        std::vector<std::int64_t> hotQty;
        hotQty.reserve(chunk.hotQtyCount);
        if (file.size() - offset < chunk.hotQtyTableBytes) return Status::CorruptData;
        const auto* hotQtyPtr = file.data() + offset;
        const auto* hotQtyEnd = hotQtyPtr + chunk.hotQtyTableBytes;
        for (std::uint32_t i = 0; i < chunk.hotQtyCount; ++i) {
            std::uint64_t qty = 0;
            if (!readVarint(hotQtyPtr, hotQtyEnd, qty)) return Status::CorruptData;
            hotQty.push_back(static_cast<std::int64_t>(qty));
        }
        if (hotQtyPtr != hotQtyEnd) return Status::CorruptData;
        offset += chunk.hotQtyTableBytes;
        const auto streamsSize = static_cast<std::size_t>(chunk.timeStreamBytes)
            + chunk.priceStreamBytes + chunk.sideStreamBytes + chunk.dpZeroStreamBytes + chunk.countOneStreamBytes
            + chunk.priceGroupCountOneStreamBytes + chunk.qtyCodeStreamBytes + chunk.qtyEscapeStreamBytes;
        if (file.size() - offset < streamsSize) return Status::CorruptData;
        const auto payloadSize = (offset - payloadStart) + streamsSize;
        const auto* payloadData = file.data() + payloadStart;
        std::vector<std::uint8_t> payload;
        payload.insert(payload.end(), payloadData, payloadData + payloadSize);
        if (format::crc32c(payload) != chunk.payloadCrc32c) return Status::CorruptData;

        std::size_t streamOffset = offset;
        std::span<const std::uint8_t> timeStream{file.data() + streamOffset, chunk.timeStreamBytes};
        streamOffset += chunk.timeStreamBytes;
        std::span<const std::uint8_t> priceStream{file.data() + streamOffset, chunk.priceStreamBytes};
        streamOffset += chunk.priceStreamBytes;
        std::span<const std::uint8_t> sideStream{file.data() + streamOffset, chunk.sideStreamBytes};
        streamOffset += chunk.sideStreamBytes;
        std::span<const std::uint8_t> dpZeroStream{file.data() + streamOffset, chunk.dpZeroStreamBytes};
        streamOffset += chunk.dpZeroStreamBytes;
        std::span<const std::uint8_t> countOneStream{file.data() + streamOffset, chunk.countOneStreamBytes};
        streamOffset += chunk.countOneStreamBytes;
        std::span<const std::uint8_t> priceGroupCountOneStream{file.data() + streamOffset, chunk.priceGroupCountOneStreamBytes};
        streamOffset += chunk.priceGroupCountOneStreamBytes;
        std::span<const std::uint8_t> qtyCodeStream{file.data() + streamOffset, chunk.qtyCodeStreamBytes};
        streamOffset += chunk.qtyCodeStreamBytes;
        std::span<const std::uint8_t> qtyEscapeStream{file.data() + streamOffset, chunk.qtyEscapeStreamBytes};

        if (encodedJsonOut != nullptr) {
            if (chunkIndex != 0u) *encodedJsonOut << ",\n";
            *encodedJsonOut << "    [" << chunkIndex << ", " << chunk.recordCount << ", ";
        }
        std::string jsonl;
        jsonl.reserve(static_cast<std::size_t>(chunk.recordCount) * 32u);
        const auto decodeStatus = decodeChunk(
                                              chunk,
                                              {hotQty.data(), hotQty.size()},
                                              timeStream,
                                              priceStream,
                                              sideStream,
                                              dpZeroStream,
                                              countOneStream,
                                              priceGroupCountOneStream,
                                              qtyCodeStream,
                                              qtyEscapeStream,
                                              header.lineEnding == 2u ? std::string_view{"\r\n"} : std::string_view{"\n"},
                                              onJsonl == nullptr ? nullptr : &jsonl,
                                              encodedJsonOut);
        if (!isOk(decodeStatus)) return decodeStatus;
        if (encodedJsonOut != nullptr) *encodedJsonOut << "\n    ]";
        if (onJsonl != nullptr && !jsonl.empty()) {
            if (!(*onJsonl)(std::span<const std::uint8_t>{reinterpret_cast<const std::uint8_t*>(jsonl.data()), jsonl.size()})) return Status::Ok;
        }
        if (binaryDumpOut != nullptr) {
            if (chunkIndex != 0u) *binaryDumpOut << ',';
            *binaryDumpOut << "{\"chunk\":" << chunkIndex
                           << ",\"chunk_header_offset\":" << chunkStart
                           << ",\"time_stream_bytes\":" << chunk.timeStreamBytes
                           << ",\"price_stream_bytes\":" << chunk.priceStreamBytes
                           << ",\"side_stream_bytes\":" << chunk.sideStreamBytes
                           << ",\"dp_zero_stream_bytes\":" << chunk.dpZeroStreamBytes
                           << ",\"count_one_stream_bytes\":" << chunk.countOneStreamBytes
                           << ",\"qty_code_stream_bytes\":" << chunk.qtyCodeStreamBytes
                           << ",\"qty_escape_stream_bytes\":" << chunk.qtyEscapeStreamBytes
                           << ",\"checksum\":" << chunk.payloadCrc32c << '}';
        }
        records += chunk.recordCount;
        offset += streamsSize;
    }
    if (offset != file.size()) return Status::CorruptData;
    if (records != header.recordCount) return Status::CorruptData;
    if (encodedJsonOut != nullptr) *encodedJsonOut << "\n  ]\n}\n";
    if (binaryDumpOut != nullptr) *binaryDumpOut << "]}\n";
    return Status::Ok;
}

Status writeStringBlock(const std::string& text, const DecodedBlockCallback& onBlock) noexcept {
    if (!onBlock) return Status::InvalidArgument;
    return onBlock(std::span<const std::uint8_t>{reinterpret_cast<const std::uint8_t*>(text.data()), text.size()})
        ? Status::Ok
        : Status::CallbackStopped;
}

}  // namespace

CompressionResult compress(const CompressionRequest& request, const PipelineDescriptor& pipeline) noexcept {
    if (request.inputPath.empty()) {
        auto result = internal::fail(Status::InvalidArgument, request, &pipeline, "input path is empty");
        metrics::recordRun(result);
        return result;
    }
    if (inferStreamTypeFromPath(request.inputPath) != StreamType::Trades) {
        auto result = internal::fail(Status::UnsupportedStream, request, &pipeline, "expected trades.jsonl");
        metrics::recordRun(result);
        return result;
    }
    CompressionResult result{};
    internal::applyPipeline(result, &pipeline);
    result.streamType = StreamType::Trades;
    result.inputPath = request.inputPath;
    const auto encodeTotalStartNs = timing::nowNs();
    const auto readStartNs = timing::nowNs();
    std::vector<std::uint8_t> input;
    if (!internal::readFileBytes(request.inputPath, input)) {
        auto result = internal::fail(Status::IoError, request, &pipeline, "failed to read input file");
        metrics::recordRun(result);
        return result;
    }
    result.readNs = timing::nowNs() - readStartNs;
    result.inputBytes = static_cast<std::uint64_t>(input.size());
    const auto parseStartNs = timing::nowNs();
    std::vector<Trade> trades;
    trades.reserve(static_cast<std::size_t>(std::count(input.begin(), input.end(), static_cast<std::uint8_t>('\n'))) + 1u);
    if (!parseTrades(input, trades)) {
        auto result = internal::fail(Status::CorruptData, request, &pipeline, "input is not clean canonical trades jsonl");
        metrics::recordRun(result);
        return result;
    }
    result.parseNs = timing::nowNs() - parseStartNs;

    const auto outputPath = internal::outputPathFor(request, pipeline, StreamType::Trades);
    std::error_code ec;
    std::filesystem::create_directories(outputPath.parent_path(), ec);
    if (ec) {
        auto result = internal::fail(Status::IoError, request, &pipeline, "failed to create output directory");
        metrics::recordRun(result);
        return result;
    }

    result.outputPath = outputPath;
    result.metricsPath = outputPath.parent_path() / (outputPath.stem().string() + ".metrics.json");

    std::ofstream out(outputPath, std::ios::binary | std::ios::trunc);
    if (!out) {
        auto failed = internal::fail(Status::IoError, request, &pipeline, "failed to open output file");
        metrics::recordRun(failed);
        return failed;
    }

    FileHeader fileHeader{};
    fileHeader.version = kVersionV3;
    fileHeader.stream = format::streamToWire(StreamType::Trades);
    fileHeader.lineEnding = detectLineEnding(input);
    fileHeader.chunkRecords = kDefaultChunkRecords;
    fileHeader.inputBytes = result.inputBytes;
    const auto placeholder = serializeFileHeader(fileHeader, true);
    out.write(reinterpret_cast<const char*>(placeholder.data()), static_cast<std::streamsize>(placeholder.size()));

    const auto encodeStartNs = timing::nowNs();
    const auto encodeStartCycles = timing::readCycles();
    for (std::size_t begin = 0; begin < trades.size(); begin += fileHeader.chunkRecords) {
        const auto end = std::min<std::size_t>(trades.size(), begin + fileHeader.chunkRecords);
        const auto chunk = encodeChunk(trades, begin, end);
        writeChunk(out, chunk);
        ++fileHeader.chunkCount;
        fileHeader.recordCount += chunk.recordCount;
        fileHeader.timeGroupCount += chunk.timeGroupCount;
        fileHeader.priceGroupCount += chunk.priceGroupCount;
        fileHeader.qtyEscapeCount += chunk.qtyEscapeCount;
        result.lineCount += chunk.recordCount;
        ++result.blockCount;
    }
    result.encodeCycles = timing::readCycles() - encodeStartCycles;
    result.encodeCoreNs = timing::nowNs() - encodeStartNs;
    const auto writeStartNs = timing::nowNs();
    out.flush();
    result.outputBytes = static_cast<std::uint64_t>(out.tellp());
    fileHeader.outputBytes = result.outputBytes;
    fileHeader.headerCrc32c = headerCrc32c(fileHeader);
    const auto finalHeader = serializeFileHeader(fileHeader, true);
    out.seekp(0, std::ios::beg);
    out.write(reinterpret_cast<const char*>(finalHeader.data()), static_cast<std::streamsize>(finalHeader.size()));
    out.close();
    result.writeNs = timing::nowNs() - writeStartNs;
    result.encodeNs = timing::nowNs() - encodeTotalStartNs;
    if (!out) {
        auto failed = internal::fail(Status::IoError, request, &pipeline, "failed to write trade grouped artifact");
        metrics::recordRun(failed);
        return failed;
    }

    std::size_t decodedOffset = 0;
    bool decodedMatchesInput = true;
    const auto decodeStartNs = timing::nowNs();
    const auto decodeStartCycles = timing::readCycles();
    const auto decodeStatus = decodeFile(outputPath, [&](std::span<const std::uint8_t> block) {
        if (decodedOffset + block.size() > input.size()) {
            decodedMatchesInput = false;
            return false;
        }
        decodedMatchesInput = std::equal(block.begin(), block.end(), input.begin() + static_cast<std::ptrdiff_t>(decodedOffset));
        decodedOffset += block.size();
        return decodedMatchesInput;
    });
    result.decodeCycles = timing::readCycles() - decodeStartCycles;
    result.decodeNs = timing::nowNs() - decodeStartNs;
    result.decodeCoreNs = result.decodeNs;
    result.roundtripOk = isOk(decodeStatus) && decodedMatchesInput && decodedOffset == input.size();
    result.status = result.roundtripOk ? Status::Ok : Status::DecodeError;
    if (!result.roundtripOk) result.error = "roundtrip check failed";

    (void)internal::writeTextFile(result.metricsPath, toMetricsJson(result));
    metrics::recordRun(result);
    return result;
}

ReplayArtifactInfo inspectArtifact(const std::filesystem::path& path, const PipelineDescriptor& pipeline) noexcept {
    std::vector<std::uint8_t> file;
    if (path.empty()) return failArtifact(path, Status::InvalidArgument, "artifact path is empty");
    if (!readFile(path, file)) return failArtifact(path, Status::IoError, "failed to read artifact");
    FileHeader header{};
    const auto status = walkFile(file, nullptr, nullptr, nullptr, &header);
    if (!isOk(status)) return failArtifact(path, status, "invalid trade grouped artifact");
    ReplayArtifactInfo info{};
    info.status = Status::Ok;
    info.found = true;
    info.path = path;
    info.formatId = pipeline.id == std::string_view{"hftmac.trades_grouped_delta_qtydict_v1"}
        ? "hftmac.trades_grouped_delta_qtydict.v1"
        : "hftmac.trades_grouped_delta_qtydict.math.v3";
    info.pipelineId = std::string{pipeline.id};
    info.transform = std::string{pipeline.transform};
    info.entropy = std::string{pipeline.entropy};
    info.streamType = StreamType::Trades;
    info.version = header.version;
    info.inputBytes = header.inputBytes;
    info.outputBytes = header.outputBytes;
    info.lineCount = header.recordCount;
    info.blockCount = header.chunkCount;
    return info;
}

Status decode(std::span<const std::uint8_t> file, const DecodedBlockCallback& onBlock) noexcept {
    if (!onBlock) return Status::InvalidArgument;
    return walkFile(file, &onBlock, nullptr, nullptr);
}

Status decodeFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept {
    if (path.empty() || !onBlock) return Status::InvalidArgument;
    std::vector<std::uint8_t> file;
    if (!readFile(path, file)) return Status::IoError;
    return decode(file, onBlock);
}

Status inspectEncodedJsonFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept {
    std::vector<std::uint8_t> file;
    if (!readFile(path, file)) return Status::IoError;
    std::ostringstream out;
    const auto status = walkFile(file, nullptr, &out, nullptr);
    if (!isOk(status)) return status;
    return writeStringBlock(out.str(), onBlock);
}

Status inspectEncodedBinaryFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept {
    std::vector<std::uint8_t> file;
    if (!readFile(path, file)) return Status::IoError;
    std::ostringstream out;
    const auto status = walkFile(file, nullptr, nullptr, &out);
    if (!isOk(status)) return status;
    return writeStringBlock(out.str(), onBlock);
}

Status inspectStatsJsonFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept {
    std::vector<std::uint8_t> file;
    if (!readFile(path, file)) return Status::IoError;
    FileHeader header{};
    const auto status = walkFile(file, nullptr, nullptr, nullptr, &header);
    if (!isOk(status)) return status;

    std::uint64_t hotQtyTableBytes = 0;
    std::uint64_t timeStreamBytes = 0;
    std::uint64_t priceStreamBytes = 0;
    std::uint64_t sideStreamBytes = 0;
    std::uint64_t dpZeroStreamBytes = 0;
    std::uint64_t countOneStreamBytes = 0;
    std::uint64_t priceGroupCountOneStreamBytes = 0;
    std::uint64_t qtyCodeStreamBytes = 0;
    std::uint64_t qtyEscapeStreamBytes = 0;
    std::uint64_t dpZeroCount = 0;
    std::uint64_t countOneCount = 0;
    std::uint64_t priceGroupCountOneCount = 0;
    std::int64_t firstPriceScale = 1;
    std::int64_t firstQtyScale = 1;
    std::int64_t firstTimeScale = 1;
    std::uint32_t firstHotQtyBits = 7;

    std::size_t offset = kFileHeaderBytes;
    for (std::uint64_t chunkIndex = 0; chunkIndex < header.chunkCount; ++chunkIndex) {
        if (file.size() - offset < kChunkHeaderBytes) return Status::CorruptData;
        ChunkHeader chunk{};
        if (!parseChunkHeader(file.data() + offset, file.size() - offset, chunk) || !validChunkHeader(chunk)) return Status::CorruptData;
        offset += kChunkHeaderBytes;
        if (chunkIndex == 0u) {
            firstPriceScale = chunk.priceScale;
            firstQtyScale = chunk.qtyScale;
            firstTimeScale = chunk.timeScale;
            firstHotQtyBits = chunk.hotQtyBits;
        }
        const auto tableBytes = static_cast<std::size_t>(chunk.hotQtyTableBytes);
        const auto streamsSize = static_cast<std::size_t>(chunk.timeStreamBytes)
            + chunk.priceStreamBytes + chunk.sideStreamBytes + chunk.dpZeroStreamBytes + chunk.countOneStreamBytes
            + chunk.priceGroupCountOneStreamBytes + chunk.qtyCodeStreamBytes + chunk.qtyEscapeStreamBytes;
        if (file.size() - offset < tableBytes + streamsSize) return Status::CorruptData;
        offset += tableBytes + streamsSize;
        hotQtyTableBytes += tableBytes;
        timeStreamBytes += chunk.timeStreamBytes;
        priceStreamBytes += chunk.priceStreamBytes;
        sideStreamBytes += chunk.sideStreamBytes;
        dpZeroStreamBytes += chunk.dpZeroStreamBytes;
        countOneStreamBytes += chunk.countOneStreamBytes;
        priceGroupCountOneStreamBytes += chunk.priceGroupCountOneStreamBytes;
        qtyCodeStreamBytes += chunk.qtyCodeStreamBytes;
        qtyEscapeStreamBytes += chunk.qtyEscapeStreamBytes;
        dpZeroCount += chunk.dpZeroCount;
        countOneCount += chunk.countOneCount;
        priceGroupCountOneCount += chunk.priceGroupCountOneCount;
    }
    if (offset != file.size()) return Status::CorruptData;

    std::ostringstream out;
    out << "{\n"
        << "  \"pipeline_id\": \"hftmac.trades_grouped_delta_qtydict_v1\",\n"
        << "  \"version\": " << header.version << ",\n"
        << "  \"record_count\": " << header.recordCount << ",\n"
        << "  \"chunk_count\": " << header.chunkCount << ",\n"
        << "  \"input_bytes\": " << header.inputBytes << ",\n"
        << "  \"encoded_bytes\": " << header.outputBytes << ",\n"
        << "  \"bytes_per_trade\": " << (header.recordCount == 0u ? 0.0 : static_cast<double>(header.outputBytes) / static_cast<double>(header.recordCount)) << ",\n"
        << "  \"timestamp_group_count\": " << header.timeGroupCount << ",\n"
        << "  \"price_group_count\": " << header.priceGroupCount << ",\n"
        << "  \"qty_escape_count\": " << header.qtyEscapeCount << ",\n"
        << "  \"dp_zero_count\": " << dpZeroCount << ",\n"
        << "  \"count_one_count\": " << countOneCount << ",\n"
        << "  \"price_group_count_one_count\": " << priceGroupCountOneCount << ",\n"
        << "  \"first_chunk_price_scale\": " << firstPriceScale << ",\n"
        << "  \"first_chunk_qty_scale\": " << firstQtyScale << ",\n"
        << "  \"first_chunk_time_scale\": " << firstTimeScale << ",\n"
        << "  \"first_chunk_hot_qty_bits\": " << firstHotQtyBits << ",\n"
        << "  \"hot_qty_table_bytes\": " << hotQtyTableBytes << ",\n"
        << "  \"time_stream_bytes\": " << timeStreamBytes << ",\n"
        << "  \"price_stream_bytes\": " << priceStreamBytes << ",\n"
        << "  \"side_stream_bytes\": " << sideStreamBytes << ",\n"
        << "  \"dp_zero_stream_bytes\": " << dpZeroStreamBytes << ",\n"
        << "  \"count_one_stream_bytes\": " << countOneStreamBytes << ",\n"
        << "  \"price_group_count_one_stream_bytes\": " << priceGroupCountOneStreamBytes << ",\n"
        << "  \"qty_code_stream_bytes\": " << qtyCodeStreamBytes << ",\n"
        << "  \"qty_escape_stream_bytes\": " << qtyEscapeStreamBytes << "\n"
        << "}\n";
    return writeStringBlock(out.str(), onBlock);
}

}  // namespace hft_compressor::codecs::trades_grouped_delta_qtydict

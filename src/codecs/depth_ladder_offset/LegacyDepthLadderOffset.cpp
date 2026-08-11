#include "codecs/depth_ladder_offset/LegacyDepthLadderOffset.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <sstream>
#include <string>
#include <type_traits>
#include <unordered_map>
#include <vector>

#include "common/CompressionInternals.hpp"

namespace hft_compressor::codecs::legacy_depth_ladder_offset {
namespace {

constexpr std::uint32_t kMagic = 0x50454443u;  // CDEP
constexpr std::uint16_t kLegacyArtifactVersion = 1u;
constexpr std::size_t kHeaderBytes = 160u;
constexpr std::uint32_t kHotQtyCount = 64u;

struct Level {
    std::int64_t price{0};
    std::int64_t qty{0};
    std::int64_t side{0};
};

struct Header {
    std::uint32_t magic{kMagic};
    std::uint16_t version{kLegacyArtifactVersion};
    std::uint16_t reserved{0};
    std::uint64_t inputBytes{0};
    std::uint64_t outputBytes{0};
    std::uint64_t batchCount{0};
    std::uint64_t levelCount{0};
    std::int64_t timeScale{1};
    std::int64_t priceScale{1};
    std::int64_t qtyScale{1};
    std::int64_t baseTsUnit{0};
    std::uint32_t hotQtyCount{0};
    std::uint32_t batchBytes{0};
    std::uint32_t sideBytes{0};
    std::uint32_t priceModeBytes{0};
    std::uint32_t deleteBytes{0};
    std::uint32_t priceBytes{0};
    std::uint32_t qtyCodeBytes{0};
    std::uint32_t qtyEscapeBytes{0};
    std::uint32_t hotQtyBytes{0};
    std::uint32_t deleteCount{0};
    std::uint32_t qtyEscapeCount{0};
    std::uint32_t offsetPriceCount{0};
    std::uint32_t absolutePriceCount{0};
    std::uint32_t explicitSideCount{0};
};

template <class T>
bool readLittleEndian(const std::uint8_t*& cursor, const std::uint8_t* end, T& out) noexcept {
    if (static_cast<std::size_t>(end - cursor) < sizeof(T)) return false;
    using Unsigned = std::make_unsigned_t<T>;
    Unsigned value = 0;
    for (std::size_t i = 0; i < sizeof(T); ++i) {
        value |= static_cast<Unsigned>(*cursor++) << (i * 8u);
    }
    out = static_cast<T>(value);
    return true;
}

bool readVarint(const std::uint8_t*& cursor, const std::uint8_t* end, std::uint64_t& out) noexcept {
    out = 0;
    unsigned shift = 0;
    while (cursor < end && shift <= 63u) {
        const auto byte = *cursor++;
        out |= static_cast<std::uint64_t>(byte & 0x7fu) << shift;
        if ((byte & 0x80u) == 0u) return true;
        shift += 7u;
    }
    return false;
}

struct BitReader {
    const std::uint8_t* cursor{};
    const std::uint8_t* end{};
    std::uint8_t current{0};
    unsigned used{8};

    bool bit(bool& value) noexcept {
        if (used == 8u) {
            if (cursor >= end) return false;
            current = *cursor++;
            used = 0;
        }
        value = ((current >> used) & 1u) != 0u;
        ++used;
        return true;
    }
};

struct BookState {
    std::unordered_map<std::int64_t, std::int64_t> bids;
    std::unordered_map<std::int64_t, std::int64_t> asks;
    bool haveBid{false};
    bool haveAsk{false};
    std::int64_t bestBid{0};
    std::int64_t bestAsk{0};

    void recompute() noexcept {
        haveBid = !bids.empty();
        haveAsk = !asks.empty();
        if (haveBid) {
            bestBid = bids.begin()->first;
            for (const auto& [price, qty] : bids) {
                (void)qty;
                bestBid = std::max(bestBid, price);
            }
        }
        if (haveAsk) {
            bestAsk = asks.begin()->first;
            for (const auto& [price, qty] : asks) {
                (void)qty;
                bestAsk = std::min(bestAsk, price);
            }
        }
    }

    void apply(std::int64_t side, std::int64_t price, std::int64_t qty) {
        auto& levels = side == 0 ? bids : asks;
        if (qty == 0) levels.erase(price);
        else levels[price] = qty;
    }
};

bool readHeader(std::span<const std::uint8_t> data, Header& header) noexcept {
    if (data.size() < kHeaderBytes) return false;
    const auto* cursor = data.data();
    const auto* end = data.data() + kHeaderBytes;
    return readLittleEndian(cursor, end, header.magic)
        && readLittleEndian(cursor, end, header.version)
        && readLittleEndian(cursor, end, header.reserved)
        && readLittleEndian(cursor, end, header.inputBytes)
        && readLittleEndian(cursor, end, header.outputBytes)
        && readLittleEndian(cursor, end, header.batchCount)
        && readLittleEndian(cursor, end, header.levelCount)
        && readLittleEndian(cursor, end, header.timeScale)
        && readLittleEndian(cursor, end, header.priceScale)
        && readLittleEndian(cursor, end, header.qtyScale)
        && readLittleEndian(cursor, end, header.baseTsUnit)
        && readLittleEndian(cursor, end, header.hotQtyCount)
        && readLittleEndian(cursor, end, header.batchBytes)
        && readLittleEndian(cursor, end, header.sideBytes)
        && readLittleEndian(cursor, end, header.priceModeBytes)
        && readLittleEndian(cursor, end, header.deleteBytes)
        && readLittleEndian(cursor, end, header.priceBytes)
        && readLittleEndian(cursor, end, header.qtyCodeBytes)
        && readLittleEndian(cursor, end, header.qtyEscapeBytes)
        && readLittleEndian(cursor, end, header.hotQtyBytes)
        && readLittleEndian(cursor, end, header.deleteCount)
        && readLittleEndian(cursor, end, header.qtyEscapeCount)
        && readLittleEndian(cursor, end, header.offsetPriceCount)
        && readLittleEndian(cursor, end, header.absolutePriceCount)
        && readLittleEndian(cursor, end, header.explicitSideCount)
        && header.magic == kMagic
        && header.version == kLegacyArtifactVersion
        && header.hotQtyCount <= kHotQtyCount
        && header.hotQtyBytes == header.hotQtyCount * sizeof(std::int64_t);
}

bool take(std::span<const std::uint8_t> data,
          std::size_t& offset,
          std::uint32_t size,
          std::span<const std::uint8_t>& out) noexcept {
    if (size > data.size() - offset) return false;
    out = data.subspan(offset, size);
    offset += size;
    return true;
}

std::int64_t unzigzag(std::uint64_t value) noexcept {
    return (value & 1u) != 0u
        ? -static_cast<std::int64_t>((value + 1u) / 2u)
        : static_cast<std::int64_t>(value / 2u);
}

Status decodeBytes(std::span<const std::uint8_t> data, std::string* jsonl, std::ostream* encoded) noexcept {
    Header header{};
    if (!readHeader(data, header)) return Status::CorruptData;

    std::size_t offset = kHeaderBytes;
    if (header.hotQtyBytes > data.size() - offset) return Status::CorruptData;
    std::vector<std::int64_t> hotQty(header.hotQtyCount);
    const auto* hotCursor = data.data() + offset;
    const auto* hotEnd = hotCursor + header.hotQtyBytes;
    for (auto& qty : hotQty) {
        if (!readLittleEndian(hotCursor, hotEnd, qty)) return Status::CorruptData;
    }
    offset += header.hotQtyBytes;

    std::span<const std::uint8_t> batchStream;
    std::span<const std::uint8_t> sideStream;
    std::span<const std::uint8_t> priceModeStream;
    std::span<const std::uint8_t> deleteStream;
    std::span<const std::uint8_t> priceStream;
    std::span<const std::uint8_t> qtyCodeStream;
    std::span<const std::uint8_t> qtyEscapeStream;
    if (!take(data, offset, header.batchBytes, batchStream)
        || !take(data, offset, header.sideBytes, sideStream)
        || !take(data, offset, header.priceModeBytes, priceModeStream)
        || !take(data, offset, header.deleteBytes, deleteStream)
        || !take(data, offset, header.priceBytes, priceStream)
        || !take(data, offset, header.qtyCodeBytes, qtyCodeStream)
        || !take(data, offset, header.qtyEscapeBytes, qtyEscapeStream)
        || offset != data.size()) {
        return Status::CorruptData;
    }

    const auto* batchCursor = batchStream.data();
    const auto* batchEnd = batchStream.data() + batchStream.size();
    const auto* priceCursor = priceStream.data();
    const auto* priceEnd = priceStream.data() + priceStream.size();
    const auto* qtyCodeCursor = qtyCodeStream.data();
    const auto* qtyCodeEnd = qtyCodeStream.data() + qtyCodeStream.size();
    const auto* qtyEscapeCursor = qtyEscapeStream.data();
    const auto* qtyEscapeEnd = qtyEscapeStream.data() + qtyEscapeStream.size();
    BitReader sideBits{sideStream.data(), sideStream.data() + sideStream.size()};
    BitReader priceModeBits{priceModeStream.data(), priceModeStream.data() + priceModeStream.size()};
    BitReader deleteBits{deleteStream.data(), deleteStream.data() + deleteStream.size()};

    BookState state;
    std::int64_t timestamp = header.baseTsUnit;
    if (encoded != nullptr) {
        *encoded << "{\n  \"pipeline_id\": \"hftmac.depth_ladder_offset_v1\",\n  \"batch_count\": "
                 << header.batchCount << ",\n  \"batches\": [\n";
    }
    for (std::uint64_t batchIndex = 0; batchIndex < header.batchCount; ++batchIndex) {
        std::uint64_t timestampDelta = 0;
        std::uint64_t levelCount = 0;
        if (!readVarint(batchCursor, batchEnd, timestampDelta)
            || !readVarint(batchCursor, batchEnd, levelCount)) {
            return Status::CorruptData;
        }
        timestamp += static_cast<std::int64_t>(timestampDelta);
        std::vector<Level> levels;
        levels.reserve(static_cast<std::size_t>(levelCount));
        if (encoded != nullptr) {
            *encoded << (batchIndex != 0u ? ",\n" : "")
                     << "    {\"dt\":" << timestampDelta << ",\"level_count\":" << levelCount << "}";
        }
        for (std::uint64_t levelIndex = 0; levelIndex < levelCount; ++levelIndex) {
            bool sideBit = false;
            bool offsetMode = false;
            bool deleted = false;
            if (!sideBits.bit(sideBit) || !priceModeBits.bit(offsetMode) || !deleteBits.bit(deleted)) {
                return Status::CorruptData;
            }
            const std::int64_t side = sideBit ? 1 : 0;
            std::uint64_t priceValue = 0;
            if (!readVarint(priceCursor, priceEnd, priceValue)) return Status::CorruptData;
            std::int64_t price = 0;
            if (offsetMode) {
                const auto priceOffset = static_cast<std::int64_t>(priceValue);
                price = side == 0 ? state.bestBid - priceOffset : state.bestAsk + priceOffset;
            } else {
                price = unzigzag(priceValue);
            }
            std::int64_t qty = 0;
            if (!deleted) {
                std::uint64_t qtyCode = 0;
                if (!readVarint(qtyCodeCursor, qtyCodeEnd, qtyCode)) return Status::CorruptData;
                if (qtyCode < hotQty.size()) {
                    qty = hotQty[static_cast<std::size_t>(qtyCode)];
                } else {
                    std::uint64_t rawQty = 0;
                    if (!readVarint(qtyEscapeCursor, qtyEscapeEnd, rawQty)) return Status::CorruptData;
                    qty = static_cast<std::int64_t>(rawQty);
                }
            }
            levels.push_back(Level{price, qty, side});
        }
        if (jsonl != nullptr) {
            *jsonl += "[";
            for (std::size_t i = 0; i < levels.size(); ++i) {
                if (i != 0u) *jsonl += ",";
                const auto& level = levels[i];
                *jsonl += "[" + std::to_string(level.price * header.priceScale)
                    + "," + std::to_string(level.qty * header.qtyScale)
                    + "," + std::to_string(level.side) + "]";
            }
            *jsonl += "," + std::to_string(timestamp * header.timeScale) + "]\n";
        }
        for (const auto& level : levels) state.apply(level.side, level.price, level.qty);
        state.recompute();
    }
    if (batchCursor != batchEnd
        || priceCursor != priceEnd
        || qtyCodeCursor != qtyCodeEnd
        || qtyEscapeCursor != qtyEscapeEnd) {
        return Status::CorruptData;
    }
    if (encoded != nullptr) *encoded << "\n  ]\n}\n";
    return Status::Ok;
}

bool readFile(const std::filesystem::path& path, std::vector<std::uint8_t>& out) noexcept {
    return internal::readFileBytes(path, out);
}

Status emitText(const std::string& text, const DecodedBlockCallback& onBlock) noexcept {
    return onBlock(std::span<const std::uint8_t>{reinterpret_cast<const std::uint8_t*>(text.data()), text.size()})
        ? Status::Ok
        : Status::CallbackStopped;
}

std::string statsJson(const Header& header) {
    std::ostringstream out;
    out << "{\n  \"pipeline_id\": \"hftmac.depth_ladder_offset_v1\",\n"
        << "  \"version\": " << header.version << ",\n"
        << "  \"batch_count\": " << header.batchCount << ",\n"
        << "  \"total_level_count\": " << header.levelCount << ",\n"
        << "  \"raw_runtime_bytes\": " << (header.batchCount * 8u + header.levelCount * 32u) << ",\n"
        << "  \"encoded_bytes\": " << header.outputBytes << ",\n"
        << "  \"bytes_per_level\": "
        << (header.levelCount != 0u ? static_cast<double>(header.outputBytes) / static_cast<double>(header.levelCount) : 0.0) << ",\n"
        << "  \"delete_count\": " << header.deleteCount << ",\n"
        << "  \"offset_price_count\": " << header.offsetPriceCount << ",\n"
        << "  \"absolute_price_count\": " << header.absolutePriceCount << ",\n"
        << "  \"qty_escape_count\": " << header.qtyEscapeCount << ",\n"
        << "  \"batch_stream_bytes\": " << header.batchBytes << ",\n"
        << "  \"side_stream_bytes\": " << header.sideBytes << ",\n"
        << "  \"price_mode_stream_bytes\": " << header.priceModeBytes << ",\n"
        << "  \"delete_bit_stream_bytes\": " << header.deleteBytes << ",\n"
        << "  \"price_stream_bytes\": " << header.priceBytes << ",\n"
        << "  \"qty_code_stream_bytes\": " << header.qtyCodeBytes << ",\n"
        << "  \"qty_escape_stream_bytes\": " << header.qtyEscapeBytes << "\n}\n";
    return out.str();
}

}  // namespace

ReplayArtifactInfo inspectArtifact(const std::filesystem::path& path,
                                   const PipelineDescriptor& pipeline) noexcept {
    std::vector<std::uint8_t> data;
    ReplayArtifactInfo info{};
    info.path = path;
    if (!readFile(path, data)) {
        info.status = Status::IoError;
        info.error = "failed to read artifact";
        return info;
    }
    Header header{};
    if (!readHeader(data, header)) {
        info.status = Status::CorruptData;
        info.error = "invalid legacy depth artifact";
        return info;
    }
    info.status = Status::Ok;
    info.found = true;
    info.formatId = "hftmac.depth_ladder_offset.v1";
    info.pipelineId = std::string{pipeline.id};
    info.transform = std::string{pipeline.transform};
    info.entropy = std::string{pipeline.entropy};
    info.streamType = StreamType::Depth;
    info.version = header.version;
    info.inputBytes = header.inputBytes;
    info.outputBytes = header.outputBytes;
    info.lineCount = header.batchCount;
    info.blockCount = 1;
    return info;
}

Status decode(std::span<const std::uint8_t> bytes, const DecodedBlockCallback& onBlock) noexcept {
    std::string out;
    const auto status = decodeBytes(bytes, &out, nullptr);
    if (!isOk(status)) return status;
    return emitText(out, onBlock);
}

Status decodeFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept {
    std::vector<std::uint8_t> data;
    if (!readFile(path, data)) return Status::IoError;
    return decode(data, onBlock);
}

Status inspectEncodedJsonFile(const std::filesystem::path& path,
                              const DecodedBlockCallback& onBlock) noexcept {
    std::vector<std::uint8_t> data;
    if (!readFile(path, data)) return Status::IoError;
    std::ostringstream out;
    const auto status = decodeBytes(data, nullptr, &out);
    if (!isOk(status)) return status;
    return emitText(out.str(), onBlock);
}

Status inspectEncodedBinaryFile(const std::filesystem::path& path,
                                const DecodedBlockCallback& onBlock) noexcept {
    std::vector<std::uint8_t> data;
    if (!readFile(path, data)) return Status::IoError;
    Header header{};
    if (!readHeader(data, header)) return Status::CorruptData;
    std::ostringstream out;
    out << "depth_ladder_offset_v1 bytes=" << data.size()
        << " header=" << kHeaderBytes
        << " hot_qty=" << header.hotQtyBytes
        << " batch=" << header.batchBytes
        << " side=" << header.sideBytes
        << " mode=" << header.priceModeBytes
        << " delete=" << header.deleteBytes
        << " price=" << header.priceBytes
        << " qty_code=" << header.qtyCodeBytes
        << " qty_escape=" << header.qtyEscapeBytes << "\n";
    return emitText(out.str(), onBlock);
}

Status inspectStatsJsonFile(const std::filesystem::path& path,
                            const DecodedBlockCallback& onBlock) noexcept {
    std::vector<std::uint8_t> data;
    if (!readFile(path, data)) return Status::IoError;
    Header header{};
    if (!readHeader(data, header)) return Status::CorruptData;
    return emitText(statsJson(header), onBlock);
}

}  // namespace hft_compressor::codecs::legacy_depth_ladder_offset

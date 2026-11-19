#include "TradesGroupedDeltaQtyDictInternal.hpp"

namespace hft_compressor::codecs::trades_grouped_delta_qtydict::codec_detail {

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

std::uint64_t zigzag(std::int64_t value) noexcept {
    return (static_cast<std::uint64_t>(value) << 1u) ^ static_cast<std::uint64_t>(value >> 63u);
}

std::int64_t unzigzag(std::uint64_t value) noexcept {
    return static_cast<std::int64_t>((value >> 1u) ^ (~(value & 1u) + 1u));
}

std::int64_t gcdPositive(std::int64_t a, std::int64_t b) noexcept {
    return math::binaryGcdAbs(a, b);
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
    return math::safePositiveScale(value);
}

std::vector<std::uint8_t> serializeFileHeader(FileHeader header, bool includeCrc) {
    if (!includeCrc) header.headerCrc32c = 0u;
    std::vector<std::uint8_t> out;
    out.reserve(kFileHeaderBytes);
    writeLe(out, header.magic);
    writeLe(out, header.stream);
    writeLe(out, header.reserved);
    writeLe(out, static_cast<std::uint32_t>(header.layout.streamType));
    writeLe(out, header.layout.recordBytes);
    writeLe(out, header.layout.payloadOffset);
    writeLe(out, header.layout.timestampOffset);
    writeLe(out, header.layout.depthLevelsOffset);
    writeLe(out, header.layout.depthLevelStride);
    writeLe(out, header.layout.depthLevelCountOffset);
    writeLe(out, header.layout.depthLevelCapacity);
    writeLe(out, header.chunkRecords);
    writeLe(out, header.inputBytes);
    writeLe(out, header.outputBytes);
    writeLe(out, header.recordCount);
    writeLe(out, header.chunkCount);
    writeLe(out, header.timeGroupCount);
    writeLe(out, header.priceGroupCount);
    writeLe(out, header.qtyEscapeCount);
    writeLe(out, header.recordsCrc32c);
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
    writeLe(out, header.sideMode);
    writeLe(out, header.timeStreamBytes);
    writeLe(out, header.priceStreamBytes);
    writeLe(out, header.sideStreamBytes);
    writeLe(out, header.dpZeroStreamBytes);
    writeLe(out, header.countOneStreamBytes);
    writeLe(out, header.priceGroupCountOneStreamBytes);
    writeLe(out, header.qtyCodeStreamBytes);
    writeLe(out, header.qtyEscapeStreamBytes);
    writeLe(out, header.hotQtyTableBytes);
    writeLe(out, header.residualStreamBytes);
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
    std::uint32_t streamRaw = 0u;
    const bool ok = readLe(p, end, out.magic)
        && readLe(p, end, out.stream)
        && readLe(p, end, out.reserved)
        && readLe(p, end, streamRaw)
        && readLe(p, end, out.layout.recordBytes)
        && readLe(p, end, out.layout.payloadOffset)
        && readLe(p, end, out.layout.timestampOffset)
        && readLe(p, end, out.layout.depthLevelsOffset)
        && readLe(p, end, out.layout.depthLevelStride)
        && readLe(p, end, out.layout.depthLevelCountOffset)
        && readLe(p, end, out.layout.depthLevelCapacity)
        && readLe(p, end, out.chunkRecords)
        && readLe(p, end, out.inputBytes)
        && readLe(p, end, out.outputBytes)
        && readLe(p, end, out.recordCount)
        && readLe(p, end, out.chunkCount)
        && readLe(p, end, out.timeGroupCount)
        && readLe(p, end, out.priceGroupCount)
        && readLe(p, end, out.qtyEscapeCount)
        && readLe(p, end, out.recordsCrc32c)
        && readLe(p, end, out.headerCrc32c);
    if (!ok || !std::all_of(p, end, [](auto b) { return b == 0u; })) return false;
    out.layout.streamType = static_cast<StreamType>(streamRaw);
    return true;
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
        && readLe(p, end, out.sideMode)
        && readLe(p, end, out.timeStreamBytes)
        && readLe(p, end, out.priceStreamBytes)
        && readLe(p, end, out.sideStreamBytes)
        && readLe(p, end, out.dpZeroStreamBytes)
        && readLe(p, end, out.countOneStreamBytes)
        && readLe(p, end, out.priceGroupCountOneStreamBytes)
        && readLe(p, end, out.qtyCodeStreamBytes)
        && readLe(p, end, out.qtyEscapeStreamBytes)
        && readLe(p, end, out.hotQtyTableBytes)
        && readLe(p, end, out.residualStreamBytes)
        && readLe(p, end, out.dpZeroCount)
        && readLe(p, end, out.countOneCount)
        && readLe(p, end, out.priceGroupCountOneCount)
        && readLe(p, end, out.payloadCrc32c)
        && std::all_of(p, end, [](auto b) { return b == 0u; });
}

std::uint32_t headerCrc32c(const FileHeader& header) {
    return format::crc32c(serializeFileHeader(header, false));
}

bool validHeader(const FileHeader& header) noexcept {
    return header.magic == kFileMagic
        && format::streamFromWire(header.stream) == StreamType::Trades
        && header.layout.streamType == StreamType::Trades
        && validByteBlockLayout(header.layout)
        && header.reserved == 0u
        && header.chunkRecords != 0u;
}

bool validChunkHeader(const ChunkHeader& header) noexcept {
    return header.magic == kChunkMagic
        && header.recordCount != 0u
        && header.timeGroupCount != 0u
        && header.priceGroupCount != 0u
        && header.hotQtyCount <= kMaxHotQtyCount
        && header.hotQtyBits <= 8u
        && header.sideMode <= 1u;
}

}  // namespace hft_compressor::codecs::trades_grouped_delta_qtydict::codec_detail

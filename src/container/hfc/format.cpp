#include "format.hpp"

#include <algorithm>
#include <iterator>

namespace hft_compressor::format {
namespace {

constexpr std::uint32_t kCrc32cPolynomial = 0x82f63b78u;

template <typename T>
void writeLe(std::vector<std::uint8_t>& out, T value) {
    for (std::size_t i = 0; i < sizeof(T); ++i) {
        out.push_back(static_cast<std::uint8_t>((static_cast<std::uint64_t>(value) >> (i * 8u)) & 0xffu));
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

void writeLeAt(std::uint8_t* out, std::uint32_t value) noexcept {
    for (std::size_t i = 0; i < sizeof(value); ++i) {
        out[i] = static_cast<std::uint8_t>((value >> (i * 8u)) & 0xffu);
    }
}

std::uint32_t readLe32(const std::uint8_t* data) noexcept {
    return static_cast<std::uint32_t>(data[0])
        | (static_cast<std::uint32_t>(data[1]) << 8u)
        | (static_cast<std::uint32_t>(data[2]) << 16u)
        | (static_cast<std::uint32_t>(data[3]) << 24u);
}

}  // namespace

std::vector<std::uint8_t> serializeFileHeader(const FileHeader& header) {
    std::vector<std::uint8_t> out;
    out.reserve(kFileHeaderBytes);
    writeLe(out, header.magic);
    writeLe(out, header.version);
    writeLe(out, header.codec);
    writeLe(out, header.stream);
    writeLe(out, header.reserved0);
    writeLe(out, header.blockBytes);
    writeLe(out, header.inputBytes);
    writeLe(out, header.outputBytes);
    writeLe(out, header.lineCount);
    writeLe(out, header.blockCount);
    out.insert(out.end(), std::begin(header.reserved1), std::end(header.reserved1));
    out.resize(kFileHeaderBytes, 0);
    return out;
}

std::vector<std::uint8_t> serializeBlockHeader(const BlockHeader& header) {
    std::vector<std::uint8_t> out;
    out.reserve(kBlockHeaderBytes);
    writeLe(out, header.magic);
    writeLe(out, header.uncompressedBytes);
    writeLe(out, header.compressedBytes);
    writeLe(out, header.lineCount);
    writeLe(out, header.firstByteOffset);
    writeLe(out, header.reserved);
    out.resize(kBlockHeaderBytes, 0);
    return out;
}

bool parseFileHeader(const std::uint8_t* data, std::size_t len, FileHeader& out) noexcept {
    if (data == nullptr || len < kFileHeaderBytes) return false;
    const auto* p = data;
    const auto* end = data + kFileHeaderBytes;
    const bool ok = readLe(p, end, out.magic) && readLe(p, end, out.version) && readLe(p, end, out.codec)
        && readLe(p, end, out.stream) && readLe(p, end, out.reserved0) && readLe(p, end, out.blockBytes)
        && readLe(p, end, out.inputBytes) && readLe(p, end, out.outputBytes)
        && readLe(p, end, out.lineCount) && readLe(p, end, out.blockCount);
    if (!ok || static_cast<std::size_t>(end - p) < sizeof(out.reserved1)) return false;
    std::copy(p, p + sizeof(out.reserved1), std::begin(out.reserved1));
    return true;
}

bool parseBlockHeader(const std::uint8_t* data, std::size_t len, BlockHeader& out) noexcept {
    if (data == nullptr || len < kBlockHeaderBytes) return false;
    const auto* p = data;
    const auto* end = data + kBlockHeaderBytes;
    return readLe(p, end, out.magic) && readLe(p, end, out.uncompressedBytes)
        && readLe(p, end, out.compressedBytes) && readLe(p, end, out.lineCount)
        && readLe(p, end, out.firstByteOffset) && readLe(p, end, out.reserved);
}

bool isSupportedVersion(std::uint16_t version) noexcept {
    return version == kVersion1 || version == kVersion2;
}

std::uint32_t crc32c(std::span<const std::uint8_t> data) noexcept {
    std::uint32_t crc = 0xffffffffu;
    for (const std::uint8_t byte : data) {
        crc ^= byte;
        for (std::uint32_t bit = 0; bit < 8u; ++bit) {
            const std::uint32_t mask = 0u - (crc & 1u);
            crc = (crc >> 1u) ^ (kCrc32cPolynomial & mask);
        }
    }
    return ~crc;
}

std::uint32_t storedHeaderCrc32c(const FileHeader& header) noexcept {
    return readLe32(header.reserved1);
}

std::uint32_t headerCrc32c(const FileHeader& header) noexcept {
    FileHeader copy = header;
    setHeaderCrc32c(copy, 0u);
    const auto bytes = serializeFileHeader(copy);
    return crc32c(bytes);
}

void setHeaderCrc32c(FileHeader& header, std::uint32_t crc) noexcept {
    writeLeAt(header.reserved1, crc);
}

std::uint32_t compressedCrc32c(const BlockHeader& header) noexcept {
    return static_cast<std::uint32_t>(header.reserved & 0xffffffffu);
}

std::uint32_t uncompressedCrc32c(const BlockHeader& header) noexcept {
    return static_cast<std::uint32_t>((header.reserved >> 32u) & 0xffffffffu);
}

void setBlockChecksums(BlockHeader& header, std::uint32_t compressedCrc, std::uint32_t uncompressedCrc) noexcept {
    header.reserved = static_cast<std::uint64_t>(compressedCrc)
        | (static_cast<std::uint64_t>(uncompressedCrc) << 32u);
}

std::uint16_t streamToWire(StreamType type) noexcept {
    switch (type) {
        case StreamType::Trades: return 1;
        case StreamType::BookTicker: return 2;
        case StreamType::Depth: return 3;
        case StreamType::Unknown: return 0;
    }
    return 0;
}

StreamType streamFromWire(std::uint16_t value) noexcept {
    switch (value) {
        case 1: return StreamType::Trades;
        case 2: return StreamType::BookTicker;
        case 3: return StreamType::Depth;
        default: return StreamType::Unknown;
    }
}

}  // namespace hft_compressor::format

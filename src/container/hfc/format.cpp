#include "format.hpp"

#include <iterator>

namespace hft_compressor::format {
namespace {

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
    return readLe(p, end, out.magic) && readLe(p, end, out.version) && readLe(p, end, out.codec)
        && readLe(p, end, out.stream) && readLe(p, end, out.reserved0) && readLe(p, end, out.blockBytes)
        && readLe(p, end, out.inputBytes) && readLe(p, end, out.outputBytes)
        && readLe(p, end, out.lineCount) && readLe(p, end, out.blockCount);
}

bool parseBlockHeader(const std::uint8_t* data, std::size_t len, BlockHeader& out) noexcept {
    if (data == nullptr || len < kBlockHeaderBytes) return false;
    const auto* p = data;
    const auto* end = data + kBlockHeaderBytes;
    return readLe(p, end, out.magic) && readLe(p, end, out.uncompressedBytes)
        && readLe(p, end, out.compressedBytes) && readLe(p, end, out.lineCount)
        && readLe(p, end, out.firstByteOffset) && readLe(p, end, out.reserved);
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

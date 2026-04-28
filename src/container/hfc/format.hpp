#pragma once

#include <cstddef>
#include <cstdint>
#include <span>
#include <vector>

#include "hft_compressor/stream_type.hpp"

namespace hft_compressor::format {

inline constexpr std::uint32_t kFileMagic = 0x31434648u;
inline constexpr std::uint32_t kBlockMagic = 0x30424648u;
inline constexpr std::uint16_t kVersion1 = 1;
inline constexpr std::uint16_t kVersion2 = 2;
inline constexpr std::uint16_t kVersion = kVersion2;
inline constexpr std::uint16_t kCodecZstdJsonlBlocksV1 = 1;
inline constexpr std::size_t kFileHeaderBytes = 64;
inline constexpr std::size_t kBlockHeaderBytes = 32;

struct FileHeader {
    std::uint32_t magic{kFileMagic};
    std::uint16_t version{kVersion};
    std::uint16_t codec{kCodecZstdJsonlBlocksV1};
    std::uint16_t stream{0};
    std::uint16_t reserved0{0};
    std::uint32_t blockBytes{0};
    std::uint64_t inputBytes{0};
    std::uint64_t outputBytes{0};
    std::uint64_t lineCount{0};
    std::uint64_t blockCount{0};
    std::uint8_t reserved1[16]{};
};

struct BlockHeader {
    std::uint32_t magic{kBlockMagic};
    std::uint32_t uncompressedBytes{0};
    std::uint32_t compressedBytes{0};
    std::uint32_t lineCount{0};
    std::uint64_t firstByteOffset{0};
    std::uint64_t reserved{0};
};

std::vector<std::uint8_t> serializeFileHeader(const FileHeader& header);
std::vector<std::uint8_t> serializeBlockHeader(const BlockHeader& header);
bool parseFileHeader(const std::uint8_t* data, std::size_t len, FileHeader& out) noexcept;
bool parseBlockHeader(const std::uint8_t* data, std::size_t len, BlockHeader& out) noexcept;
bool isSupportedVersion(std::uint16_t version) noexcept;
std::uint32_t crc32c(std::span<const std::uint8_t> data) noexcept;
std::uint32_t storedHeaderCrc32c(const FileHeader& header) noexcept;
std::uint32_t headerCrc32c(const FileHeader& header) noexcept;
void setHeaderCrc32c(FileHeader& header, std::uint32_t crc) noexcept;
std::uint32_t compressedCrc32c(const BlockHeader& header) noexcept;
std::uint32_t uncompressedCrc32c(const BlockHeader& header) noexcept;
void setBlockChecksums(BlockHeader& header, std::uint32_t compressedCrc, std::uint32_t uncompressedCrc) noexcept;
std::uint16_t streamToWire(StreamType type) noexcept;
StreamType streamFromWire(std::uint16_t value) noexcept;

}  // namespace hft_compressor::format

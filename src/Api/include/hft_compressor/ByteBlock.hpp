#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

#include "hft_compressor/Api.hpp"
#include "hft_compressor/Status.hpp"
#include "hft_compressor/StreamType.hpp"

namespace hft_compressor {

inline constexpr std::size_t kByteBlockMaxRecords = 256;
inline constexpr std::size_t kByteBlockMaxRecordBytes = 1024;
inline constexpr std::size_t kByteBlockMaxDepthLevels = 32;
inline constexpr std::size_t kByteBlockHeaderBytes = 64;
inline constexpr std::uint16_t kByteBlockCodec = 1;
inline constexpr std::uint16_t kByteBlockSchemaVersion = 1;

// Cold, independently decodable blocks. Offsets are record-relative; the
// caller owns the record schema. Unknown selects opaque byte compression.
// Numeric payload fields are little-endian eight-byte integer bit patterns.
// Trades use price/quantity/side at payload+0/+8/+16; BBO uses bid,
// bid quantity, ask, ask quantity at payload+0/+8/+16/+24. Depth levels
// use price/quantity/side at +0/+8/+16, with a two-byte level count.
struct ByteBlockLayout {
    StreamType streamType{StreamType::Unknown};
    std::uint32_t recordBytes{};
    std::uint32_t payloadOffset{};
    std::uint32_t timestampOffset{};
    std::uint32_t depthLevelsOffset{};
    std::uint32_t depthLevelStride{};
    std::uint32_t depthLevelCountOffset{};
    std::uint32_t depthLevelCapacity{};
};

// Zero means invalid layout/count. Includes the complete encoded block.
HFT_COMPRESSOR_API std::size_t byteBlockEncodeBound(
    const ByteBlockLayout& layout, std::size_t recordCount) noexcept;

// No output is published on failure (including overlapping input/output).
// All bytes, padding, clocks and input order round-trip exactly. State resets
// at each call. Cold scratch allocations have a fixed block-size ceiling.
HFT_COMPRESSOR_API Status encodeByteBlock(const ByteBlockLayout& layout,
    std::span<const std::uint8_t> records, std::span<std::uint8_t> encoded,
    std::size_t& encodedBytes) noexcept;
HFT_COMPRESSOR_API Status decodeByteBlock(const ByteBlockLayout& layout,
    std::span<const std::uint8_t> encoded, std::span<std::uint8_t> records,
    std::size_t& recordBytes) noexcept;

} // namespace hft_compressor

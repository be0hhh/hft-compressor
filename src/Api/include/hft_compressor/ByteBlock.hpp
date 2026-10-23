#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
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

// Caller-owned cold scratch, reused across any valid layout and block count.
// prepare() allocates once at the fixed maximum block ceiling; repeated calls
// retain that storage. reset() discards transient block state, not capacity.
// Unprepared workspaces refuse encode/decode. One owner, no concurrent calls.
class HFT_COMPRESSOR_API ByteBlockWorkspace {
public:
    ByteBlockWorkspace() noexcept;
    ~ByteBlockWorkspace();
    ByteBlockWorkspace(const ByteBlockWorkspace&) = delete;
    ByteBlockWorkspace& operator=(const ByteBlockWorkspace&) = delete;
    Status prepare() noexcept;
    void reset() noexcept;

private:
    struct Scratch;
    std::unique_ptr<Scratch> scratch_;
    friend Status encodeByteBlock(ByteBlockWorkspace&, const ByteBlockLayout&,
        std::span<const std::uint8_t>, std::span<std::uint8_t>, std::size_t&) noexcept;
    friend Status decodeByteBlock(ByteBlockWorkspace&, const ByteBlockLayout&,
        std::span<const std::uint8_t>, std::span<std::uint8_t>, std::size_t&) noexcept;
};

// No output is published on failure (including overlapping input/output).
// All bytes, padding, clocks and input order round-trip exactly. State resets
// at each call. Prepared workspace overloads perform no scratch allocation;
// convenience overloads prepare private scratch with the same fixed ceiling.
HFT_COMPRESSOR_API Status encodeByteBlock(ByteBlockWorkspace& workspace,
    const ByteBlockLayout& layout, std::span<const std::uint8_t> records,
    std::span<std::uint8_t> encoded, std::size_t& encodedBytes) noexcept;
HFT_COMPRESSOR_API Status decodeByteBlock(ByteBlockWorkspace& workspace,
    const ByteBlockLayout& layout, std::span<const std::uint8_t> encoded,
    std::span<std::uint8_t> records, std::size_t& recordBytes) noexcept;
HFT_COMPRESSOR_API Status encodeByteBlock(const ByteBlockLayout& layout,
    std::span<const std::uint8_t> records, std::span<std::uint8_t> encoded,
    std::size_t& encodedBytes) noexcept;
HFT_COMPRESSOR_API Status decodeByteBlock(const ByteBlockLayout& layout,
    std::span<const std::uint8_t> encoded, std::span<std::uint8_t> records,
    std::size_t& recordBytes) noexcept;

} // namespace hft_compressor

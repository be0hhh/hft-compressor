#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string_view>

#include "hft_compressor/Api.hpp"
#include "hft_compressor/Status.hpp"
#include "hft_compressor/StreamType.hpp"

namespace hft_compressor {

inline constexpr std::size_t kByteBlockMaxRecords = 256;
inline constexpr std::size_t kByteBlockMaxRecordBytes = 1024;
inline constexpr std::size_t kByteBlockMaxDepthLevels = 32;
inline constexpr std::size_t kByteBlockHeaderBytes = 62;
inline constexpr std::uint16_t kByteBlockCodec = 1;

// Two-tier lossless ByteBlock modes:
// - Online live tiers: Lz4Fast (default), ZstdFast, TransformOnly, Opaque.
// - Cold/archival & lab tiers: ZstdMax, Brotli, XzLzma, ArithmeticShift, RansByteStatic.
// Every mode writes into the single current HBC\0 wire format and falls back
// monotonically to Transform/Opaque when a block is incompressible, respecting
// byteBlockEncodeBound(layout, recordCount).
enum class ByteBlockCodecMode : std::uint8_t {
    Opaque = 0u,
    TransformOnly = 1u,
    Lz4Fast = 2u,
    ZstdFast = 3u,
    ZstdMax = 4u,
    Brotli = 5u,
    XzLzma = 6u,
    ArithmeticShift = 7u,
    RansByteStatic = 8u,
};

// Cold, independently decodable blocks. Offsets are record-relative; the
// caller owns the compact per-channel record schema (1..kByteBlockMaxRecordBytes,
// zero 1024B tail-padding assumptions). Unknown and DepthCoverage select
// plane-wise modular byte deltas without numeric payload assumptions.
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

HFT_COMPRESSOR_API bool validByteBlockLayout(const ByteBlockLayout& layout) noexcept;
HFT_COMPRESSOR_API ByteBlockCodecMode defaultByteBlockCodecMode() noexcept;
HFT_COMPRESSOR_API bool byteBlockCodecModeAvailable(ByteBlockCodecMode mode) noexcept;
HFT_COMPRESSOR_API std::string_view byteBlockCodecModeName(ByteBlockCodecMode mode) noexcept;
HFT_COMPRESSOR_API bool parseByteBlockCodecMode(
    std::string_view text, ByteBlockCodecMode& out) noexcept;

// Zero means invalid layout/count. Includes the complete encoded block.
HFT_COMPRESSOR_API std::size_t byteBlockEncodeBound(
    const ByteBlockLayout& layout, std::size_t recordCount) noexcept;

// Caller-owned cold scratch, reused across any valid layout, block count and mode.
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
    friend Status encodeByteBlock(ByteBlockWorkspace&, const ByteBlockLayout&,
        std::span<const std::uint8_t>, ByteBlockCodecMode,
        std::span<std::uint8_t>, std::size_t&) noexcept;
    friend Status decodeByteBlock(ByteBlockWorkspace&, const ByteBlockLayout&,
        std::span<const std::uint8_t>, std::span<std::uint8_t>, std::size_t&) noexcept;
};

// No output is published on failure (including overlapping input/output).
// All bytes, padding, clocks and input order round-trip exactly. State resets
// at each call. Prepared workspace overloads perform no per-block scratch
// allocation; convenience overloads prepare private scratch with the same ceiling.
// Default overloads select defaultByteBlockCodecMode() (online fast tier: Lz4Fast).
HFT_COMPRESSOR_API Status encodeByteBlock(ByteBlockWorkspace& workspace,
    const ByteBlockLayout& layout, std::span<const std::uint8_t> records,
    std::span<std::uint8_t> encoded, std::size_t& encodedBytes) noexcept;
HFT_COMPRESSOR_API Status encodeByteBlock(ByteBlockWorkspace& workspace,
    const ByteBlockLayout& layout, std::span<const std::uint8_t> records,
    ByteBlockCodecMode mode, std::span<std::uint8_t> encoded,
    std::size_t& encodedBytes) noexcept;
HFT_COMPRESSOR_API Status decodeByteBlock(ByteBlockWorkspace& workspace,
    const ByteBlockLayout& layout, std::span<const std::uint8_t> encoded,
    std::span<std::uint8_t> records, std::size_t& recordBytes) noexcept;
HFT_COMPRESSOR_API Status encodeByteBlock(const ByteBlockLayout& layout,
    std::span<const std::uint8_t> records, std::span<std::uint8_t> encoded,
    std::size_t& encodedBytes) noexcept;
HFT_COMPRESSOR_API Status encodeByteBlock(const ByteBlockLayout& layout,
    std::span<const std::uint8_t> records, ByteBlockCodecMode mode,
    std::span<std::uint8_t> encoded, std::size_t& encodedBytes) noexcept;
HFT_COMPRESSOR_API Status decodeByteBlock(const ByteBlockLayout& layout,
    std::span<const std::uint8_t> encoded, std::span<std::uint8_t> records,
    std::size_t& recordBytes) noexcept;

} // namespace hft_compressor

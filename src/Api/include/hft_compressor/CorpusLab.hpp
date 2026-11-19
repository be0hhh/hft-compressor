#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <span>
#include <string_view>
#include <type_traits>
#include <vector>

#include "hft_compressor/Api.hpp"
#include "hft_compressor/ByteBlock.hpp"
#include "hft_compressor/Status.hpp"
#include "hft_compressor/StreamType.hpp"

namespace hft_compressor {

// Self-describing compact binary record slice header ("HBS\0", 48 bytes) used
// by standalone binary slice files and artifact roundtrip verification.
inline constexpr std::uint32_t kBinarySliceMagic = 0x00534248u; // HBS\0
inline constexpr std::size_t kBinarySliceHeaderBytes = 48u;

struct BinarySliceView final {
    ByteBlockLayout layout{};
    std::uint32_t recordCount{0u};
    std::uint32_t recordsCrc32c{0u};
    std::span<const std::uint8_t> records{};
};

HFT_COMPRESSOR_API Status encodeBinarySliceHeader(
    const ByteBlockLayout& layout,
    std::span<const std::uint8_t> records,
    std::span<std::uint8_t, kBinarySliceHeaderBytes> outHeader) noexcept;

HFT_COMPRESSOR_API Status writeBinarySlice(
    const ByteBlockLayout& layout,
    std::span<const std::uint8_t> records,
    std::vector<std::uint8_t>& outSlice) noexcept;

HFT_COMPRESSOR_API Status parseBinarySlice(
    std::span<const std::uint8_t> sliceBytes,
    BinarySliceView& outView) noexcept;

// Per-stream-family lossless Compression Lab over compact binary record slices.
// Mandatory baselines (lz4, zstd, brotli, xz/lzma) are always evaluated before
// claiming any custom codec beats baselines on a stream family.
inline constexpr std::size_t kCorpusLabMaxCodecs = 16u;
inline constexpr std::size_t kCorpusLabMaxCorpusIdBytes = 128u;

enum class CorpusLabCodecId : std::uint8_t {
    BaselineLz4 = 0u,
    BaselineZstdFast = 1u,
    BaselineZstdMax = 2u,
    BaselineBrotli = 3u,
    BaselineXzLzma = 4u,
    ByteBlockTransformOnly = 5u,
    ByteBlockLz4Fast = 6u,
    ByteBlockZstdFast = 7u,
    ByteBlockZstdMax = 8u,
    ByteBlockBrotli = 9u,
    ByteBlockXzLzma = 10u,
    ByteBlockArithmeticShift = 11u,
    ByteBlockRansByteStatic = 12u,
    HftMacStreamTransform = 13u,
    HftMacStreamArithmeticShift = 14u,
    HftMacStreamRansByteStatic = 15u,
};

[[nodiscard]] inline constexpr std::uint32_t corpusLabCodecBit(
    CorpusLabCodecId id) noexcept {
    const auto index = static_cast<std::uint8_t>(id);
    return index < kCorpusLabMaxCodecs ? (std::uint32_t{1u} << index) : 0u;
}

inline constexpr std::uint32_t kCorpusLabMandatoryBaselinesMask =
    corpusLabCodecBit(CorpusLabCodecId::BaselineLz4) |
    corpusLabCodecBit(CorpusLabCodecId::BaselineZstdFast) |
    corpusLabCodecBit(CorpusLabCodecId::BaselineZstdMax) |
    corpusLabCodecBit(CorpusLabCodecId::BaselineBrotli) |
    corpusLabCodecBit(CorpusLabCodecId::BaselineXzLzma);

inline constexpr std::uint32_t kCorpusLabAllCodecsMask =
    (std::uint32_t{1u} << kCorpusLabMaxCodecs) - 1u;

struct CorpusLabCodecDescriptor final {
    CorpusLabCodecId codecId{CorpusLabCodecId::BaselineLz4};
    bool isBaseline{true};
    bool isOnlineTier{false};
    bool isByteBlockCodec{false};
    ByteBlockCodecMode byteBlockMode{ByteBlockCodecMode::Opaque};
    std::string_view id{};
    std::string_view label{};
    std::string_view transform{};
    std::string_view entropy{};
};

// Bounded integer-only POD result row per codec (zero floating-point fields).
struct CorpusLabCodecEntry final {
    CorpusLabCodecId codecId{CorpusLabCodecId::BaselineLz4};
    StreamType streamFamily{StreamType::Unknown};
    Status status{Status::NotImplemented};
    ByteBlockCodecMode byteBlockMode{ByteBlockCodecMode::Opaque};
    bool isBaseline{false};
    bool isOnlineTier{false};
    bool isByteBlockCodec{false};
    bool losslessVerified{false};
    bool beatsAllBaselinesInRatio{false};
    std::uint16_t ratioRank{0u};
    std::uint32_t blockRecords{0u};
    std::uint32_t blockBytes{0u};
    std::uint32_t rawCrc32c{0u};
    std::uint32_t decodedCrc32c{0u};
    std::uint64_t recordCount{0u};
    std::uint64_t blockCount{0u};
    std::uint64_t rawBytes{0u};
    std::uint64_t compressedBytes{0u};
    // Parts per million: 1,000,000 == 1.000000x (rawBytes * 1,000,000 / compressedBytes).
    std::uint64_t compressionRatioPpm{0u};
    // Permille saved: (rawBytes - compressedBytes) * 1,000 / rawBytes when compressedBytes < rawBytes.
    std::uint64_t spaceSavingsPermille{0u};
    std::uint64_t encodeNs{0u};
    std::uint64_t decodeNs{0u};
    std::uint64_t encodeCycles{0u};
    std::uint64_t decodeCycles{0u};
    std::uint64_t encodeBytesPerSec{0u};
    std::uint64_t decodeBytesPerSec{0u};
};
static_assert(std::is_trivially_copyable_v<CorpusLabCodecEntry>);

struct CorpusLabRequest final {
    ByteBlockLayout layout{};
    std::span<const std::uint8_t> records{};
    const char* corpusIdData{nullptr};
    std::size_t corpusIdSize{0u};
    std::uint32_t blockRecords{static_cast<std::uint32_t>(kByteBlockMaxRecords)};
    std::uint32_t codecMask{kCorpusLabAllCodecsMask};

    constexpr void setCorpusId(const char* data, std::size_t size) noexcept {
        corpusIdData = data;
        corpusIdSize = size;
    }
    constexpr void setCorpusId(std::string_view value) noexcept {
        corpusIdData = value.data();
        corpusIdSize = value.size();
    }
};

struct CorpusLabReport final {
    Status status{Status::Ok};
    StreamType streamFamily{StreamType::Unknown};
    std::uint32_t recordBytes{0u};
    std::uint32_t blockRecords{0u};
    std::uint32_t blockBytes{0u};
    std::uint32_t rawCrc32c{0u};
    std::uint64_t recordCount{0u};
    std::uint64_t rawBytes{0u};
    std::uint16_t corpusIdBytes{0u};
    std::uint16_t entryCount{0u};
    std::uint16_t verifiedCount{0u};
    bool mandatoryBaselinesComplete{false};
    CorpusLabCodecId bestRatioCodec{CorpusLabCodecId::BaselineLz4};
    CorpusLabCodecId bestOnlineCodec{CorpusLabCodecId::ByteBlockLz4Fast};
    std::array<char, kCorpusLabMaxCorpusIdBytes> corpusIdStorage{};
    std::array<CorpusLabCodecEntry, kCorpusLabMaxCodecs> entries{};

    [[nodiscard]] constexpr const char* corpusIdData() const noexcept {
        return corpusIdStorage.data();
    }
    [[nodiscard]] constexpr std::size_t corpusIdSize() const noexcept {
        return corpusIdBytes;
    }
    [[nodiscard]] constexpr std::string_view corpusIdView() const noexcept {
        return {corpusIdStorage.data(), corpusIdBytes};
    }
    [[nodiscard]] constexpr std::span<const CorpusLabCodecEntry> activeEntries() const noexcept {
        return {entries.data(), entryCount};
    }
};
static_assert(std::is_trivially_copyable_v<CorpusLabReport>);

class HFT_COMPRESSOR_API CorpusLabWorkspace final {
public:
    CorpusLabWorkspace() noexcept;
    ~CorpusLabWorkspace();
    CorpusLabWorkspace(const CorpusLabWorkspace&) = delete;
    CorpusLabWorkspace& operator=(const CorpusLabWorkspace&) = delete;
    Status prepare() noexcept;
    void reset() noexcept;

private:
    struct Scratch;
    std::unique_ptr<Scratch> scratch_;
    friend Status runCorpusLab(CorpusLabWorkspace&, const CorpusLabRequest&,
        CorpusLabReport&) noexcept;
};

HFT_COMPRESSOR_API std::span<const CorpusLabCodecDescriptor> listCorpusLabCodecs() noexcept;
HFT_COMPRESSOR_API std::string_view corpusLabCodecIdName(CorpusLabCodecId id) noexcept;
HFT_COMPRESSOR_API std::string_view corpusLabCodecLabel(CorpusLabCodecId id) noexcept;
HFT_COMPRESSOR_API std::string_view corpusLabCodecTransformName(CorpusLabCodecId id) noexcept;
HFT_COMPRESSOR_API std::string_view corpusLabCodecEntropyName(CorpusLabCodecId id) noexcept;

HFT_COMPRESSOR_API Status runCorpusLab(
    CorpusLabWorkspace& workspace,
    const CorpusLabRequest& request,
    CorpusLabReport& report) noexcept;

HFT_COMPRESSOR_API Status runCorpusLab(
    const CorpusLabRequest& request,
    CorpusLabReport& report) noexcept;

} // namespace hft_compressor

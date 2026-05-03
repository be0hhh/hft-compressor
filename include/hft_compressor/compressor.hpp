#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <functional>
#include <span>
#include <string>
#include <vector>

#include "hft_compressor/api.hpp"
#include "hft_compressor/pipeline.hpp"
#include "hft_compressor/result.hpp"

namespace hft_compressor {

struct CompressionRequest {
    std::filesystem::path inputPath{};
    std::filesystem::path outputRoot{};
    std::filesystem::path outputPathOverride{};
    std::string pipelineId{};
    std::uint32_t blockBytes{1024u * 1024u};
    int zstdLevel{3};
};

enum class VerifyMode : std::uint8_t {
    ByteExact = 1u,
    RecordExact = 2u,
    Both = 3u,
};

struct DecodeVerifyRequest {
    std::filesystem::path compressedPath{};
    std::filesystem::path canonicalPath{};
    std::string pipelineId{"std.zstd_jsonl_blocks_v1"};
    bool verifyBytes{true};
    VerifyMode verifyMode{VerifyMode::Both};
};

using DecodedBlockCallback = std::function<bool(std::span<const std::uint8_t> block)>;

enum class ReplayRecordKind : std::uint8_t {
    Unknown = 0u,
    Trade = 1u,
    BookTicker = 2u,
    Depth = 3u,
};

inline constexpr std::size_t kReplaySymbolBytes = 32u;
inline constexpr std::size_t kReplayExchangeBytes = 24u;
inline constexpr std::size_t kReplayMarketBytes = 24u;

struct ReplayTradeRecord {
    std::array<char, kReplaySymbolBytes> symbol{};
    std::array<char, kReplayExchangeBytes> exchange{};
    std::array<char, kReplayMarketBytes> market{};
    std::int64_t tsNs{0};
    std::int64_t priceE8{0};
    std::int64_t qtyE8{0};
    std::int64_t side{0};
};

struct ReplayBookTickerRecord {
    std::array<char, kReplaySymbolBytes> symbol{};
    std::array<char, kReplayExchangeBytes> exchange{};
    std::array<char, kReplayMarketBytes> market{};
    std::int64_t tsNs{0};
    std::int64_t bidPriceE8{0};
    std::int64_t bidQtyE8{0};
    std::int64_t askPriceE8{0};
    std::int64_t askQtyE8{0};
};

struct ReplayDepthLevel {
    std::int64_t priceE8{0};
    std::int64_t qtyE8{0};
    std::int64_t side{0};
};

struct ReplayDepthRecord {
    std::int64_t tsNs{0};
    std::vector<ReplayDepthLevel> levels{};
};

struct ReplayRecord {
    ReplayRecordKind kind{ReplayRecordKind::Unknown};
    ReplayTradeRecord trade{};
    ReplayBookTickerRecord bookTicker{};
    ReplayDepthRecord depth{};
};

using DecodedRecordCallback = std::function<bool(const ReplayRecord& record)>;

enum class ArtifactPreference : std::uint8_t {
    CurrentBaseline = 1u,
    Replay = 2u,
    Archive = 3u,
    Live = 4u,
};

struct ReplayArtifactRequest {
    std::filesystem::path compressedRoot{};
    std::filesystem::path sessionDir{};
    std::string sessionId{};
    StreamType streamType{StreamType::Unknown};
    std::string preferredPipelineId{};
    ArtifactPreference preference{ArtifactPreference::CurrentBaseline};
};

struct ReplayArtifactInfo {
    Status status{Status::Ok};
    bool found{false};
    std::string error{};
    std::filesystem::path path{};
    std::string formatId{};
    std::string pipelineId{};
    std::string transform{};
    std::string entropy{};
    StreamType streamType{StreamType::Unknown};
    std::uint16_t version{0};
    std::uint64_t inputBytes{0};
    std::uint64_t outputBytes{0};
    std::uint64_t lineCount{0};
    std::uint64_t blockCount{0};
};

struct HfcBlockInfo {
    std::uint64_t fileOffset{0};
    std::uint32_t uncompressedBytes{0};
    std::uint32_t compressedBytes{0};
    std::uint32_t lineCount{0};
    std::uint64_t firstByteOffset{0};
    std::uint32_t compressedCrc32c{0};
    std::uint32_t uncompressedCrc32c{0};
};

struct HfcFileInfo {
    Status status{Status::Ok};
    std::string error{};
    std::filesystem::path path{};
    StreamType streamType{StreamType::Unknown};
    std::uint16_t version{0};
    std::uint16_t codec{0};
    std::uint32_t blockBytes{0};
    std::uint64_t inputBytes{0};
    std::uint64_t outputBytes{0};
    std::uint64_t lineCount{0};
    std::uint64_t blockCount{0};
    std::vector<HfcBlockInfo> blocks{};
};

HFT_COMPRESSOR_API std::filesystem::path defaultOutputRoot();
HFT_COMPRESSOR_API CompressionResult compress(const CompressionRequest& request) noexcept;
HFT_COMPRESSOR_API DecodeVerifyResult decodeAndVerify(const DecodeVerifyRequest& request) noexcept;
HFT_COMPRESSOR_API ReplayArtifactInfo discoverReplayArtifact(const ReplayArtifactRequest& request) noexcept;
HFT_COMPRESSOR_API Status decodeReplayArtifactJsonl(const ReplayArtifactInfo& artifact,
                    const DecodedBlockCallback& onBlock) noexcept;
HFT_COMPRESSOR_API Status decodeReplayJsonl(const ReplayArtifactRequest& request,
                    const DecodedBlockCallback& onBlock) noexcept;
HFT_COMPRESSOR_API Status inspectCompressedArtifact(const std::filesystem::path& path,
                    std::string_view pipelineId,
                    std::string_view view,
                    const DecodedBlockCallback& onBlock) noexcept;
HFT_COMPRESSOR_API Status decodeReplayRecords(const ReplayArtifactRequest& request,
                    const DecodedRecordCallback& onRecord) noexcept;
HFT_COMPRESSOR_API HfcFileInfo openHfcFile(const std::filesystem::path& path) noexcept;
HFT_COMPRESSOR_API Status decodeHfcFile(const std::filesystem::path& path,
                    const DecodedBlockCallback& onBlock) noexcept;
HFT_COMPRESSOR_API Status decodeHfcBuffer(std::span<const std::uint8_t> compressedFile,
                    const DecodedBlockCallback& onBlock) noexcept;

}  // namespace hft_compressor

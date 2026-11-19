#include "hft_compressor/Compressor.hpp"
#include "hft_compressor/CorpusLab.hpp"

#include <algorithm>
#include <array>
#include <atomic>
#include <cstdint>
#include <fcntl.h>
#include <fstream>
#include <limits>
#include <string>
#include <string_view>
#include <sys/stat.h>
#include <unistd.h>
#include <vector>

#include "../../Runtime/src/Codecs/BaseDecode.hpp"
#include "../../Runtime/src/Codecs/ByteBlock/BinaryTransform.hpp"
#include "../../Runtime/src/Codecs/EntropyHftmac/EntropyHftMacInternal.hpp"
#include "../../Runtime/src/Codecs/ExternalBlockPrimitives.hpp"
#include "../../Runtime/src/Common/CompressionInternals.hpp"
#include "../../Runtime/src/Common/ExactScaleMath.hpp"
#include "../../Runtime/src/Common/Timing.hpp"
#include "../../Runtime/src/Container/Hfc/Format.hpp"
#include "../../Runtime/src/Pipelines/PipelineBackend.hpp"

namespace hft_compressor {
namespace {

namespace ext = codecs::external_primitives;
namespace entropy = codecs::entropy_hftmac::detail;

// Only compressed artifacts/metrics are staged, never a decoded-base spool.
class PendingArtifact {
    int artifactFd_{-1}, metricsFd_{-1};
    static bool owns(const std::filesystem::path& path, int fd) noexcept {
        if (fd < 0) return false;
        struct stat opened{}, current{};
        return ::fstat(fd, &opened) == 0 && ::lstat(path.c_str(), &current) == 0
            && S_ISREG(current.st_mode) && opened.st_dev == current.st_dev && opened.st_ino == current.st_ino;
    }
    static void removeOwned(const std::filesystem::path& path, int fd) noexcept {
        if (fd < 0) return;
        if (owns(path, fd)) (void)::unlink(path.c_str());
        (void)::close(fd);
    }
public:
    std::filesystem::path artifact, metrics;
    explicit PendingArtifact(const std::filesystem::path& finalPath) {
        static std::atomic<std::uint64_t> sequence{};
        const auto serial = sequence.fetch_add(1u, std::memory_order_relaxed);
        const auto name = "." + finalPath.stem().string() + ".partial."
            + std::to_string(::getpid()) + "." + std::to_string(serial) + finalPath.extension().string();
        artifact = finalPath.parent_path() / name;
        metrics = artifact.parent_path() / (artifact.stem().string() + ".metrics.json");
    }
    ~PendingArtifact() {
        removeOwned(artifact, artifactFd_);
        removeOwned(metrics, metricsFd_);
    }
    bool reserve() noexcept {
        artifactFd_ = ::open(artifact.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0666);
        if (artifactFd_ < 0) return false;
        metricsFd_ = ::open(metrics.c_str(), O_WRONLY | O_CREAT | O_EXCL | O_CLOEXEC | O_NOFOLLOW, 0666);
        return metricsFd_ >= 0;
    }
    bool ownsFiles() const noexcept { return owns(artifact, artifactFd_) && owns(metrics, metricsFd_); }
};

CompressionResult compressToPublishedArtifact(const CompressionRequest& request,
                                              const PipelineDescriptor& pipeline,
                                              const pipelines::PipelineBackend& backend) {
    const auto stream = inferStreamTypeFromPath(request.inputPath);
    if (stream == StreamType::Unknown) {
        return internal::fail(Status::UnsupportedStream, request, &pipeline, "input stream is unknown");
    }
    const auto finalPath = internal::outputPathFor(request, pipeline, stream);
    const auto parent = finalPath.parent_path();
    std::error_code error;
    if (!parent.empty()) std::filesystem::create_directories(parent, error);
    if (error) return internal::fail(Status::IoError, request, &pipeline, "failed to create artifact directory");
    PendingArtifact pending(finalPath);
    if (!pending.reserve()) return internal::fail(Status::IoError, request, &pipeline, "failed to reserve owned partial artifact");
    CompressionRequest staged = request;
    staged.outputPathOverride = pending.artifact;
    auto result = backend.compress(staged, pipeline);
    if (!isOk(result.status) || !result.roundtripOk) {
        if (isOk(result.status)) result.status = Status::DecodeError;
        result.outputPath.clear();
        result.metricsPath.clear();
        return result;
    }
    if (result.outputPath != pending.artifact || result.metricsPath != pending.metrics || !pending.ownsFiles()) {
        result.status = Status::IoError;
        result.error = "verified staged artifact identity changed";
        result.outputPath.clear();
        result.metricsPath.clear();
        return result;
    }
    result.outputPath = finalPath;
    result.metricsPath = parent / (finalPath.stem().string() + ".metrics.json");
    const auto text = toMetricsJson(result);
    std::ofstream metadata(pending.metrics, std::ios::binary | std::ios::trunc);
    metadata.write(text.data(), static_cast<std::streamsize>(text.size()));
    metadata.close();
    if (!metadata) {
        result.status = Status::IoError;
        result.error = "failed to finish artifact metrics";
        result.outputPath.clear();
        result.metricsPath.clear();
        return result;
    }
    if (!pending.ownsFiles()) {
        result.status = Status::IoError;
        result.error = "staged artifact identity changed before publication";
        result.outputPath.clear();
        result.metricsPath.clear();
        return result;
    }
    std::filesystem::rename(pending.artifact, finalPath, error);
    if (error) {
        result.status = Status::IoError;
        result.error = "failed to publish verified artifact";
        result.outputPath.clear();
        result.metricsPath.clear();
        return result;
    }
    std::filesystem::rename(pending.metrics, result.metricsPath, error);
    if (error) {
        result.status = Status::IoError;
        result.error = "verified artifact published; metrics publication failed";
    }
    return result;
}

constexpr auto kLabDescriptors = std::to_array<CorpusLabCodecDescriptor>({
    {CorpusLabCodecId::BaselineLz4, true, true, false, ByteBlockCodecMode::Opaque, "baseline.lz4", "Baseline LZ4", "raw_binary", "lz4"},
    {CorpusLabCodecId::BaselineZstdFast, true, true, false, ByteBlockCodecMode::Opaque, "baseline.zstd_fast", "Baseline Zstd fast (L1)", "raw_binary", "zstd_fast"},
    {CorpusLabCodecId::BaselineZstdMax, true, false, false, ByteBlockCodecMode::Opaque, "baseline.zstd_max", "Baseline Zstd max (L19)", "raw_binary", "zstd_max"},
    {CorpusLabCodecId::BaselineBrotli, true, false, false, ByteBlockCodecMode::Opaque, "baseline.brotli", "Baseline Brotli", "raw_binary", "brotli"},
    {CorpusLabCodecId::BaselineXzLzma, true, false, false, ByteBlockCodecMode::Opaque, "baseline.xz_lzma", "Baseline XZ/LZMA", "raw_binary", "xz_lzma"},
    {CorpusLabCodecId::ByteBlockTransformOnly, false, true, true, ByteBlockCodecMode::TransformOnly, "byteblock.transform_only", "ByteBlock domain transform + plane delta", "stream_column_plane_delta", "none"},
    {CorpusLabCodecId::ByteBlockLz4Fast, false, true, true, ByteBlockCodecMode::Lz4Fast, "byteblock.lz4_fast", "ByteBlock transform + LZ4 fast (online)", "stream_column_plane_delta", "lz4_fast"},
    {CorpusLabCodecId::ByteBlockZstdFast, false, true, true, ByteBlockCodecMode::ZstdFast, "byteblock.zstd_fast", "ByteBlock transform + Zstd fast (online)", "stream_column_plane_delta", "zstd_fast"},
    {CorpusLabCodecId::ByteBlockZstdMax, false, false, true, ByteBlockCodecMode::ZstdMax, "byteblock.zstd_max", "ByteBlock transform + Zstd max (archive)", "stream_column_plane_delta", "zstd_max"},
    {CorpusLabCodecId::ByteBlockBrotli, false, false, true, ByteBlockCodecMode::Brotli, "byteblock.brotli", "ByteBlock transform + Brotli (archive)", "stream_column_plane_delta", "brotli"},
    {CorpusLabCodecId::ByteBlockXzLzma, false, false, true, ByteBlockCodecMode::XzLzma, "byteblock.xz_lzma", "ByteBlock transform + XZ/LZMA (archive)", "stream_column_plane_delta", "xz_lzma"},
    {CorpusLabCodecId::ByteBlockArithmeticShift, false, false, true, ByteBlockCodecMode::ArithmeticShift, "byteblock.arithmetic_shift", "ByteBlock transform + 15-bit shift AC", "stream_column_plane_delta", "arithmetic_shift"},
    {CorpusLabCodecId::ByteBlockRansByteStatic, false, false, true, ByteBlockCodecMode::RansByteStatic, "byteblock.rans_byte_static", "ByteBlock transform + 12-bit byte rANS", "stream_column_plane_delta", "rans_byte_static"},
    {CorpusLabCodecId::HftMacStreamTransform, false, false, false, ByteBlockCodecMode::Opaque, "hftmac.stream_transform", "HFT-MAC stream-family GCD columnar transform", "hftmac_stream_gcd_columnar", "none"},
    {CorpusLabCodecId::HftMacStreamArithmeticShift, false, false, false, ByteBlockCodecMode::Opaque, "hftmac.stream_arithmetic_shift", "HFT-MAC stream-family + 15-bit shift AC", "hftmac_stream_gcd_columnar", "arithmetic_shift"},
    {CorpusLabCodecId::HftMacStreamRansByteStatic, false, false, false, ByteBlockCodecMode::Opaque, "hftmac.stream_rans_byte_static", "HFT-MAC stream-family + 12-bit byte rANS", "hftmac_stream_gcd_columnar", "rans_byte_static"},
});

// Cold summary integer quotients computed once per benchmarked codec row for
// reporting/IPC display; codec ranking below uses exact 128-bit cross-products.
void populateColdSummaryRates(CorpusLabCodecEntry& entry) noexcept {
    if (!entry.losslessVerified || entry.compressedBytes == 0u || entry.rawBytes == 0u) return;
    entry.compressionRatioPpm = static_cast<std::uint64_t>(
        (static_cast<unsigned __int128>(entry.rawBytes) * 1'000'000u) / entry.compressedBytes);
    if (entry.compressedBytes < entry.rawBytes) {
        entry.spaceSavingsPermille = static_cast<std::uint64_t>(
            (static_cast<unsigned __int128>(entry.rawBytes - entry.compressedBytes) * 1'000u) / entry.rawBytes);
    }
    if (entry.encodeNs > 0u) {
        entry.encodeBytesPerSec = static_cast<std::uint64_t>(
            (static_cast<unsigned __int128>(entry.rawBytes) * 1'000'000'000ull) / entry.encodeNs);
    }
    if (entry.decodeNs > 0u) {
        entry.decodeBytesPerSec = static_cast<std::uint64_t>(
            (static_cast<unsigned __int128>(entry.rawBytes) * 1'000'000'000ull) / entry.decodeNs);
    }
}

bool betterRatioCrossProduct(const CorpusLabCodecEntry& a, const CorpusLabCodecEntry& b) noexcept {
    // Compare rawBytes / compressedBytes <=> a.rawBytes * b.compressedBytes > b.rawBytes * a.compressedBytes.
    // Since both share the same slice rawBytes, higher ratio <=> smaller compressedBytes!
    if (a.compressedBytes != b.compressedBytes) {
        return a.compressedBytes < b.compressedBytes;
    }
    if (a.decodeNs != b.decodeNs) return a.decodeNs < b.decodeNs;
    if (a.encodeNs != b.encodeNs) return a.encodeNs < b.encodeNs;
    return static_cast<std::uint8_t>(a.codecId) < static_cast<std::uint8_t>(b.codecId);
}

} // namespace

std::filesystem::path defaultOutputRoot() {
    const auto cwd = std::filesystem::current_path();
    if (cwd.filename() == "hft-recorder") return cwd.parent_path() / "hft-compressor" / "compressedData";
    if (cwd.filename() == "hft-compressor") return cwd / "compressedData";
    return cwd / "compressedData";
}

CompressionResult compress(const CompressionRequest& request) noexcept {
    try {
        const auto* pipeline = findPipeline(request.pipelineId);
        if (pipeline == nullptr) {
            return internal::fail(Status::UnsupportedPipeline, request, nullptr, "unknown pipeline id");
        }
        if (pipeline->availability == PipelineAvailability::DependencyUnavailable) {
            return internal::fail(Status::DependencyUnavailable, request, pipeline, std::string{pipeline->availabilityReason});
        }
        if (pipeline->availability == PipelineAvailability::NotImplemented) {
            return internal::fail(Status::NotImplemented, request, pipeline, std::string{pipeline->availabilityReason});
        }
        if (const auto* backend = pipelines::findBackend(pipeline->id); backend != nullptr && backend->compress != nullptr) {
            return compressToPublishedArtifact(request, *pipeline, *backend);
        }
        return internal::fail(Status::UnsupportedPipeline, request, pipeline, "pipeline has no compressor implementation");
    } catch (...) {
        CompressionResult failed{};
        failed.status = Status::DecodeError;
        return failed;
    }
}

Status decodeHfcBuffer(std::span<const std::uint8_t> compressedFile,
                       const DecodedBlockCallback& onBlock) noexcept {
    const auto* backend = pipelines::findBackend("std.zstd_blocks");
    return backend != nullptr && backend->decodeBuffer != nullptr
        ? backend->decodeBuffer(compressedFile, onBlock)
        : Status::NotImplemented;
}

Status decodeHfcFile(const std::filesystem::path& path,
                     const DecodedBlockCallback& onBlock) noexcept {
    const auto* backend = pipelines::findBackend("std.zstd_blocks");
    return backend != nullptr && backend->decodeJsonl != nullptr
        ? backend->decodeJsonl(path, onBlock)
        : Status::NotImplemented;
}

Status encodeBinarySliceHeader(
    const ByteBlockLayout& layout,
    std::span<const std::uint8_t> records,
    std::span<std::uint8_t, kBinarySliceHeaderBytes> outHeader) noexcept {
    if (!validByteBlockLayout(layout) || records.empty()) return Status::InvalidArgument;
    std::size_t count = 0u;
    if (!math::exactRecordCount(records.size(), layout.recordBytes, count) ||
        count > std::numeric_limits<std::uint32_t>::max()) {
        return Status::InvalidArgument;
    }
    if (layout.streamType == StreamType::Depth) {
        for (std::size_t off = 0u; off < records.size(); off += layout.recordBytes) {
            if (codecs::byte_block::word(records, off + layout.depthLevelCountOffset, 2u) >
                layout.depthLevelCapacity) {
                return Status::InvalidArgument;
            }
        }
    }
    std::fill(outHeader.begin(), outHeader.end(), std::uint8_t{0u});
    codecs::byte_block::put(outHeader, 0u, kBinarySliceMagic, 4u);
    codecs::byte_block::put(outHeader, 4u, static_cast<std::uint32_t>(layout.streamType), 4u);
    codecs::byte_block::put(outHeader, 8u, layout.recordBytes, 4u);
    codecs::byte_block::put(outHeader, 12u, layout.payloadOffset, 4u);
    codecs::byte_block::put(outHeader, 16u, layout.timestampOffset, 4u);
    codecs::byte_block::put(outHeader, 20u, layout.depthLevelsOffset, 4u);
    codecs::byte_block::put(outHeader, 24u, layout.depthLevelStride, 4u);
    codecs::byte_block::put(outHeader, 28u, layout.depthLevelCountOffset, 4u);
    codecs::byte_block::put(outHeader, 32u, layout.depthLevelCapacity, 4u);
    codecs::byte_block::put(outHeader, 36u, static_cast<std::uint32_t>(count), 4u);
    codecs::byte_block::put(outHeader, 40u, format::crc32c(records), 4u);
    codecs::byte_block::put(outHeader, 44u, format::crc32c(outHeader), 4u);
    return Status::Ok;
}

Status writeBinarySlice(
    const ByteBlockLayout& layout,
    std::span<const std::uint8_t> records,
    std::vector<std::uint8_t>& outSlice) noexcept {
    try {
        std::array<std::uint8_t, kBinarySliceHeaderBytes> hdr{};
        const auto st = encodeBinarySliceHeader(layout, records, hdr);
        if (!isOk(st)) return st;
        outSlice.clear();
        outSlice.reserve(hdr.size() + records.size());
        outSlice.insert(outSlice.end(), hdr.begin(), hdr.end());
        outSlice.insert(outSlice.end(), records.begin(), records.end());
        return Status::Ok;
    } catch (...) {
        return Status::DecodeError;
    }
}

Status parseBinarySlice(
    std::span<const std::uint8_t> sliceBytes,
    BinarySliceView& outView) noexcept {
    outView = {};
    if (sliceBytes.size() <= kBinarySliceHeaderBytes) return Status::CorruptData;
    std::array<std::uint8_t, kBinarySliceHeaderBytes> hdr{};
    std::copy_n(sliceBytes.data(), kBinarySliceHeaderBytes, hdr.data());
    const auto storedHdrCrc = static_cast<std::uint32_t>(codecs::byte_block::word(hdr, 44u, 4u));
    codecs::byte_block::put(hdr, 44u, 0u, 4u);
    if (format::crc32c(hdr) != storedHdrCrc ||
        codecs::byte_block::word(hdr, 0u, 4u) != kBinarySliceMagic) {
        return Status::CorruptData;
    }

    ByteBlockLayout layout{};
    layout.streamType = static_cast<StreamType>(codecs::byte_block::word(hdr, 4u, 4u));
    layout.recordBytes = static_cast<std::uint32_t>(codecs::byte_block::word(hdr, 8u, 4u));
    layout.payloadOffset = static_cast<std::uint32_t>(codecs::byte_block::word(hdr, 12u, 4u));
    layout.timestampOffset = static_cast<std::uint32_t>(codecs::byte_block::word(hdr, 16u, 4u));
    layout.depthLevelsOffset = static_cast<std::uint32_t>(codecs::byte_block::word(hdr, 20u, 4u));
    layout.depthLevelStride = static_cast<std::uint32_t>(codecs::byte_block::word(hdr, 24u, 4u));
    layout.depthLevelCountOffset = static_cast<std::uint32_t>(codecs::byte_block::word(hdr, 28u, 4u));
    layout.depthLevelCapacity = static_cast<std::uint32_t>(codecs::byte_block::word(hdr, 32u, 4u));

    const auto recordCount = static_cast<std::uint32_t>(codecs::byte_block::word(hdr, 36u, 4u));
    const auto recordsCrc = static_cast<std::uint32_t>(codecs::byte_block::word(hdr, 40u, 4u));
    if (!validByteBlockLayout(layout) || recordCount == 0u) return Status::CorruptData;

    const auto body = sliceBytes.subspan(kBinarySliceHeaderBytes);
    if (static_cast<unsigned __int128>(recordCount) * layout.recordBytes != body.size()) {
        return Status::CorruptData;
    }
    if (format::crc32c(body) != recordsCrc) return Status::CorruptData;
    if (layout.streamType == StreamType::Depth) {
        for (std::size_t off = 0u; off < body.size(); off += layout.recordBytes) {
            if (codecs::byte_block::word(body, off + layout.depthLevelCountOffset, 2u) >
                layout.depthLevelCapacity) {
                return Status::CorruptData;
            }
        }
    }

    outView.layout = layout;
    outView.recordCount = recordCount;
    outView.recordsCrc32c = recordsCrc;
    outView.records = body;
    return Status::Ok;
}

struct CorpusLabWorkspace::Scratch {
    ByteBlockWorkspace byteBlockWorkspace{};
    entropy::ArithmeticWorkspace entropyWorkspace{};
    std::vector<std::uint8_t> lz4State{};
    void* zstdCCtx{nullptr};
    void* zstdDCtx{nullptr};
    std::vector<std::uint8_t> encodedBlocks{};
    std::vector<std::uint32_t> blockEncodedSizes{};
    std::vector<std::uint8_t> decodedRecords{};
    std::vector<std::uint8_t> streamBase{};
    std::vector<std::uint8_t> streamEntropy{};

    Scratch() {
        constexpr std::size_t maxBlock = kByteBlockMaxRecords * kByteBlockMaxRecordBytes;
        if (ext::lz4Available()) lz4State.resize(ext::lz4StateBytes());
        encodedBlocks.reserve(maxBlock * 4u);
        decodedRecords.reserve(maxBlock * 4u);
        blockEncodedSizes.reserve(256u);
    }

    ~Scratch() {
        if (zstdCCtx != nullptr) ext::freeZstdCCtx(zstdCCtx);
        if (zstdDCtx != nullptr) ext::freeZstdDCtx(zstdDCtx);
    }

    Scratch(const Scratch&) = delete;
    Scratch& operator=(const Scratch&) = delete;

    void* ensureZstdCCtx() noexcept {
        if (zstdCCtx == nullptr) zstdCCtx = ext::createZstdCCtx();
        return zstdCCtx;
    }

    void* ensureZstdDCtx() noexcept {
        if (zstdDCtx == nullptr) zstdDCtx = ext::createZstdDCtx();
        return zstdDCtx;
    }
};

CorpusLabWorkspace::CorpusLabWorkspace() noexcept = default;
CorpusLabWorkspace::~CorpusLabWorkspace() = default;

Status CorpusLabWorkspace::prepare() noexcept {
    if (scratch_) return Status::Ok;
    try {
        auto ptr = std::make_unique<Scratch>();
        const auto st = ptr->byteBlockWorkspace.prepare();
        if (!isOk(st)) return st;
        scratch_ = std::move(ptr);
        return Status::Ok;
    } catch (...) {
        return Status::DecodeError;
    }
}

void CorpusLabWorkspace::reset() noexcept {
    if (!scratch_) return;
    scratch_->byteBlockWorkspace.reset();
    scratch_->encodedBlocks.clear();
    scratch_->blockEncodedSizes.clear();
    scratch_->decodedRecords.clear();
    scratch_->streamBase.clear();
    scratch_->streamEntropy.clear();
}

std::span<const CorpusLabCodecDescriptor> listCorpusLabCodecs() noexcept {
    return kLabDescriptors;
}

std::string_view corpusLabCodecIdName(CorpusLabCodecId id) noexcept {
    const auto idx = static_cast<std::size_t>(id);
    return idx < kLabDescriptors.size() ? kLabDescriptors[idx].id : "unknown";
}

std::string_view corpusLabCodecLabel(CorpusLabCodecId id) noexcept {
    const auto idx = static_cast<std::size_t>(id);
    return idx < kLabDescriptors.size() ? kLabDescriptors[idx].label : "Unknown";
}

std::string_view corpusLabCodecTransformName(CorpusLabCodecId id) noexcept {
    const auto idx = static_cast<std::size_t>(id);
    return idx < kLabDescriptors.size() ? kLabDescriptors[idx].transform : "unknown";
}

std::string_view corpusLabCodecEntropyName(CorpusLabCodecId id) noexcept {
    const auto idx = static_cast<std::size_t>(id);
    return idx < kLabDescriptors.size() ? kLabDescriptors[idx].entropy : "unknown";
}

Status runCorpusLab(
    CorpusLabWorkspace& workspace,
    const CorpusLabRequest& request,
    CorpusLabReport& report) noexcept {
    report = {};
    if (!workspace.scratch_ || !validByteBlockLayout(request.layout) || request.records.empty() ||
        request.blockRecords == 0u || request.blockRecords > kByteBlockMaxRecords) {
        report.status = Status::InvalidArgument;
        return Status::InvalidArgument;
    }
    std::size_t totalRecords = 0u;
    if (!math::exactRecordCount(request.records.size(), request.layout.recordBytes, totalRecords)) {
        report.status = Status::InvalidArgument;
        return Status::InvalidArgument;
    }
    if (request.layout.streamType == StreamType::Depth) {
        for (std::size_t off = 0u; off < request.records.size(); off += request.layout.recordBytes) {
            if (codecs::byte_block::word(request.records, off + request.layout.depthLevelCountOffset, 2u) >
                request.layout.depthLevelCapacity) {
                report.status = Status::InvalidArgument;
                return Status::InvalidArgument;
            }
        }
    }

    // Enforce AGENTS.md: custom codecs always evaluate alongside mandatory baselines.
    std::uint32_t mask = request.codecMask & kCorpusLabAllCodecsMask;
    if (mask == 0u) mask = kCorpusLabAllCodecsMask;
    if ((mask & ~kCorpusLabMandatoryBaselinesMask) != 0u) {
        mask |= kCorpusLabMandatoryBaselinesMask;
    }

    try {
        workspace.reset();
        auto& scratch = *workspace.scratch_;
        const std::uint32_t rawCrc = format::crc32c(request.records);
        const std::size_t stride = request.layout.recordBytes;
        const std::size_t maxBlockBytes = static_cast<std::size_t>(request.blockRecords) * stride;

        report.status = Status::Ok;
        report.streamFamily = request.layout.streamType;
        report.recordBytes = request.layout.recordBytes;
        report.blockRecords = request.blockRecords;
        report.blockBytes = static_cast<std::uint32_t>(maxBlockBytes);
        report.rawCrc32c = rawCrc;
        report.recordCount = totalRecords;
        report.rawBytes = request.records.size();

        if (request.corpusIdData != nullptr && request.corpusIdSize != 0u) {
            const std::size_t copyLen = std::min<std::size_t>(
                request.corpusIdSize, report.corpusIdStorage.size());
            std::copy_n(request.corpusIdData, copyLen, report.corpusIdStorage.data());
            report.corpusIdBytes = static_cast<std::uint16_t>(copyLen);
        }

        // Precompute block count without runtime integer division.
        std::size_t blockCount = 0u;
        for (std::size_t pos = 0u; pos < totalRecords; pos += request.blockRecords) {
            ++blockCount;
        }

        auto benchmarkRawBaseline = [&](CorpusLabCodecEntry& entry, CorpusLabCodecId id) {
            bool avail = false;
            switch (id) {
                case CorpusLabCodecId::BaselineLz4: avail = ext::lz4Available(); break;
                case CorpusLabCodecId::BaselineZstdFast:
                case CorpusLabCodecId::BaselineZstdMax: avail = ext::zstdAvailable(); break;
                case CorpusLabCodecId::BaselineBrotli: avail = ext::brotliAvailable(); break;
                case CorpusLabCodecId::BaselineXzLzma: avail = ext::xzLzmaAvailable(); break;
                default: break;
            }
            if (!avail) {
                entry.status = Status::DependencyUnavailable;
                return;
            }

            scratch.encodedBlocks.clear();
            scratch.blockEncodedSizes.clear();
            scratch.blockEncodedSizes.reserve(blockCount);

            std::vector<std::uint8_t> blockScratch;
            const auto encStartNs = timing::nowNs();
            const auto encStartCycles = timing::readCycles();

            for (std::size_t rPos = 0u; rPos < totalRecords; rPos += request.blockRecords) {
                const std::size_t rTake = std::min<std::size_t>(request.blockRecords, totalRecords - rPos);
                const auto plain = request.records.subspan(rPos * stride, rTake * stride);
                std::size_t written = 0u;
                Status st = Status::DecodeError;
                switch (id) {
                    case CorpusLabCodecId::BaselineLz4:
                        blockScratch.resize(ext::lz4CompressBound(plain.size()));
                        st = ext::lz4CompressFast(scratch.lz4State, plain, blockScratch, written);
                        break;
                    case CorpusLabCodecId::BaselineZstdFast:
                        blockScratch.resize(ext::zstdCompressBound(plain.size()));
                        st = ext::zstdCompressWithCtx(scratch.ensureZstdCCtx(), 1, plain, blockScratch, written);
                        break;
                    case CorpusLabCodecId::BaselineZstdMax:
                        blockScratch.resize(ext::zstdCompressBound(plain.size()));
                        st = ext::zstdCompressWithCtx(scratch.ensureZstdCCtx(), 19, plain, blockScratch, written);
                        break;
                    case CorpusLabCodecId::BaselineBrotli:
                        blockScratch.resize(ext::brotliCompressBound(plain.size()));
                        st = ext::brotliCompressBlock(6, plain, blockScratch, written);
                        break;
                    case CorpusLabCodecId::BaselineXzLzma:
                        blockScratch.resize(ext::xzLzmaCompressBound(plain.size()));
                        st = ext::xzLzmaCompressBlock(4, plain, blockScratch, written);
                        break;
                    default:
                        break;
                }
                if (!isOk(st) || written == 0u) {
                    entry.status = isOk(st) ? Status::DecodeError : st;
                    return;
                }
                // Include 16B per-block framing overhead (compressedBytes + uncompressedBytes + 2xCRC32C)
                // so baseline block framing is apples-to-apples with framed block codecs.
                scratch.blockEncodedSizes.push_back(static_cast<std::uint32_t>(written));
                scratch.encodedBlocks.insert(
                    scratch.encodedBlocks.end(), blockScratch.begin(), blockScratch.begin() + static_cast<std::ptrdiff_t>(written));
            }
            entry.encodeCycles = timing::readCycles() - encStartCycles;
            entry.encodeNs = timing::nowNs() - encStartNs;
            entry.compressedBytes = scratch.encodedBlocks.size() + blockCount * 16u;

            scratch.decodedRecords.assign(request.records.size(), 0u);
            const auto decStartNs = timing::nowNs();
            const auto decStartCycles = timing::readCycles();
            std::size_t compPos = 0u;
            std::size_t blockIdx = 0u;
            for (std::size_t rPos = 0u; rPos < totalRecords; rPos += request.blockRecords, ++blockIdx) {
                const std::size_t rTake = std::min<std::size_t>(request.blockRecords, totalRecords - rPos);
                const std::size_t compLen = scratch.blockEncodedSizes[blockIdx];
                const std::span<const std::uint8_t> compSlice{scratch.encodedBlocks.data() + compPos, compLen};
                const std::span<std::uint8_t> outSlice{scratch.decodedRecords.data() + rPos * stride, rTake * stride};
                compPos += compLen;
                Status st = Status::DecodeError;
                switch (id) {
                    case CorpusLabCodecId::BaselineLz4:
                        st = ext::lz4DecompressBlock(compSlice, outSlice);
                        break;
                    case CorpusLabCodecId::BaselineZstdFast:
                    case CorpusLabCodecId::BaselineZstdMax:
                        st = ext::zstdDecompressWithCtx(scratch.ensureZstdDCtx(), compSlice, outSlice);
                        break;
                    case CorpusLabCodecId::BaselineBrotli:
                        st = ext::brotliDecompressBlock(compSlice, outSlice);
                        break;
                    case CorpusLabCodecId::BaselineXzLzma:
                        st = ext::xzLzmaDecompressBlock(compSlice, outSlice);
                        break;
                    default:
                        break;
                }
                if (!isOk(st)) {
                    entry.status = st;
                    return;
                }
            }
            entry.decodeCycles = timing::readCycles() - decStartCycles;
            entry.decodeNs = timing::nowNs() - decStartNs;
            entry.decodedCrc32c = format::crc32c(scratch.decodedRecords);
            entry.losslessVerified = (entry.decodedCrc32c == rawCrc) &&
                std::equal(request.records.begin(), request.records.end(), scratch.decodedRecords.begin());
            entry.status = entry.losslessVerified ? Status::Ok : Status::VerificationFailed;
        };

        auto benchmarkByteBlockMode = [&](CorpusLabCodecEntry& entry, ByteBlockCodecMode mode) {
            if (!byteBlockCodecModeAvailable(mode)) {
                entry.status = Status::DependencyUnavailable;
                return;
            }
            scratch.encodedBlocks.clear();
            scratch.blockEncodedSizes.clear();
            scratch.blockEncodedSizes.reserve(blockCount);

            std::vector<std::uint8_t> singleBlock(byteBlockEncodeBound(request.layout, request.blockRecords));
            const auto encStartNs = timing::nowNs();
            const auto encStartCycles = timing::readCycles();
            for (std::size_t rPos = 0u; rPos < totalRecords; rPos += request.blockRecords) {
                const std::size_t rTake = std::min<std::size_t>(request.blockRecords, totalRecords - rPos);
                const auto plain = request.records.subspan(rPos * stride, rTake * stride);
                std::size_t written = 0u;
                const auto st = encodeByteBlock(
                    scratch.byteBlockWorkspace, request.layout, plain, mode, singleBlock, written);
                if (!isOk(st) || written == 0u) {
                    entry.status = isOk(st) ? Status::DecodeError : st;
                    return;
                }
                scratch.blockEncodedSizes.push_back(static_cast<std::uint32_t>(written));
                scratch.encodedBlocks.insert(
                    scratch.encodedBlocks.end(), singleBlock.begin(), singleBlock.begin() + static_cast<std::ptrdiff_t>(written));
            }
            entry.encodeCycles = timing::readCycles() - encStartCycles;
            entry.encodeNs = timing::nowNs() - encStartNs;
            entry.compressedBytes = scratch.encodedBlocks.size();

            scratch.decodedRecords.assign(request.records.size(), 0u);
            const auto decStartNs = timing::nowNs();
            const auto decStartCycles = timing::readCycles();
            std::size_t compPos = 0u;
            std::size_t blockIdx = 0u;
            for (std::size_t rPos = 0u; rPos < totalRecords; rPos += request.blockRecords, ++blockIdx) {
                const std::size_t rTake = std::min<std::size_t>(request.blockRecords, totalRecords - rPos);
                const std::size_t compLen = scratch.blockEncodedSizes[blockIdx];
                const std::span<const std::uint8_t> compSlice{scratch.encodedBlocks.data() + compPos, compLen};
                const std::span<std::uint8_t> outSlice{scratch.decodedRecords.data() + rPos * stride, rTake * stride};
                compPos += compLen;
                std::size_t obtained = 0u;
                const auto st = decodeByteBlock(
                    scratch.byteBlockWorkspace, request.layout, compSlice, outSlice, obtained);
                if (!isOk(st) || obtained != outSlice.size()) {
                    entry.status = isOk(st) ? Status::DecodeError : st;
                    return;
                }
            }
            entry.decodeCycles = timing::readCycles() - decStartCycles;
            entry.decodeNs = timing::nowNs() - decStartNs;
            entry.decodedCrc32c = format::crc32c(scratch.decodedRecords);
            entry.losslessVerified = (entry.decodedCrc32c == rawCrc) &&
                std::equal(request.records.begin(), request.records.end(), scratch.decodedRecords.begin());
            entry.status = entry.losslessVerified ? Status::Ok : Status::VerificationFailed;
        };

        auto benchmarkHftMacStream = [&](CorpusLabCodecEntry& entry, CorpusLabCodecId id) {
            if (request.layout.streamType != StreamType::Trades &&
                request.layout.streamType != StreamType::BookTicker &&
                request.layout.streamType != StreamType::Depth) {
                entry.status = Status::UnsupportedStream;
                return;
            }
            scratch.streamBase.clear();
            std::uint64_t outRecords = 0u;
            std::uint64_t baseNs = 0u;
            std::uint64_t baseCycles = 0u;
            std::string err;

            const auto encStartNs = timing::nowNs();
            const auto encStartCycles = timing::readCycles();
            Status st = Status::UnsupportedStream;
            switch (request.layout.streamType) {
                case StreamType::Trades:
                    st = codecs::trades_grouped_delta_qtydict::encodeSliceBytes(
                        request.layout, request.records, scratch.streamBase, outRecords, baseNs, baseCycles, err);
                    break;
                case StreamType::BookTicker:
                    st = codecs::bookticker_delta_mask::encodeSliceBytes(
                        request.layout, request.records, scratch.streamBase, outRecords, baseNs, baseCycles, err);
                    break;
                case StreamType::Depth:
                    st = codecs::depth_ladder_offset::encodeSliceBytes(
                        request.layout, request.records, scratch.streamBase, outRecords, baseNs, baseCycles, err);
                    break;
                default:
                    break;
            }
            if (!isOk(st) || scratch.streamBase.empty()) {
                entry.status = isOk(st) ? Status::DecodeError : st;
                return;
            }

            bool withEntropy = (id != CorpusLabCodecId::HftMacStreamTransform);
            entropy::EntropyKind eKind = (id == CorpusLabCodecId::HftMacStreamRansByteStatic)
                ? entropy::EntropyKind::RansByteStatic
                : entropy::EntropyKind::Ac16Ctx8;
            if (withEntropy) {
                if (!entropy::arithmeticEncodeBounded(
                        scratch.entropyWorkspace,
                        scratch.streamBase,
                        eKind,
                        std::numeric_limits<std::size_t>::max(),
                        scratch.streamEntropy)) {
                    entry.status = Status::DecodeError;
                    return;
                }
            }
            entry.encodeCycles = timing::readCycles() - encStartCycles;
            entry.encodeNs = timing::nowNs() - encStartNs;
            entry.compressedBytes = withEntropy
                ? (entropy::kHeaderBytes + scratch.streamEntropy.size())
                : scratch.streamBase.size();
            entry.blockCount = 1u;

            scratch.decodedRecords.clear();
            scratch.decodedRecords.reserve(kBinarySliceHeaderBytes + request.records.size());
            const auto decStartNs = timing::nowNs();
            const auto decStartCycles = timing::readCycles();

            std::vector<std::uint8_t> decodedBaseBuf;
            std::span<const std::uint8_t> activeBase = scratch.streamBase;
            if (withEntropy) {
                decodedBaseBuf.resize(scratch.streamBase.size());
                if (!entropy::arithmeticDecodeBytes(
                        scratch.entropyWorkspace, scratch.streamEntropy, eKind, decodedBaseBuf)) {
                    entry.status = Status::CorruptData;
                    return;
                }
                activeBase = decodedBaseBuf;
            }

            internal::SpanDecodeSource spanSource(activeBase);
            const DecodedBlockCallback collect = [&](std::span<const std::uint8_t> chunk) {
                scratch.decodedRecords.insert(scratch.decodedRecords.end(), chunk.begin(), chunk.end());
                return true;
            };
            Status decSt = Status::UnsupportedStream;
            switch (request.layout.streamType) {
                case StreamType::Trades:
                    decSt = codecs::trades_grouped_delta_qtydict::decodeSource(spanSource, collect);
                    break;
                case StreamType::BookTicker:
                    decSt = codecs::bookticker_delta_mask::decodeSource(spanSource, collect);
                    break;
                case StreamType::Depth:
                    decSt = codecs::depth_ladder_offset::decodeSource(spanSource, collect);
                    break;
                default:
                    break;
            }
            entry.decodeCycles = timing::readCycles() - decStartCycles;
            entry.decodeNs = timing::nowNs() - decStartNs;
            if (!isOk(decSt)) {
                entry.status = decSt;
                return;
            }
            BinarySliceView view{};
            const auto parseSt = parseBinarySlice(scratch.decodedRecords, view);
            if (!isOk(parseSt)) {
                entry.status = parseSt;
                return;
            }
            entry.decodedCrc32c = view.recordsCrc32c;
            entry.losslessVerified = (view.records.size() == request.records.size()) &&
                (view.recordsCrc32c == rawCrc) &&
                std::equal(request.records.begin(), request.records.end(), view.records.begin());
            entry.status = entry.losslessVerified ? Status::Ok : Status::VerificationFailed;
        };

        for (std::size_t idx = 0u; idx < kLabDescriptors.size(); ++idx) {
            const auto& desc = kLabDescriptors[idx];
            if ((mask & corpusLabCodecBit(desc.codecId)) == 0u) continue;
            auto& entry = report.entries[report.entryCount++];
            entry.codecId = desc.codecId;
            entry.streamFamily = request.layout.streamType;
            entry.byteBlockMode = desc.byteBlockMode;
            entry.isBaseline = desc.isBaseline;
            entry.isOnlineTier = desc.isOnlineTier;
            entry.isByteBlockCodec = desc.isByteBlockCodec;
            entry.blockRecords = request.blockRecords;
            entry.blockBytes = static_cast<std::uint32_t>(maxBlockBytes);
            entry.rawCrc32c = rawCrc;
            entry.recordCount = totalRecords;
            entry.blockCount = blockCount;
            entry.rawBytes = request.records.size();

            if (desc.isBaseline) {
                benchmarkRawBaseline(entry, desc.codecId);
            } else if (desc.isByteBlockCodec) {
                benchmarkByteBlockMode(entry, desc.byteBlockMode);
            } else {
                benchmarkHftMacStream(entry, desc.codecId);
            }
            populateColdSummaryRates(entry);
            if (entry.losslessVerified) ++report.verifiedCount;
        }

        // Check mandatory baseline families: lz4, zstd, brotli, xz/lzma.
        bool haveLz4 = false, haveZstd = false, haveBrotli = false, haveXz = false;
        std::uint64_t bestBaselineBytes = std::numeric_limits<std::uint64_t>::max();
        for (std::uint16_t i = 0u; i < report.entryCount; ++i) {
            const auto& e = report.entries[i];
            if (!e.isBaseline || !e.losslessVerified) continue;
            bestBaselineBytes = std::min(bestBaselineBytes, e.compressedBytes);
            if (e.codecId == CorpusLabCodecId::BaselineLz4) haveLz4 = true;
            if (e.codecId == CorpusLabCodecId::BaselineZstdFast ||
                e.codecId == CorpusLabCodecId::BaselineZstdMax) haveZstd = true;
            if (e.codecId == CorpusLabCodecId::BaselineBrotli) haveBrotli = true;
            if (e.codecId == CorpusLabCodecId::BaselineXzLzma) haveXz = true;
        }
        report.mandatoryBaselinesComplete = haveLz4 && haveZstd && haveBrotli && haveXz;

        // Rank verified codecs within this stream family without reordering entries[]
        // (preserving deterministic index stability while assigning 1-based ratioRank).
        std::array<std::uint16_t, kCorpusLabMaxCodecs> order{};
        std::size_t rankedCount = 0u;
        for (std::uint16_t i = 0u; i < report.entryCount; ++i) {
            if (report.entries[i].losslessVerified) {
                order[rankedCount++] = i;
            }
        }
        std::sort(order.begin(), order.begin() + rankedCount, [&](std::uint16_t lhs, std::uint16_t rhs) {
            return betterRatioCrossProduct(report.entries[lhs], report.entries[rhs]);
        });

        bool haveOnlineBest = false;
        std::size_t bestOnlineIdx = 0u;
        for (std::size_t r = 0u; r < rankedCount; ++r) {
            auto& e = report.entries[order[r]];
            e.ratioRank = static_cast<std::uint16_t>(r + 1u);
            if (!e.isBaseline && report.mandatoryBaselinesComplete && e.compressedBytes < bestBaselineBytes) {
                e.beatsAllBaselinesInRatio = true;
            }
            if (r == 0u) report.bestRatioCodec = e.codecId;
            if (e.isOnlineTier && e.isByteBlockCodec) {
                if (!haveOnlineBest || e.encodeNs < report.entries[bestOnlineIdx].encodeNs) {
                    haveOnlineBest = true;
                    bestOnlineIdx = order[r];
                    report.bestOnlineCodec = e.codecId;
                }
            }
        }
        return Status::Ok;
    } catch (...) {
        report.status = Status::DecodeError;
        return Status::DecodeError;
    }
}

Status runCorpusLab(
    const CorpusLabRequest& request,
    CorpusLabReport& report) noexcept {
    CorpusLabWorkspace workspace;
    const auto st = workspace.prepare();
    if (!isOk(st)) {
        report = {};
        report.status = st;
        return st;
    }
    return runCorpusLab(workspace, request, report);
}

}  // namespace hft_compressor

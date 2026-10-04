#include "hft_compressor/Compressor.hpp"

#include <array>
#include <atomic>
#include <fcntl.h>
#include <fstream>
#include <sys/stat.h>
#include <unistd.h>
#include <string_view>

#include "../../Runtime/src/Common/CompressionInternals.hpp"
#include "../../Runtime/src/Pipelines/PipelineBackend.hpp"

namespace hft_compressor {
namespace {

// Only compressed artifacts/metrics are staged, never a decoded-base spool.
// The final artifact stays untouched until the backend's real roundtrip admits
// the result. Exclusive names ensure failure cleanup owns both staged files.
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
    if (stream == StreamType::Unknown)
        return internal::fail(Status::UnsupportedStream, request, &pipeline, "input stream is unknown");
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
        result.outputPath.clear(); result.metricsPath.clear();
        return result;
    }
    if (result.outputPath != pending.artifact || result.metricsPath != pending.metrics || !pending.ownsFiles()) {
        result.status = Status::IoError; result.error = "verified staged artifact identity changed";
        result.outputPath.clear(); result.metricsPath.clear(); return result;
    }
    result.outputPath = finalPath;
    result.metricsPath = parent / (finalPath.stem().string() + ".metrics.json");
    const auto text = toMetricsJson(result);
    std::ofstream metadata(pending.metrics, std::ios::binary | std::ios::trunc);
    metadata.write(text.data(), static_cast<std::streamsize>(text.size()));
    metadata.close();
    if (!metadata) {
        result.status = Status::IoError; result.error = "failed to finish artifact metrics";
        result.outputPath.clear(); result.metricsPath.clear(); return result;
    }
    if (!pending.ownsFiles()) {
        result.status = Status::IoError; result.error = "staged artifact identity changed before publication";
        result.outputPath.clear(); result.metricsPath.clear(); return result;
    }
    std::filesystem::rename(pending.artifact, finalPath, error);
    if (error) {
        result.status = Status::IoError; result.error = "failed to publish verified artifact";
        result.outputPath.clear(); result.metricsPath.clear(); return result;
    }
    // A metrics rename error never turns partial decoded data into an artifact:
    // the already-published compressed artifact completed its full roundtrip.
    std::filesystem::rename(pending.metrics, result.metricsPath, error);
    if (error) { result.status = Status::IoError; result.error = "verified artifact published; metrics publication failed"; }
    return result;
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
        auto result = internal::fail(Status::UnsupportedPipeline, request, nullptr, "unknown pipeline id");
        return result;
    }
    if (pipeline->availability == PipelineAvailability::DependencyUnavailable) {
        auto result = internal::fail(Status::DependencyUnavailable, request, pipeline, std::string{pipeline->availabilityReason});
        return result;
    }
    if (pipeline->availability == PipelineAvailability::NotImplemented) {
        auto result = internal::fail(Status::NotImplemented, request, pipeline, std::string{pipeline->availabilityReason});
        return result;
    }
    if (const auto* backend = pipelines::findBackend(pipeline->id); backend != nullptr && backend->compress != nullptr) {
        return compressToPublishedArtifact(request, *pipeline, *backend);
    }

    auto result = internal::fail(Status::UnsupportedPipeline, request, pipeline, "pipeline has no compressor implementation");
    return result;
    } catch (...) { CompressionResult failed{}; failed.status = Status::DecodeError; return failed; }
}

Status decodeHfcBuffer(std::span<const std::uint8_t> compressedFile,
                    const DecodedBlockCallback& onBlock) noexcept {
    const auto* backend = pipelines::findBackend("std.zstd_jsonl_blocks_v1");
    return backend != nullptr && backend->decodeBuffer != nullptr ? backend->decodeBuffer(compressedFile, onBlock) : Status::NotImplemented;
}

Status decodeHfcFile(const std::filesystem::path& path,
                    const DecodedBlockCallback& onBlock) noexcept {
    const auto* backend = pipelines::findBackend("std.zstd_jsonl_blocks_v1");
    return backend != nullptr && backend->decodeJsonl != nullptr ? backend->decodeJsonl(path, onBlock) : Status::NotImplemented;
}

}  // namespace hft_compressor

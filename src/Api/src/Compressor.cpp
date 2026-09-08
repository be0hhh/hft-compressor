#include "hft_compressor/Compressor.hpp"

#include <array>
#include <string_view>

#include "../../Runtime/src/Common/CompressionInternals.hpp"
#include "hft_compressor/Metrics.hpp"
#include "../../Runtime/src/Pipelines/PipelineBackend.hpp"

namespace hft_compressor {
std::filesystem::path defaultOutputRoot() {
    const auto cwd = std::filesystem::current_path();
    if (cwd.filename() == "hft-recorder") return cwd.parent_path() / "hft-compressor" / "compressedData";
    if (cwd.filename() == "hft-compressor") return cwd / "compressedData";
    return cwd / "compressedData";
}

CompressionResult compress(const CompressionRequest& request) noexcept {
    const auto* pipeline = findPipeline(request.pipelineId);
    if (pipeline == nullptr) {
        auto result = internal::fail(Status::UnsupportedPipeline, request, nullptr, "unknown pipeline id");
        metrics::recordRun(result);
        return result;
    }
    if (pipeline->availability == PipelineAvailability::DependencyUnavailable) {
        auto result = internal::fail(Status::DependencyUnavailable, request, pipeline, std::string{pipeline->availabilityReason});
        metrics::recordRun(result);
        return result;
    }
    if (pipeline->availability == PipelineAvailability::NotImplemented) {
        auto result = internal::fail(Status::NotImplemented, request, pipeline, std::string{pipeline->availabilityReason});
        metrics::recordRun(result);
        return result;
    }
    if (const auto* backend = pipelines::findBackend(pipeline->id); backend != nullptr && backend->compress != nullptr) {
        return backend->compress(request, *pipeline);
    }

    auto result = internal::fail(Status::UnsupportedPipeline, request, pipeline, "pipeline has no compressor implementation");
    metrics::recordRun(result);
    return result;
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



#include "common/CompressionInternals.hpp"

#include <algorithm>
#include <fstream>
#include <system_error>
#include <utility>

namespace hft_compressor::internal {

void applyPipeline(CompressionResult& result, const PipelineDescriptor* pipeline) {
    if (pipeline == nullptr) return;
    result.pipelineId = std::string{pipeline->id};
    result.representation = std::string{pipeline->representation};
    result.transform = std::string{pipeline->transform};
    result.entropy = std::string{pipeline->entropy};
    result.profile = std::string{pipeline->profile};
    result.implementationKind = std::string{pipeline->implementationKind};
}

CompressionResult fail(Status status, const CompressionRequest& request, const PipelineDescriptor* pipeline, std::string error) {
    CompressionResult result{};
    result.status = status;
    result.error = std::move(error);
    result.inputPath = request.inputPath;
    result.streamType = inferStreamTypeFromPath(request.inputPath);
    applyPipeline(result, pipeline);
    return result;
}

std::uint64_t countLines(std::span<const std::uint8_t> data, bool isLastBlock) noexcept {
    std::uint64_t count = 0;
    for (const auto c : data) if (c == static_cast<std::uint8_t>('\n')) ++count;
    if (isLastBlock && !data.empty() && data.back() != static_cast<std::uint8_t>('\n')) ++count;
    return count;
}

std::string sessionIdForInput(const std::filesystem::path& inputPath) {
    const auto parent = inputPath.parent_path();
    if (!parent.empty()) return parent.filename().string();
    return std::string{"manual_"} + inputPath.stem().string();
}

bool readFileBytes(const std::filesystem::path& path, std::vector<std::uint8_t>& out) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return false;
    in.seekg(0, std::ios::end);
    const auto size = in.tellg();
    if (size < 0) return false;
    in.seekg(0, std::ios::beg);
    out.resize(static_cast<std::size_t>(size));
    if (!out.empty()) in.read(reinterpret_cast<char*>(out.data()), static_cast<std::streamsize>(out.size()));
    return static_cast<bool>(in) || in.eof();
}

bool writeTextFile(const std::filesystem::path& path, const std::string& text) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) return false;
    out.write(text.data(), static_cast<std::streamsize>(text.size()));
    return static_cast<bool>(out);
}

std::filesystem::path outputPathFor(const CompressionRequest& request, const PipelineDescriptor& pipeline, StreamType streamType) {
    if (!request.outputPathOverride.empty()) return request.outputPathOverride;
    const auto root = request.outputRoot.empty() ? defaultOutputRoot() : request.outputRoot;
    const std::string extension = pipeline.fileExtension.empty() ? ".hfc" : std::string{pipeline.fileExtension};
    return root / std::string{pipeline.outputSlug} / "sessions" / sessionIdForInput(request.inputPath)
        / (std::string{streamTypeChannelName(streamType)} + extension);
}

}  // namespace hft_compressor::internal

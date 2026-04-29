#pragma once

#include <filesystem>
#include <span>
#include <string_view>

#include "hft_compressor/compressor.hpp"
#include "hft_compressor/pipeline.hpp"

namespace hft_compressor::pipelines {

struct PipelineBackend {
    std::string_view pipelineId{};
    std::string_view formatId{};
    CompressionResult (*compress)(const CompressionRequest& request, const PipelineDescriptor& pipeline) noexcept {nullptr};
    ReplayArtifactInfo (*inspectArtifact)(const std::filesystem::path& path, const PipelineDescriptor& pipeline) noexcept {nullptr};
    Status (*decodeJsonl)(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept {nullptr};
    Status (*decodeBuffer)(std::span<const std::uint8_t> bytes, const DecodedBlockCallback& onBlock) noexcept {nullptr};
};

const PipelineBackend* findBackend(std::string_view pipelineId) noexcept;

}  // namespace hft_compressor::pipelines

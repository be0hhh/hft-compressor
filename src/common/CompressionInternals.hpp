#pragma once

#include <filesystem>
#include <span>
#include <cstdint>
#include <string>
#include <vector>

#include "hft_compressor/compressor.hpp"
#include "hft_compressor/pipeline.hpp"

namespace hft_compressor::internal {

void applyPipeline(CompressionResult& result, const PipelineDescriptor* pipeline);
CompressionResult fail(Status status, const CompressionRequest& request, const PipelineDescriptor* pipeline, std::string error);
std::uint64_t countLines(std::span<const std::uint8_t> data, bool isLastBlock) noexcept;
bool readFileBytes(const std::filesystem::path& path, std::vector<std::uint8_t>& out);
bool writeTextFile(const std::filesystem::path& path, const std::string& text);
std::filesystem::path outputPathFor(const CompressionRequest& request, const PipelineDescriptor& pipeline, StreamType streamType);

}  // namespace hft_compressor::internal

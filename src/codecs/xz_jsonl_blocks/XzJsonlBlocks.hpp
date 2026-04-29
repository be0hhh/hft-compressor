#pragma once

#include <cstdint>
#include <filesystem>
#include <span>

#include "hft_compressor/compressor.hpp"
#include "hft_compressor/pipeline.hpp"

namespace hft_compressor::codecs::xz_jsonl_blocks {

CompressionResult compress(const CompressionRequest& request, const PipelineDescriptor& pipeline) noexcept;
ReplayArtifactInfo inspectArtifact(const std::filesystem::path& path, const PipelineDescriptor& pipeline) noexcept;
Status decode(std::span<const std::uint8_t> compressedFile, const DecodedBlockCallback& onBlock) noexcept;
Status decodeFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept;

}  // namespace hft_compressor::codecs::xz_jsonl_blocks

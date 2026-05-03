#pragma once

#include <filesystem>
#include <span>

#include "hft_compressor/compressor.hpp"
#include "hft_compressor/pipeline.hpp"

namespace hft_compressor::codecs::depth_ladder_offset_v2 {

CompressionResult compress(const CompressionRequest& request, const PipelineDescriptor& pipeline) noexcept;
ReplayArtifactInfo inspectArtifact(const std::filesystem::path& path, const PipelineDescriptor& pipeline) noexcept;
Status decodeFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept;
Status decode(std::span<const std::uint8_t> bytes, const DecodedBlockCallback& onBlock) noexcept;
Status inspectEncodedJsonFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept;
Status inspectEncodedBinaryFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept;
Status inspectStatsJsonFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept;

}  // namespace hft_compressor::codecs::depth_ladder_offset_v2
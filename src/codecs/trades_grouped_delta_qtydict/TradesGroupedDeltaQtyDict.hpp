#pragma once

#include <filesystem>
#include <span>

#include "hft_compressor/compressor.hpp"
#include "hft_compressor/pipeline.hpp"
#include "hft_compressor/status.hpp"

namespace hft_compressor::codecs::trades_grouped_delta_qtydict {

CompressionResult compress(const CompressionRequest& request, const PipelineDescriptor& pipeline) noexcept;
ReplayArtifactInfo inspectArtifact(const std::filesystem::path& path, const PipelineDescriptor& pipeline) noexcept;
Status decode(std::span<const std::uint8_t> file, const DecodedBlockCallback& onBlock) noexcept;
Status decodeFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept;

Status inspectEncodedJsonFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept;
Status inspectEncodedBinaryFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept;
Status inspectStatsJsonFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept;

}  // namespace hft_compressor::codecs::trades_grouped_delta_qtydict

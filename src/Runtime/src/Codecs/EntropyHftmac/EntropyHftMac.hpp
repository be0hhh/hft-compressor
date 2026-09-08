#pragma once

#include <filesystem>
#include <span>

#include "hft_compressor/Compressor.hpp"
#include "hft_compressor/Pipeline.hpp"
#include "hft_compressor/Status.hpp"

namespace hft_compressor::codecs::entropy_hftmac {

CompressionResult compress(const CompressionRequest& request, const PipelineDescriptor& pipeline) noexcept;
ReplayArtifactInfo inspectArtifact(const std::filesystem::path& path, const PipelineDescriptor& pipeline) noexcept;
Status decode(std::span<const std::uint8_t> file, const DecodedBlockCallback& onBlock) noexcept;
Status decodeFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept;

Status inspectEncodedJsonFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept;
Status inspectEncodedBinaryFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept;
Status inspectStatsJsonFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept;

}  // namespace hft_compressor::codecs::entropy_hftmac

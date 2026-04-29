#pragma once

#include <filesystem>
#include <span>

#include "hft_compressor/compressor.hpp"
#include "hft_compressor/pipeline.hpp"

namespace hft_compressor::codecs::hftmac_varint {

CompressionResult compressTradeVarint(const CompressionRequest& request, const PipelineDescriptor& pipeline) noexcept;
CompressionResult compressBookTickerVarint(const CompressionRequest& request, const PipelineDescriptor& pipeline) noexcept;
CompressionResult compressDepthVarint(const CompressionRequest& request, const PipelineDescriptor& pipeline) noexcept;

ReplayArtifactInfo inspectTradeVarint(const std::filesystem::path& path, const PipelineDescriptor& pipeline) noexcept;
ReplayArtifactInfo inspectBookTickerVarint(const std::filesystem::path& path, const PipelineDescriptor& pipeline) noexcept;
ReplayArtifactInfo inspectDepthVarint(const std::filesystem::path& path, const PipelineDescriptor& pipeline) noexcept;

Status decodeTradeVarintFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept;
Status decodeBookTickerVarintFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept;
Status decodeDepthVarintFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept;

Status decodeTradeVarint(std::span<const std::uint8_t> bytes, const DecodedBlockCallback& onBlock) noexcept;
Status decodeBookTickerVarint(std::span<const std::uint8_t> bytes, const DecodedBlockCallback& onBlock) noexcept;
Status decodeDepthVarint(std::span<const std::uint8_t> bytes, const DecodedBlockCallback& onBlock) noexcept;

}  // namespace hft_compressor::codecs::hftmac_varint

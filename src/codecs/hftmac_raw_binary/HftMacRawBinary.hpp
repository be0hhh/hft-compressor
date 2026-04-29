#pragma once

#include <filesystem>
#include <span>

#include "hft_compressor/compressor.hpp"
#include "hft_compressor/pipeline.hpp"

namespace hft_compressor::codecs::hftmac_raw_binary {

CompressionResult compressTradeRawBinary(const CompressionRequest& request, const PipelineDescriptor& pipeline) noexcept;
CompressionResult compressBookTickerRawBinary(const CompressionRequest& request, const PipelineDescriptor& pipeline) noexcept;
CompressionResult compressDepthRawBinary(const CompressionRequest& request, const PipelineDescriptor& pipeline) noexcept;

ReplayArtifactInfo inspectTradeRawBinary(const std::filesystem::path& path, const PipelineDescriptor& pipeline) noexcept;
ReplayArtifactInfo inspectBookTickerRawBinary(const std::filesystem::path& path, const PipelineDescriptor& pipeline) noexcept;
ReplayArtifactInfo inspectDepthRawBinary(const std::filesystem::path& path, const PipelineDescriptor& pipeline) noexcept;

Status decodeTradeRawBinaryFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept;
Status decodeBookTickerRawBinaryFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept;
Status decodeDepthRawBinaryFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept;

Status decodeTradeRawBinary(std::span<const std::uint8_t> bytes, const DecodedBlockCallback& onBlock) noexcept;
Status decodeBookTickerRawBinary(std::span<const std::uint8_t> bytes, const DecodedBlockCallback& onBlock) noexcept;
Status decodeDepthRawBinary(std::span<const std::uint8_t> bytes, const DecodedBlockCallback& onBlock) noexcept;

}  // namespace hft_compressor::codecs::hftmac_raw_binary

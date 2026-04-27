#pragma once

#include <span>
#include <cstdint>

#include "hft_compressor/compressor.hpp"
#include "hft_compressor/pipeline.hpp"

namespace hft_compressor::codecs::zstd_jsonl_blocks {

CompressionResult compress(const CompressionRequest& request, const PipelineDescriptor& pipeline) noexcept;
Status decode(std::span<const std::uint8_t> compressedFile, const DecodedBlockCallback& onBlock) noexcept;

}  // namespace hft_compressor::codecs::zstd_jsonl_blocks

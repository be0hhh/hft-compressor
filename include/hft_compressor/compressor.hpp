#pragma once

#include <cstdint>
#include <filesystem>
#include <functional>
#include <span>
#include <string>

#include "hft_compressor/api.hpp"
#include "hft_compressor/pipeline.hpp"
#include "hft_compressor/result.hpp"

namespace hft_compressor {

struct CompressionRequest {
    std::filesystem::path inputPath{};
    std::filesystem::path outputRoot{};
    std::filesystem::path outputPathOverride{};
    std::string pipelineId{};
    std::uint32_t blockBytes{1024u * 1024u};
    int zstdLevel{3};
};

using DecodedBlockCallback = std::function<bool(std::span<const std::uint8_t> block)>;

HFT_COMPRESSOR_API std::filesystem::path defaultOutputRoot();
HFT_COMPRESSOR_API CompressionResult compress(const CompressionRequest& request) noexcept;
HFT_COMPRESSOR_API Status decodeHfcBuffer(std::span<const std::uint8_t> compressedFile,
                    const DecodedBlockCallback& onBlock) noexcept;

}  // namespace hft_compressor

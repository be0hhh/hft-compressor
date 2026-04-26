#pragma once

#include <cstdint>
#include <filesystem>
#include <string>

#include "hft_compressor/api.hpp"
#include "hft_compressor/status.hpp"
#include "hft_compressor/stream_type.hpp"

namespace hft_compressor {

struct CompressionResult {
    Status status{Status::Ok};
    std::string error{};
    std::string codec{"zstd_jsonl_blocks_v1"};
    StreamType streamType{StreamType::Unknown};
    std::filesystem::path inputPath{};
    std::filesystem::path outputPath{};
    std::filesystem::path metricsPath{};
    std::uint64_t inputBytes{0};
    std::uint64_t outputBytes{0};
    std::uint64_t lineCount{0};
    std::uint64_t blockCount{0};
    std::uint64_t encodeNs{0};
    std::uint64_t decodeNs{0};
    std::uint64_t encodeCycles{0};
    std::uint64_t decodeCycles{0};
    bool roundtripOk{false};
};

HFT_COMPRESSOR_API double ratio(const CompressionResult& result) noexcept;
HFT_COMPRESSOR_API double encodeMbPerSec(const CompressionResult& result) noexcept;
HFT_COMPRESSOR_API double decodeMbPerSec(const CompressionResult& result) noexcept;
HFT_COMPRESSOR_API std::string toMetricsJson(const CompressionResult& result);

}  // namespace hft_compressor

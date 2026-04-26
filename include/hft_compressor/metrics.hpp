#pragma once

#include <string>

#include "hft_compressor/api.hpp"
#include "hft_compressor/result.hpp"

namespace hft_compressor::metrics {

HFT_COMPRESSOR_API void recordRun(const CompressionResult& result) noexcept;
HFT_COMPRESSOR_API CompressionResult lastRun() noexcept;
HFT_COMPRESSOR_API void renderPrometheus(std::string& out) noexcept;

}  // namespace hft_compressor::metrics

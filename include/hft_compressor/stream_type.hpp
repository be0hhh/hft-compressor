#pragma once

#include <filesystem>
#include <string_view>

#include "hft_compressor/api.hpp"

namespace hft_compressor {

enum class StreamType {
    Unknown,
    Trades,
    BookTicker,
    Depth,
};

HFT_COMPRESSOR_API StreamType inferStreamTypeFromPath(const std::filesystem::path& path) noexcept;
HFT_COMPRESSOR_API std::string_view streamTypeToString(StreamType type) noexcept;
HFT_COMPRESSOR_API std::string_view streamTypeChannelName(StreamType type) noexcept;

}  // namespace hft_compressor


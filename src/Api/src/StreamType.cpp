#include "hft_compressor/StreamType.hpp"

#include <string_view>

namespace hft_compressor {

StreamType inferStreamTypeFromPath(const std::filesystem::path& path) noexcept {
    // Classification is noexcept and precedes backend allocation handling.
    // Read the existing native path storage; filename()/string() can allocate.
    const auto& native = path.native();
    auto separator = native.find_last_of('/');
    if constexpr (std::filesystem::path::preferred_separator != '/') {
        const auto preferred = native.find_last_of(std::filesystem::path::preferred_separator);
        if (preferred != native.npos && (separator == native.npos || preferred > separator)) separator = preferred;
    }
    const auto begin = separator == native.npos ? 0u : separator + 1u;
    const auto matches = [&](std::string_view expected) noexcept {
        if (native.size() - begin != expected.size()) return false;
        for (std::size_t i = 0; i < expected.size(); ++i) {
            const auto value = native[begin + i];
            const auto lower = value >= 'A' && value <= 'Z' ? value + ('a' - 'A') : value;
            if (lower != expected[i]) return false;
        }
        return true;
    };
    if (matches("trades.jsonl")) return StreamType::Trades;
    if (matches("bookticker.jsonl")) return StreamType::BookTicker;
    if (matches("depth.jsonl")) return StreamType::Depth;
    return StreamType::Unknown;
}

std::string_view streamTypeToString(StreamType type) noexcept {
    switch (type) {
        case StreamType::Trades: return "trades";
        case StreamType::BookTicker: return "bookticker";
        case StreamType::Depth: return "depth";
        case StreamType::Unknown: return "unknown";
    }
    return "unknown";
}

std::string_view streamTypeChannelName(StreamType type) noexcept {
    return streamTypeToString(type);
}

}  // namespace hft_compressor

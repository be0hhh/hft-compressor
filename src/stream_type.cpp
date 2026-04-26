#include "hft_compressor/stream_type.hpp"

#include <algorithm>
#include <cctype>
#include <string>

namespace hft_compressor {

StreamType inferStreamTypeFromPath(const std::filesystem::path& path) noexcept {
    auto name = path.filename().string();
    std::transform(name.begin(), name.end(), name.begin(), [](unsigned char c) {
        return static_cast<char>(std::tolower(c));
    });
    if (name == "trades.jsonl") return StreamType::Trades;
    if (name == "bookticker.jsonl") return StreamType::BookTicker;
    if (name == "depth.jsonl") return StreamType::Depth;
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

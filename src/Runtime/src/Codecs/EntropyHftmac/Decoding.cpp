#include "EntropyHftMac.hpp"
#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <span>
#include <sstream>
#include <string_view>
#include <system_error>
#include <vector>
#include "../BooktickerDeltaMask/BookTickerDeltaMask.hpp"
#include "../DepthLadderOffset/DepthLadderOffset.hpp"
#include "../TradesGroupedDeltaQtydict/TradesGroupedDeltaQtyDict.hpp"
#include "../../Common/CompressionInternals.hpp"
#include "../../Common/Timing.hpp"
#include "../../Container/Hfc/Format.hpp"
#include "EntropyHftMacInternal.hpp"

namespace hft_compressor::codecs::entropy_hftmac::detail {

Status decodeBase(BaseKind base, std::span<const std::uint8_t> bytes, const DecodedBlockCallback& onBlock) noexcept {
    switch (base) {
        case BaseKind::Trades: return trades_grouped_delta_qtydict::decode(bytes, onBlock);
        case BaseKind::BookTicker: return bookticker_delta_mask::decode(bytes, onBlock);
        case BaseKind::Depth: return depth_ladder_offset::decode(bytes, onBlock);
    }
    return Status::CorruptData;
}

Status decodePayload(std::span<const std::uint8_t> file, Header& header, std::vector<std::uint8_t>& baseBytes) noexcept {
    if (file.size() < kHeaderBytes) return Status::InvalidArgument;
    if (!parseHeader(file.data(), file.size(), header) || !validHeader(header)) return Status::CorruptData;
    if (file.size() != header.outputBytes) return Status::CorruptData;
    std::span<const std::uint8_t> payload{file.data() + kHeaderBytes, static_cast<std::size_t>(header.payloadBytes)};
    if (format::crc32c(payload) != header.payloadCrc32c) return Status::CorruptData;
    const auto status = arithmeticDecode(payload, static_cast<EntropyKind>(header.entropy), header.baseBytes, baseBytes);
    if (!isOk(status)) return status;
    if (baseBytes.size() != header.baseBytes || format::crc32c(baseBytes) != header.decodedCrc32c) return Status::CorruptData;
    return Status::Ok;
}

Status readFile(const std::filesystem::path& path, std::vector<std::uint8_t>& out) noexcept {
    return internal::readFileBytes(path, out) ? Status::Ok : Status::IoError;
}

}

namespace hft_compressor::codecs::entropy_hftmac {

using namespace detail;

Status decode(std::span<const std::uint8_t> file, const DecodedBlockCallback& onBlock) noexcept {
    if (!onBlock) return Status::InvalidArgument;
    Header header{};
    std::vector<std::uint8_t> baseBytes;
    const auto status = decodePayload(file, header, baseBytes);
    if (!isOk(status)) return status;
    return decodeBase(static_cast<BaseKind>(header.base), baseBytes, onBlock);
}

Status decodeFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept {
    if (path.empty() || !onBlock) return Status::InvalidArgument;
    std::vector<std::uint8_t> bytes;
    const auto readStatus = readFile(path, bytes);
    if (!isOk(readStatus)) return readStatus;
    return decode(bytes, onBlock);
}

}

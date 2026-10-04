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

namespace {

template <typename T>
void writeLe(std::span<std::uint8_t>& out, T value) {
    for (std::size_t i = 0; i < sizeof(T); ++i) {
        out[i] = static_cast<std::uint8_t>((static_cast<std::uint64_t>(value) >> (i * 8u)) & 0xffu);
    }
    out = out.subspan(sizeof(T));
}

template <typename T>
bool readLe(const std::uint8_t*& p, const std::uint8_t* end, T& out) noexcept {
    if (static_cast<std::size_t>(end - p) < sizeof(T)) return false;
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < sizeof(T); ++i) value |= static_cast<std::uint64_t>(p[i]) << (i * 8u);
    p += sizeof(T);
    out = static_cast<T>(value);
    return true;
}

}

namespace {
std::array<std::uint8_t, kHeaderBytes> fixedHeader(Header header, bool includeCrc) noexcept {
    if (!includeCrc) header.headerCrc32c = 0u;
    std::array<std::uint8_t, kHeaderBytes> bytes{};
    std::span<std::uint8_t> out{bytes};
    writeLe(out, header.magic);
    writeLe(out, header.version);
    writeLe(out, header.entropy);
    writeLe(out, header.base);
    writeLe(out, header.stream);
    writeLe(out, header.headerCrc32c);
    writeLe(out, header.inputBytes);
    writeLe(out, header.baseBytes);
    writeLe(out, header.outputBytes);
    writeLe(out, header.lineCount);
    writeLe(out, header.payloadBytes);
    writeLe(out, header.payloadCrc32c);
    writeLe(out, header.decodedCrc32c);
    return bytes;
}
}

std::vector<std::uint8_t> serializeHeader(Header header, bool includeCrc) {
    const auto bytes = fixedHeader(header, includeCrc);
    return {bytes.begin(), bytes.end()};
}

std::uint32_t headerCrc32c(const Header& header) noexcept {
    return format::crc32c(fixedHeader(header, false));
}

bool parseHeader(const std::uint8_t* data, std::size_t size, Header& out) noexcept {
    if (data == nullptr || size < kHeaderBytes) return false;
    const auto* p = data;
    const auto* end = data + kHeaderBytes;
    return readLe(p, end, out.magic)
        && readLe(p, end, out.version)
        && readLe(p, end, out.entropy)
        && readLe(p, end, out.base)
        && readLe(p, end, out.stream)
        && readLe(p, end, out.headerCrc32c)
        && readLe(p, end, out.inputBytes)
        && readLe(p, end, out.baseBytes)
        && readLe(p, end, out.outputBytes)
        && readLe(p, end, out.lineCount)
        && readLe(p, end, out.payloadBytes)
        && readLe(p, end, out.payloadCrc32c)
        && readLe(p, end, out.decodedCrc32c)
        && std::all_of(p, end, [](auto byte) { return byte == 0u; });
}

bool validHeader(const Header& header) noexcept {
    return header.magic == kMagic
        && header.version == kVersion
        && header.base >= static_cast<std::uint16_t>(BaseKind::Trades)
        && header.base <= static_cast<std::uint16_t>(BaseKind::Depth)
        && header.entropy >= static_cast<std::uint16_t>(EntropyKind::Ac16Ctx0)
        && header.entropy <= static_cast<std::uint16_t>(EntropyKind::RansByteStatic)
        && format::streamFromWire(header.stream) == (header.base == 1u ? StreamType::Trades : header.base == 2u ? StreamType::BookTicker : StreamType::Depth)
        && header.baseBytes != 0u && header.payloadBytes != 0u
        && header.payloadBytes <= std::numeric_limits<std::uint64_t>::max() - kHeaderBytes
        && header.headerCrc32c == headerCrc32c(header)
        && header.outputBytes == kHeaderBytes + header.payloadBytes;
}

EntropyKind entropyKindFor(std::string_view id) noexcept {
    if (id.find("ac16_ctx0") != std::string_view::npos) return EntropyKind::Ac16Ctx0;
    if (id.find("ac16_ctx8") != std::string_view::npos) return EntropyKind::Ac16Ctx8;
    if (id.find("ac16_ctx12") != std::string_view::npos) return EntropyKind::Ac16Ctx12;
    if (id.find("ac32_ctx8") != std::string_view::npos) return EntropyKind::Ac32Ctx8;
    if (id.find("range_byte_ctx8") != std::string_view::npos) return EntropyKind::RangeByteCtx8;
    return EntropyKind::RansByteStatic;
}

BaseKind baseKindFor(StreamType streamType) noexcept {
    if (streamType == StreamType::Trades) return BaseKind::Trades;
    if (streamType == StreamType::BookTicker) return BaseKind::BookTicker;
    return BaseKind::Depth;
}

std::string_view basePipelineId(BaseKind base) noexcept {
    switch (base) {
        case BaseKind::Trades: return "hftmac.trades_grouped_delta_qtydict_math_v3";
        case BaseKind::BookTicker: return "hftmac.bookticker_delta_mask_v2";
        case BaseKind::Depth: return "hftmac.depth_ladder_offset_v3";
    }
    return {};
}

std::string_view formatIdFor(BaseKind base, EntropyKind kind) noexcept {
    (void)kind;
    switch (base) {
        case BaseKind::Trades: return "hftmac.trades_grouped_delta_qtydict.entropy.v1";
        case BaseKind::BookTicker: return "hftmac.bookticker_delta_mask.entropy.v1";
        case BaseKind::Depth: return "hftmac.depth_ladder_offset.entropy.v1";
    }
    return "hftmac.entropy.v1";
}

std::string_view entropyName(EntropyKind kind) noexcept {
    switch (kind) {
        case EntropyKind::Ac16Ctx0: return "ac16_ctx0";
        case EntropyKind::Ac16Ctx8: return "ac16_ctx8";
        case EntropyKind::Ac16Ctx12: return "ac16_ctx12";
        case EntropyKind::Ac32Ctx8: return "ac32_ctx8";
        case EntropyKind::RangeByteCtx8: return "range_byte_ctx8";
        case EntropyKind::RansByteStatic: return "rans_byte_static";
    }
    return "unknown";
}

}

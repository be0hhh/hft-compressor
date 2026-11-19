#pragma once

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
#include "../../Common/DecodeSource.hpp"
#include "../../Container/Hfc/Format.hpp"

namespace hft_compressor::codecs::entropy_hftmac::detail {

constexpr std::uint32_t kMagic = 0x00464845u; // EHF
constexpr std::size_t kHeaderBytes = 126u;

constexpr std::uint32_t kTopValue = 0xffffffffu;
constexpr std::uint32_t kFirstQuarter = 0x40000000u;
constexpr std::uint32_t kHalf = 0x80000000u;
constexpr std::uint32_t kThirdQuarter = 0xc0000000u;

constexpr std::uint32_t kProbBits = 15u;
constexpr std::uint32_t kProbScale = 1u << kProbBits; // 32768
constexpr std::uint16_t kProbInitial = 1u << (kProbBits - 1u); // 16384

constexpr std::size_t kRansChunkBytes = 32768u;
constexpr std::uint32_t kRansL = 1u << 23u;

enum class BaseKind : std::uint16_t {
    Trades = 1u,
    BookTicker = 2u,
    Depth = 3u,
};

// Single-current entropy modes: division-free 15-bit shift adaptive binary
// arithmetic coding (Ac16Ctx0, Ac16Ctx8, Ac16Ctx12, Ac32Ctx8) and genuine
// 12-bit scale (M=4096) byte-level table/shift rANS (RansByteStatic).
enum class EntropyKind : std::uint16_t {
    Ac16Ctx0 = 1u,
    Ac16Ctx8 = 2u,
    Ac16Ctx12 = 3u,
    Ac32Ctx8 = 4u,
    RansByteStatic = 5u,
};

struct Header {
    std::uint32_t magic{kMagic};
    std::uint16_t entropy{0};
    std::uint16_t base{0};
    std::uint16_t stream{0};
    std::uint32_t headerCrc32c{0};
    std::uint64_t inputBytes{0};
    std::uint64_t baseBytes{0};
    std::uint64_t outputBytes{0};
    std::uint64_t lineCount{0};
    std::uint64_t payloadBytes{0};
    std::uint32_t payloadCrc32c{0};
    std::uint32_t decodedCrc32c{0};
};

std::vector<std::uint8_t> serializeHeader(Header header, bool includeCrc);
std::uint32_t headerCrc32c(const Header& header) noexcept;
bool parseHeader(const std::uint8_t* data, std::size_t size, Header& out) noexcept;
bool validHeader(const Header& header) noexcept;

// Fixed 15-bit shift-adapted probability estimator p0 in [1, 32767].
// Interval split uses `(range * p0) >> 15u` and decoder uses `code_ > split`
// with zero runtime division or modulo instructions.
struct ShiftBitModel {
    std::uint16_t p0{kProbInitial};

    constexpr void update(bool bit, unsigned shiftRate) noexcept {
        if (bit) {
            p0 = static_cast<std::uint16_t>(p0 - (p0 >> shiftRate));
        } else {
            p0 = static_cast<std::uint16_t>(p0 + ((kProbScale - p0) >> shiftRate));
        }
    }
};

// Prepared reusable entropy scratch for both shift-arithmetic and byte-rANS.
struct ArithmeticWorkspace {
    std::array<ShiftBitModel, 4096u> models{};
    std::array<std::uint32_t, 256u> ransRawCount{};
    std::array<std::uint16_t, 256u> ransFreq{};
    std::array<std::uint16_t, 256u> ransStart{};
    std::array<std::uint8_t, 4096u> ransSlotToSym{};
    std::vector<std::uint8_t> ransRevBytes{};

    ArithmeticWorkspace() {
        ransRevBytes.reserve(kRansChunkBytes * 2u);
    }

    void reset(std::size_t contexts) noexcept {
        std::fill_n(models.begin(), contexts, ShiftBitModel{});
    }
};

std::vector<std::uint8_t> arithmeticEncode(std::span<const std::uint8_t> input, EntropyKind kind);

bool arithmeticEncodeBounded(ArithmeticWorkspace& workspace,
                             std::span<const std::uint8_t> input, EntropyKind kind,
                             std::size_t limit, std::vector<std::uint8_t>& output);
bool arithmeticEncodeBounded(std::span<const std::uint8_t> input, EntropyKind kind,
                             std::size_t limit, std::vector<std::uint8_t>& output);
bool arithmeticDecodeBytes(ArithmeticWorkspace& workspace,
                           std::span<const std::uint8_t> input, EntropyKind kind,
                           std::span<std::uint8_t> output);
bool arithmeticDecodeBytes(std::span<const std::uint8_t> input, EntropyKind kind,
                           std::span<std::uint8_t> output);

std::unique_ptr<internal::DecodeCursor> entropyCursor(const internal::DecodeSource& encoded,
                                                      const Header& header);
Status verifyPayload(const internal::DecodeSource& file, Header& header);
Status readHeader(const internal::DecodeSource& file, Header& header);
Status decodeSource(const internal::DecodeSource& file, const DecodedBlockCallback& onBlock);

EntropyKind entropyKindFor(std::string_view id) noexcept;
BaseKind baseKindFor(StreamType streamType) noexcept;
std::string_view basePipelineId(BaseKind base) noexcept;
std::string_view formatIdFor(BaseKind base, EntropyKind kind) noexcept;
std::string_view entropyName(EntropyKind kind) noexcept;

}  // namespace hft_compressor::codecs::entropy_hftmac::detail

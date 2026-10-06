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

constexpr std::uint32_t kMagic = 0x31464845u;

// EHF1
constexpr std::uint16_t kVersion = 1u;

constexpr std::size_t kHeaderBytes = 128u;

constexpr std::uint32_t kTopValue = 0xffffffffu;

constexpr std::uint32_t kFirstQuarter = 0x40000000u;

constexpr std::uint32_t kHalf = 0x80000000u;

constexpr std::uint32_t kThirdQuarter = 0xc0000000u;

enum class BaseKind : std::uint16_t {
    Trades = 1u,
    BookTicker = 2u,
    Depth = 3u,
};

enum class EntropyKind : std::uint16_t {
    Ac16Ctx0 = 1u,
    Ac16Ctx8 = 2u,
    Ac16Ctx12 = 3u,
    Ac32Ctx8 = 4u,
    RangeByteCtx8 = 5u,
    RansByteStatic = 6u,
};

struct Header {
    std::uint32_t magic{kMagic};
    std::uint16_t version{kVersion};
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

// EN: Integer binary arithmetic coding. The interval [low, high] is split by
// the adaptive zero/one frequencies, so each bit costs about -log2(p(bit)).
// RU: Целочисленное бинарное арифметическое кодирование. Интервал [low, high]
// делится адаптивными частотами нулей/единиц, поэтому каждый бит стоит около
// -log2(p(bit)).
std::vector<std::uint8_t> arithmeticEncode(std::span<const std::uint8_t> input, EntropyKind kind);

// Cold byte blocks share the current arithmetic interval/model and exact
// termination rules. A full encoded bound stops writing without growing it.
bool arithmeticEncodeBounded(std::span<const std::uint8_t> input, EntropyKind kind,
                             std::size_t limit, std::vector<std::uint8_t>& output);
bool arithmeticDecodeBytes(std::span<const std::uint8_t> input, EntropyKind kind,
                           std::span<std::uint8_t> output);

std::unique_ptr<internal::DecodeCursor> arithmeticCursor(const internal::DecodeSource& encoded,
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

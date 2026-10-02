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

struct BitWriter {
    std::vector<std::uint8_t> bytes{};
    std::uint8_t current{0};
    std::uint8_t bitCount{0};

    void bit(bool value) {
        current = static_cast<std::uint8_t>((current << 1u) | (value ? 1u : 0u));
        ++bitCount;
        if (bitCount == 8u) {
            bytes.push_back(current);
            current = 0;
            bitCount = 0;
        }
    }

    void finish() {
        if (bitCount == 0u) return;
        current = static_cast<std::uint8_t>(current << (8u - bitCount));
        bytes.push_back(current);
        current = 0;
        bitCount = 0;
    }
};

struct BitReader {
    std::span<const std::uint8_t> bytes{};
    std::size_t bytePos{0};
    std::uint8_t bitPos{0};

    bool bit() noexcept {
        if (bytePos >= bytes.size()) return false;
        const bool out = ((bytes[bytePos] >> (7u - bitPos)) & 1u) != 0u;
        ++bitPos;
        if (bitPos == 8u) {
            bitPos = 0;
            ++bytePos;
        }
        return out;
    }
};

struct BitModel {
    std::uint32_t zero{1};
    std::uint32_t one{1};

    std::uint32_t total() const noexcept { return zero + one; }

    void update(bool bit, std::uint32_t maxTotal) noexcept {
        if (bit) ++one;
        else ++zero;
        if (total() <= maxTotal) return;
        zero = (zero + 1u) >> 1u;
        one = (one + 1u) >> 1u;
        if (zero == 0u) zero = 1u;
        if (one == 0u) one = 1u;
    }
};

struct ArithmeticProfile {
    EntropyKind kind{EntropyKind::Ac16Ctx0};
    std::uint32_t contextCount{1};
    std::uint32_t maxTotal{1u << 16u};
};

ArithmeticProfile profileFor(EntropyKind kind) noexcept {
    switch (kind) {
        case EntropyKind::Ac16Ctx0: return {kind, 1u, 1u << 16u};
        case EntropyKind::Ac16Ctx8: return {kind, 256u, 1u << 16u};
        case EntropyKind::Ac16Ctx12: return {kind, 4096u, 1u << 16u};
        case EntropyKind::Ac32Ctx8: return {kind, 256u, 1u << 20u};
        case EntropyKind::RangeByteCtx8: return {kind, 2048u, 1u << 16u};
        case EntropyKind::RansByteStatic: return {kind, 4096u, 1u << 16u};
    }
    return {kind, 1u, 1u << 16u};
}

std::uint32_t contextFor(const ArithmeticProfile& profile, std::uint8_t previous, std::uint8_t bitIndex) noexcept {
    switch (profile.kind) {
        case EntropyKind::Ac16Ctx0: return 0u;
        case EntropyKind::Ac16Ctx8:
        case EntropyKind::Ac32Ctx8: return previous;
        case EntropyKind::Ac16Ctx12:
        case EntropyKind::RansByteStatic: return (static_cast<std::uint32_t>(previous) << 4u) | bitIndex;
        case EntropyKind::RangeByteCtx8: return (static_cast<std::uint32_t>(previous) << 3u) | (bitIndex & 7u);
    }
    return 0u;
}

void emitBitPlusPending(BitWriter& out, bool bit, std::uint32_t& pending) {
    out.bit(bit);
    while (pending != 0u) {
        out.bit(!bit);
        --pending;
    }
}

}

// EN: Integer binary arithmetic coding. The interval [low, high] is split by
// the adaptive zero/one frequencies, so each bit costs about -log2(p(bit)).
// RU: Целочисленное бинарное арифметическое кодирование. Интервал [low, high]
// делится адаптивными частотами нулей/единиц, поэтому каждый бит стоит около
// -log2(p(bit)).
std::vector<std::uint8_t> arithmeticEncode(std::span<const std::uint8_t> input, EntropyKind kind) {
    const ArithmeticProfile profile = profileFor(kind);
    std::vector<BitModel> models(profile.contextCount);
    BitWriter out;
    std::uint32_t low = 0;
    std::uint32_t high = kTopValue;
    std::uint32_t pending = 0;
    std::uint8_t previous = 0;

    for (const auto byte : input) {
        for (std::uint8_t bitIndex = 0; bitIndex < 8u; ++bitIndex) {
            const bool bit = ((byte >> (7u - bitIndex)) & 1u) != 0u;
            auto& model = models[contextFor(profile, previous, bitIndex)];
            const std::uint64_t range = static_cast<std::uint64_t>(high) - low + 1u;
            const std::uint32_t split = static_cast<std::uint32_t>(low + ((range * model.zero) / model.total()) - 1u);
            if (bit) low = split + 1u;
            else high = split;

            for (;;) {
                if (high < kHalf) emitBitPlusPending(out, false, pending);
                else if (low >= kHalf) {
                    emitBitPlusPending(out, true, pending);
                    low -= kHalf;
                    high -= kHalf;
                } else if (low >= kFirstQuarter && high < kThirdQuarter) {
                    ++pending;
                    low -= kFirstQuarter;
                    high -= kFirstQuarter;
                } else {
                    break;
                }
                low <<= 1u;
                high = (high << 1u) | 1u;
            }
            model.update(bit, profile.maxTotal);
        }
        previous = byte;
    }

    ++pending;
    emitBitPlusPending(out, low >= kFirstQuarter, pending);
    out.finish();
    return out.bytes;
}

Status arithmeticDecode(std::span<const std::uint8_t> encoded,
                        EntropyKind kind,
                        std::uint64_t decodedBytes,
                        std::vector<std::uint8_t>& out) noexcept {
    const ArithmeticProfile profile = profileFor(kind);
    std::vector<BitModel> models(profile.contextCount);
    BitReader bits{encoded};
    std::uint32_t low = 0;
    std::uint32_t high = kTopValue;
    std::uint32_t code = 0;
    for (std::uint32_t i = 0; i < 32u; ++i) code = (code << 1u) | (bits.bit() ? 1u : 0u);

    out.clear();
    out.reserve(static_cast<std::size_t>(decodedBytes));
    std::uint8_t previous = 0;
    for (std::uint64_t i = 0; i < decodedBytes; ++i) {
        std::uint8_t byte = 0;
        for (std::uint8_t bitIndex = 0; bitIndex < 8u; ++bitIndex) {
            auto& model = models[contextFor(profile, previous, bitIndex)];
            const std::uint64_t range = static_cast<std::uint64_t>(high) - low + 1u;
            const std::uint64_t scaled = (((static_cast<std::uint64_t>(code) - low + 1u) * model.total()) - 1u) / range;
            const bool bit = scaled >= model.zero;
            const std::uint32_t split = static_cast<std::uint32_t>(low + ((range * model.zero) / model.total()) - 1u);
            if (bit) low = split + 1u;
            else high = split;

            for (;;) {
                if (high < kHalf) {
                } else if (low >= kHalf) {
                    code -= kHalf;
                    low -= kHalf;
                    high -= kHalf;
                } else if (low >= kFirstQuarter && high < kThirdQuarter) {
                    code -= kFirstQuarter;
                    low -= kFirstQuarter;
                    high -= kFirstQuarter;
                } else {
                    break;
                }
                low <<= 1u;
                high = (high << 1u) | 1u;
                code = (code << 1u) | (bits.bit() ? 1u : 0u);
            }
            model.update(bit, profile.maxTotal);
            byte = static_cast<std::uint8_t>((byte << 1u) | (bit ? 1u : 0u));
        }
        out.push_back(byte);
        previous = byte;
    }
    return Status::Ok;
}

}

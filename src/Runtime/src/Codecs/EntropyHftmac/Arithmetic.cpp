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
    std::size_t limit{std::numeric_limits<std::size_t>::max()};
    bool good{true};

    void bit(bool value) {
        if (!good) return;
        current = static_cast<std::uint8_t>((current << 1u) | (value ? 1u : 0u));
        ++bitCount;
        if (bitCount == 8u) {
            if (bytes.size() == limit) { good = false; return; }
            bytes.push_back(current);
            current = 0;
            bitCount = 0;
        }
    }

    void finish() {
        if (bitCount == 0u) return;
        if (bytes.size() == limit) { good = false; return; }
        current = static_cast<std::uint8_t>(current << (8u - bitCount));
        bytes.push_back(current);
        current = 0;
        bitCount = 0;
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
    while (pending != 0u && out.good) {
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
    std::vector<std::uint8_t> output;
    arithmeticEncodeBounded(input, kind, std::numeric_limits<std::size_t>::max(), output);
    return output;
}

bool arithmeticEncodeBounded(std::span<const std::uint8_t> input, EntropyKind kind,
                             std::size_t limit, std::vector<std::uint8_t>& output) {
    const ArithmeticProfile profile = profileFor(kind);
    std::vector<BitModel> models(profile.contextCount);
    BitWriter out;
    out.limit = limit;
    if (limit != std::numeric_limits<std::size_t>::max()) out.bytes.reserve(limit);
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
            if (!out.good) { output.clear(); return false; }
        }
        previous = byte;
    }

    ++pending;
    emitBitPlusPending(out, low >= kFirstQuarter, pending);
    out.finish();
    if (!out.good) { output.clear(); return false; }
    output = std::move(out.bytes);
    return true;
}

namespace {

// Encoder and decoder share interval/model rules. Verification emits the
// encoder's exact final bits into a second compressed-input cursor, including
// byte padding. This distinguishes a complete stream from an arbitrary CRC-
// valid prefix or a false decoded-size declaration without storing output.
class ArithmeticCursor final : public internal::DecodeCursor {
    ArithmeticProfile profile_;
    std::array<BitModel, 4096> models_{};
    std::unique_ptr<internal::DecodeCursor> input_;
    std::unique_ptr<internal::DecodeCursor> comparison_;
    std::uint64_t remaining_{};
    std::uint32_t low_{}, high_{kTopValue}, code_{};
    std::uint64_t pending_{};
    std::uint8_t previous_{}, current_{}, bitPos_{8}, virtualBits_{};
    std::uint8_t emitted_{}, emittedBits_{};
    bool good_{true};

    bool bit() {
        if (bitPos_ == 8u) {
            if (input_->remaining() == 0u) {
                // The existing encoder emits the final interval plus <=7 byte
                // padding bits. A 32-bit lookahead needs at most 30 more zero
                // bits. Exact encoder comparison below proves termination.
                if (++virtualBits_ > 30u) good_ = false;
                return false;
            }
            if (!input_->byte(current_)) { good_ = false; return false; }
            bitPos_ = 0;
        }
        return ((current_ >> (7u - bitPos_++)) & 1u) != 0u;
    }
    void emit(bool value) {
        if (!comparison_) return;
        emitted_ = static_cast<std::uint8_t>((emitted_ << 1u) | value);
        if (++emittedBits_ == 8u) {
            std::uint8_t expected{};
            if (!comparison_->byte(expected) || expected != emitted_) good_ = false;
            emitted_ = 0; emittedBits_ = 0;
        }
    }
    void emitPending(bool value) {
        emit(value);
        while (pending_ != 0u) { emit(!value); --pending_; }
    }
public:
    ArithmeticCursor(const internal::DecodeSource& encoded, const Header& header, bool verify)
        : ArithmeticCursor(encoded, kHeaderBytes, header.payloadBytes, header.baseBytes,
                           static_cast<EntropyKind>(header.entropy), verify) {}
    ArithmeticCursor(const internal::DecodeSource& encoded, std::size_t offset,
                     std::size_t bytes, std::size_t decodedBytes, EntropyKind kind, bool verify)
        : profile_(profileFor(kind)), input_(encoded.cursor(offset, bytes)),
          comparison_(verify ? encoded.cursor(offset, bytes) : nullptr), remaining_(decodedBytes) {
        if (!input_ || (verify && !comparison_)) { good_ = false; return; }
        for (unsigned i = 0; i < 32u; ++i) code_ = (code_ << 1u) | bit();
    }
    std::uint64_t remaining() const noexcept override { return remaining_; }
    bool byte(std::uint8_t& out) override {
        if (!good_ || remaining_ == 0u) return false;
        std::uint8_t value{};
        for (std::uint8_t bitIndex = 0; bitIndex < 8u; ++bitIndex) {
            if (code_ < low_ || code_ > high_) return good_ = false;
            auto& model = models_[contextFor(profile_, previous_, bitIndex)];
            const std::uint64_t range = static_cast<std::uint64_t>(high_) - low_ + 1u;
            const std::uint64_t scaled = (((static_cast<std::uint64_t>(code_) - low_ + 1u) * model.total()) - 1u) / range;
            const bool valueBit = scaled >= model.zero;
            const auto split = static_cast<std::uint32_t>(low_ + ((range * model.zero) / model.total()) - 1u);
            if (valueBit) low_ = split + 1u;
            else high_ = split;
            for (;;) {
                if (high_ < kHalf) emitPending(false);
                else if (low_ >= kHalf) {
                    emitPending(true); code_ -= kHalf; low_ -= kHalf; high_ -= kHalf;
                } else if (low_ >= kFirstQuarter && high_ < kThirdQuarter) {
                    if (pending_ == std::numeric_limits<std::uint64_t>::max()) return good_ = false;
                    ++pending_; code_ -= kFirstQuarter; low_ -= kFirstQuarter; high_ -= kFirstQuarter;
                } else break;
                low_ <<= 1u; high_ = (high_ << 1u) | 1u;
                code_ = (code_ << 1u) | bit();
                if (!good_) return false;
            }
            model.update(valueBit, profile_.maxTotal);
            value = static_cast<std::uint8_t>((value << 1u) | valueBit);
        }
        --remaining_; previous_ = value; out = value; return good_;
    }
    bool finish() {
        if (!good_ || remaining_ != 0u || !comparison_) return false;
        if (pending_ == std::numeric_limits<std::uint64_t>::max()) return false;
        ++pending_; emitPending(low_ >= kFirstQuarter);
        if (emittedBits_ != 0u) {
            while (emittedBits_ != 0u) emit(false);
        }
        return good_ && comparison_->remaining() == 0u && input_->remaining() == 0u;
    }
};

} // namespace

bool arithmeticDecodeBytes(std::span<const std::uint8_t> input, EntropyKind kind,
                           std::span<std::uint8_t> output) {
    internal::SpanDecodeSource source(input);
    auto decoded = std::make_unique<ArithmeticCursor>(source, 0, input.size(), output.size(), kind, true);
    return decoded->read(output) && decoded->finish();
}

std::unique_ptr<internal::DecodeCursor> arithmeticCursor(const internal::DecodeSource& encoded, const Header& header) {
    return std::make_unique<ArithmeticCursor>(encoded, header, false);
}

Status verifyPayload(const internal::DecodeSource& file, Header& header) {
    auto status = readHeader(file, header);
    if (!isOk(status)) return status;
    auto payload = file.cursor(kHeaderBytes, header.payloadBytes);
    if (!payload) return Status::IoError;
    auto buffer = std::make_unique<std::array<std::uint8_t, 65536>>();
    std::uint32_t payloadCrc = 0xffffffffu;
    while (payload->remaining() != 0u) {
        const auto count = static_cast<std::size_t>(std::min<std::uint64_t>(payload->remaining(), buffer->size()));
        auto bytes = std::span{*buffer}.first(count);
        if (!payload->read(bytes)) return Status::CorruptData;
        payloadCrc = format::updateCrc32c(payloadCrc, bytes);
    }
    if (~payloadCrc != header.payloadCrc32c) return Status::CorruptData;
    auto decoded = std::make_unique<ArithmeticCursor>(file, header, true);
    std::uint32_t decodedCrc = 0xffffffffu;
    while (decoded->remaining() != 0u) {
        const auto count = static_cast<std::size_t>(std::min<std::uint64_t>(decoded->remaining(), buffer->size()));
        auto bytes = std::span{*buffer}.first(count);
        if (!decoded->read(bytes)) return Status::CorruptData;
        decodedCrc = format::updateCrc32c(decodedCrc, bytes);
    }
    if (!decoded->finish() || ~decodedCrc != header.decodedCrc32c || !file.unchanged()) return Status::CorruptData;
    return Status::Ok;
}

} // namespace hft_compressor::codecs::entropy_hftmac::detail

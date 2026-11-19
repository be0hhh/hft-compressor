#include "EntropyHftMac.hpp"
#include "EntropyHftMacInternal.hpp"

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <memory>
#include <span>
#include <vector>

#include "../../Common/ExactScaleMath.hpp"
#include "../../Container/Hfc/Format.hpp"

namespace hft_compressor::codecs::entropy_hftmac::detail {
namespace {

struct BitWriter {
    std::vector<std::uint8_t>& bytes;
    std::uint8_t current{0};
    std::uint8_t bitCount{0};
    std::size_t limit{std::numeric_limits<std::size_t>::max()};
    bool good{true};

    void bit(bool value) {
        if (!good) return;
        current = static_cast<std::uint8_t>((current << 1u) | (value ? 1u : 0u));
        ++bitCount;
        if (bitCount == 8u) {
            if (bytes.size() == limit) {
                good = false;
                return;
            }
            bytes.push_back(current);
            current = 0;
            bitCount = 0;
        }
    }

    void finish() {
        if (bitCount == 0u) return;
        if (bytes.size() == limit) {
            good = false;
            return;
        }
        current = static_cast<std::uint8_t>(current << (8u - bitCount));
        bytes.push_back(current);
        current = 0;
        bitCount = 0;
    }
};

struct ArithmeticProfile {
    EntropyKind kind{EntropyKind::Ac16Ctx0};
    std::uint32_t contextCount{1u};
    unsigned shiftRate{5u};
};

constexpr ArithmeticProfile profileFor(EntropyKind kind) noexcept {
    switch (kind) {
        case EntropyKind::Ac16Ctx0: return {kind, 1u, 5u};
        case EntropyKind::Ac16Ctx8: return {kind, 256u, 5u};
        case EntropyKind::Ac16Ctx12: return {kind, 4096u, 5u};
        case EntropyKind::Ac32Ctx8: return {kind, 256u, 7u};
        case EntropyKind::RansByteStatic: return {kind, 1u, 5u};
    }
    return {kind, 1u, 5u};
}

constexpr std::uint32_t contextFor(const ArithmeticProfile& profile,
                                   std::uint8_t previous,
                                   std::uint8_t bitIndex) noexcept {
    switch (profile.kind) {
        case EntropyKind::Ac16Ctx0: return 0u;
        case EntropyKind::Ac16Ctx8:
        case EntropyKind::Ac32Ctx8: return previous;
        case EntropyKind::Ac16Ctx12: return (static_cast<std::uint32_t>(previous) << 4u) | bitIndex;
        case EntropyKind::RansByteStatic: return 0u;
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

bool appendByteBounded(std::vector<std::uint8_t>& out, std::size_t limit, std::uint8_t value) {
    if (out.size() >= limit) return false;
    out.push_back(value);
    return true;
}

bool appendLe16Bounded(std::vector<std::uint8_t>& out, std::size_t limit, std::uint16_t value) {
    return appendByteBounded(out, limit, static_cast<std::uint8_t>(value & 0xffu))
        && appendByteBounded(out, limit, static_cast<std::uint8_t>((value >> 8u) & 0xffu));
}

bool appendLe32Bounded(std::vector<std::uint8_t>& out, std::size_t limit, std::uint32_t value) {
    return appendByteBounded(out, limit, static_cast<std::uint8_t>(value & 0xffu))
        && appendByteBounded(out, limit, static_cast<std::uint8_t>((value >> 8u) & 0xffu))
        && appendByteBounded(out, limit, static_cast<std::uint8_t>((value >> 16u) & 0xffu))
        && appendByteBounded(out, limit, static_cast<std::uint8_t>((value >> 24u) & 0xffu));
}

bool appendFreqVarintBounded(std::vector<std::uint8_t>& out, std::size_t limit, std::uint16_t fMinusOne) {
    if (fMinusOne < 0x80u) {
        return appendByteBounded(out, limit, static_cast<std::uint8_t>(fMinusOne));
    }
    return appendByteBounded(out, limit, static_cast<std::uint8_t>((fMinusOne & 0x7fu) | 0x80u))
        && appendByteBounded(out, limit, static_cast<std::uint8_t>(fMinusOne >> 7u));
}

// Genuine byte-level table/shift rANS encoder (12-bit scale M = 4096).
// Zero runtime / or % instructions in both normalization and inner encode loop.
bool ransEncodeBounded(ArithmeticWorkspace& workspace,
                       std::span<const std::uint8_t> input,
                       std::size_t limit,
                       std::vector<std::uint8_t>& output) {
    output.clear();
    if (input.empty()) return true;
    if (limit != std::numeric_limits<std::size_t>::max() && output.capacity() < limit) {
        output.reserve(std::min<std::size_t>(limit, input.size() + 64u));
    }

    for (std::size_t offset = 0u; offset < input.size(); offset += kRansChunkBytes) {
        const std::size_t chunkLen = std::min<std::size_t>(kRansChunkBytes, input.size() - offset);
        const auto chunk = input.subspan(offset, chunkLen);

        workspace.ransRawCount.fill(0u);
        for (const std::uint8_t b : chunk) {
            ++workspace.ransRawCount[b];
        }

        std::size_t activeSymbols = 0u;
        std::uint8_t singleSym = 0u;
        math::normalizeByteCounts4096(
            workspace.ransRawCount, chunkLen, workspace.ransFreq, activeSymbols, singleSym);

        if (!appendLe16Bounded(output, limit, static_cast<std::uint16_t>(chunkLen - 1u))) {
            output.clear();
            return false;
        }

        if (activeSymbols == 1u) {
            if (!appendByteBounded(output, limit, 0u) ||
                !appendByteBounded(output, limit, singleSym)) {
                output.clear();
                return false;
            }
            continue;
        }

        std::uint16_t cum = 0u;
        std::array<std::uint8_t, 32u> bitmap{};
        for (std::size_t s = 0u; s < 256u; ++s) {
            workspace.ransStart[s] = cum;
            const std::uint16_t f = workspace.ransFreq[s];
            if (f != 0u) {
                bitmap[s >> 3u] = static_cast<std::uint8_t>(bitmap[s >> 3u] | (1u << (s & 7u)));
                cum = static_cast<std::uint16_t>(cum + f);
            }
        }

        workspace.ransRevBytes.clear();
        std::uint32_t x = kRansL;
        for (std::size_t idx = chunkLen; idx != 0u; --idx) {
            const std::uint8_t sym = chunk[idx - 1u];
            const std::uint32_t f = workspace.ransFreq[sym];
            const std::uint32_t xMax = f << 19u; // ((kRansL >> 12) << 8) * f
            while (x >= xMax) {
                workspace.ransRevBytes.push_back(static_cast<std::uint8_t>(x & 0xffu));
                x >>= 8u;
            }
            const auto q = static_cast<std::uint32_t>(
                (static_cast<unsigned __int128>(x + 1u) * math::kRansFreqReciprocals[f]) >> 64u);
            const std::uint32_t r = x - q * f;
            x = (q << math::kRansScaleBits) + r + workspace.ransStart[sym];
        }

        if (!appendByteBounded(output, limit, 1u)) {
            output.clear();
            return false;
        }
        if (output.size() > limit || bitmap.size() > limit - output.size()) {
            output.clear();
            return false;
        }
        output.insert(output.end(), bitmap.begin(), bitmap.end());

        for (std::size_t s = 0u; s < 256u; ++s) {
            const std::uint16_t f = workspace.ransFreq[s];
            if (f == 0u) continue;
            if (!appendFreqVarintBounded(output, limit, static_cast<std::uint16_t>(f - 1u))) {
                output.clear();
                return false;
            }
        }

        const auto streamBytes = static_cast<std::uint32_t>(workspace.ransRevBytes.size());
        if (!appendLe32Bounded(output, limit, streamBytes) ||
            !appendLe32Bounded(output, limit, x)) {
            output.clear();
            return false;
        }
        if (output.size() > limit || workspace.ransRevBytes.size() > limit - output.size()) {
            output.clear();
            return false;
        }
        for (auto it = workspace.ransRevBytes.rbegin(); it != workspace.ransRevBytes.rend(); ++it) {
            output.push_back(*it);
        }
    }
    return true;
}

bool shiftArithmeticEncodeBounded(ArithmeticWorkspace& workspace,
                                  std::span<const std::uint8_t> input,
                                  const ArithmeticProfile& profile,
                                  std::size_t limit,
                                  std::vector<std::uint8_t>& output) {
    workspace.reset(profile.contextCount);
    auto& models = workspace.models;
    output.clear();
    BitWriter out{output};
    out.limit = limit;
    if (limit != std::numeric_limits<std::size_t>::max() && out.bytes.capacity() < limit) {
        out.bytes.reserve(std::min<std::size_t>(limit, input.size() + 64u));
    }
    std::uint32_t low = 0u;
    std::uint32_t high = kTopValue;
    std::uint32_t pending = 0u;
    std::uint8_t previous = 0u;

    for (const auto byte : input) {
        for (std::uint8_t bitIndex = 0u; bitIndex < 8u; ++bitIndex) {
            const bool bit = ((byte >> (7u - bitIndex)) & 1u) != 0u;
            auto& model = models[contextFor(profile, previous, bitIndex)];
            const std::uint64_t range = static_cast<std::uint64_t>(high) - low + 1u;
            const auto split = low + static_cast<std::uint32_t>((range * model.p0) >> kProbBits) - 1u;
            if (bit) low = split + 1u;
            else high = split;

            for (;;) {
                if (high < kHalf) {
                    emitBitPlusPending(out, false, pending);
                } else if (low >= kHalf) {
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
            model.update(bit, profile.shiftRate);
            if (!out.good) {
                output.clear();
                return false;
            }
        }
        previous = byte;
    }

    ++pending;
    emitBitPlusPending(out, low >= kFirstQuarter, pending);
    out.finish();
    if (!out.good) {
        output.clear();
        return false;
    }
    return true;
}

class EntropyDecodeCursor : public internal::DecodeCursor {
public:
    virtual bool finish() = 0;
};

// Division-free 15-bit shift arithmetic decoder cursor with exact encoder
// re-emission comparison to reject truncated/padded/forged streams.
class ShiftArithmeticCursor final : public EntropyDecodeCursor {
    ArithmeticProfile profile_;
    std::unique_ptr<ArithmeticWorkspace> ownedModels_;
    std::span<ShiftBitModel> models_;
    std::unique_ptr<internal::DecodeCursor> input_;
    std::unique_ptr<internal::DecodeCursor> comparison_;
    internal::DecodeCursor* inputCursor_{};
    internal::DecodeCursor* comparisonCursor_{};
    std::uint64_t remaining_{};
    std::uint32_t low_{}, high_{kTopValue}, code_{};
    std::uint64_t pending_{};
    std::uint8_t previous_{}, current_{}, bitPos_{8u}, virtualBits_{};
    std::uint8_t emitted_{}, emittedBits_{};
    bool good_{true};

    bool bit() {
        if (bitPos_ == 8u) {
            if (inputCursor_->remaining() == 0u) {
                if (++virtualBits_ > 30u) good_ = false;
                return false;
            }
            if (!inputCursor_->byte(current_)) {
                good_ = false;
                return false;
            }
            bitPos_ = 0u;
        }
        return ((current_ >> (7u - bitPos_++)) & 1u) != 0u;
    }

    void emit(bool value) {
        if (!comparisonCursor_) return;
        emitted_ = static_cast<std::uint8_t>((emitted_ << 1u) | value);
        if (++emittedBits_ == 8u) {
            std::uint8_t expected{};
            if (!comparisonCursor_->byte(expected) || expected != emitted_) good_ = false;
            emitted_ = 0u;
            emittedBits_ = 0u;
        }
    }

    void emitPending(bool value) {
        emit(value);
        while (pending_ != 0u) {
            emit(!value);
            --pending_;
        }
    }

public:
    ShiftArithmeticCursor(const internal::DecodeSource& encoded, const Header& header, bool verify)
        : profile_(profileFor(static_cast<EntropyKind>(header.entropy))),
          ownedModels_(std::make_unique<ArithmeticWorkspace>()),
          models_(ownedModels_->models),
          input_(encoded.cursor(kHeaderBytes, header.payloadBytes)),
          comparison_(verify ? encoded.cursor(kHeaderBytes, header.payloadBytes) : nullptr),
          inputCursor_(input_.get()),
          comparisonCursor_(comparison_.get()),
          remaining_(header.baseBytes) {
        if (!input_ || (verify && !comparison_)) {
            good_ = false;
            return;
        }
        for (unsigned i = 0u; i < 32u; ++i) code_ = (code_ << 1u) | bit();
    }

    ShiftArithmeticCursor(ArithmeticWorkspace& workspace,
                          internal::DecodeCursor& input,
                          internal::DecodeCursor& comparison,
                          std::size_t decodedBytes,
                          EntropyKind kind)
        : profile_(profileFor(kind)),
          models_(workspace.models),
          inputCursor_(&input),
          comparisonCursor_(&comparison),
          remaining_(decodedBytes) {
        workspace.reset(profile_.contextCount);
        for (unsigned i = 0u; i < 32u; ++i) code_ = (code_ << 1u) | bit();
    }

    std::uint64_t remaining() const noexcept override { return remaining_; }

    bool byte(std::uint8_t& out) override {
        if (!good_ || remaining_ == 0u) return false;
        std::uint8_t value{};
        for (std::uint8_t bitIndex = 0u; bitIndex < 8u; ++bitIndex) {
            if (code_ < low_ || code_ > high_) return good_ = false;
            auto& model = models_[contextFor(profile_, previous_, bitIndex)];
            const std::uint64_t range = static_cast<std::uint64_t>(high_) - low_ + 1u;
            const auto split = low_ + static_cast<std::uint32_t>((range * model.p0) >> kProbBits) - 1u;
            const bool valueBit = (code_ > split);
            if (valueBit) low_ = split + 1u;
            else high_ = split;
            for (;;) {
                if (high_ < kHalf) {
                    emitPending(false);
                } else if (low_ >= kHalf) {
                    emitPending(true);
                    code_ -= kHalf;
                    low_ -= kHalf;
                    high_ -= kHalf;
                } else if (low_ >= kFirstQuarter && high_ < kThirdQuarter) {
                    if (pending_ == std::numeric_limits<std::uint64_t>::max()) return good_ = false;
                    ++pending_;
                    code_ -= kFirstQuarter;
                    low_ -= kFirstQuarter;
                    high_ -= kFirstQuarter;
                } else {
                    break;
                }
                low_ <<= 1u;
                high_ = (high_ << 1u) | 1u;
                code_ = (code_ << 1u) | bit();
                if (!good_) return false;
            }
            model.update(valueBit, profile_.shiftRate);
            value = static_cast<std::uint8_t>((value << 1u) | valueBit);
        }
        --remaining_;
        previous_ = value;
        out = value;
        return good_;
    }

    bool finish() override {
        if (!good_ || remaining_ != 0u || !comparisonCursor_) return false;
        if (pending_ == std::numeric_limits<std::uint64_t>::max()) return false;
        ++pending_;
        emitPending(low_ >= kFirstQuarter);
        while (emittedBits_ != 0u) emit(false);
        return good_ && comparisonCursor_->remaining() == 0u && inputCursor_->remaining() == 0u;
    }
};

// Streaming forward byte-table/shift rANS decoder cursor (12-bit scale M = 4096).
// Allocates fixed ~5 KiB table storage independent of declared baseBytes, and
// verifies exact canonical state_ == kRansL and zero trailing bytes per chunk.
class RansDecodeCursor final : public EntropyDecodeCursor {
    std::unique_ptr<ArithmeticWorkspace> ownedWorkspace_;
    ArithmeticWorkspace* ws_{};
    std::unique_ptr<internal::DecodeCursor> ownedInput_;
    internal::DecodeCursor* inputCursor_{};
    std::uint64_t remaining_{};
    std::size_t chunkRemaining_{};
    std::size_t streamRemaining_{};
    std::uint32_t state_{};
    std::uint8_t constSym_{};
    bool constantChunk_{false};
    bool good_{true};

    bool readLe16(std::uint16_t& out) {
        std::uint8_t b0{}, b1{};
        if (!inputCursor_->byte(b0) || !inputCursor_->byte(b1)) return false;
        out = static_cast<std::uint16_t>(b0 | (static_cast<std::uint16_t>(b1) << 8u));
        return true;
    }

    bool readLe32(std::uint32_t& out) {
        std::uint8_t b0{}, b1{}, b2{}, b3{};
        if (!inputCursor_->byte(b0) || !inputCursor_->byte(b1) ||
            !inputCursor_->byte(b2) || !inputCursor_->byte(b3)) return false;
        out = static_cast<std::uint32_t>(b0)
            | (static_cast<std::uint32_t>(b1) << 8u)
            | (static_cast<std::uint32_t>(b2) << 16u)
            | (static_cast<std::uint32_t>(b3) << 24u);
        return true;
    }

    bool readFreqMinusOne(std::uint16_t& out) {
        std::uint8_t low{};
        if (!inputCursor_->byte(low)) return false;
        if ((low & 0x80u) == 0u) {
            out = low;
            return true;
        }
        std::uint8_t high{};
        if (!inputCursor_->byte(high) || high == 0u) return false;
        const std::uint32_t val = static_cast<std::uint32_t>(low & 0x7fu) | (static_cast<std::uint32_t>(high) << 7u);
        if (val >= math::kRansScale - 1u) return false;
        out = static_cast<std::uint16_t>(val);
        return true;
    }

    bool loadNextChunk() {
        std::uint16_t chunkMinusOne{};
        if (!readLe16(chunkMinusOne)) return good_ = false;
        const std::size_t chunkLen = static_cast<std::size_t>(chunkMinusOne) + 1u;
        if (chunkLen > kRansChunkBytes || chunkLen > remaining_) return good_ = false;

        std::uint8_t mode{};
        if (!inputCursor_->byte(mode)) return good_ = false;
        if (mode == 0u) {
            if (!inputCursor_->byte(constSym_)) return good_ = false;
            constantChunk_ = true;
            chunkRemaining_ = chunkLen;
            streamRemaining_ = 0u;
            return true;
        }
        if (mode != 1u) return good_ = false;

        std::array<std::uint8_t, 32u> bitmap{};
        if (!inputCursor_->read(bitmap)) return good_ = false;

        ws_->ransFreq.fill(0u);
        std::uint32_t totalFreq = 0u;
        std::uint32_t activeCount = 0u;
        for (std::size_t s = 0u; s < 256u; ++s) {
            if (((bitmap[s >> 3u] >> (s & 7u)) & 1u) == 0u) continue;
            std::uint16_t fMinusOne{};
            if (!readFreqMinusOne(fMinusOne)) return good_ = false;
            const std::uint16_t f = static_cast<std::uint16_t>(fMinusOne + 1u);
            ws_->ransFreq[s] = f;
            ws_->ransStart[s] = static_cast<std::uint16_t>(totalFreq);
            totalFreq += f;
            if (totalFreq > math::kRansScale) return good_ = false;
            std::fill_n(ws_->ransSlotToSym.data() + ws_->ransStart[s], f, static_cast<std::uint8_t>(s));
            ++activeCount;
        }
        if (activeCount < 2u || totalFreq != math::kRansScale) return good_ = false;

        std::uint32_t streamBytes{};
        if (!readLe32(streamBytes) || streamBytes > chunkLen * 2u) return good_ = false;
        if (inputCursor_->remaining() < static_cast<std::uint64_t>(streamBytes) + 4u) return good_ = false;
        if (!readLe32(state_) || state_ < kRansL || state_ >= (kRansL << 8u)) return good_ = false;

        constantChunk_ = false;
        chunkRemaining_ = chunkLen;
        streamRemaining_ = streamBytes;
        return true;
    }

public:
    RansDecodeCursor(const internal::DecodeSource& encoded, const Header& header)
        : ownedWorkspace_(std::make_unique<ArithmeticWorkspace>()),
          ws_(ownedWorkspace_.get()),
          ownedInput_(encoded.cursor(kHeaderBytes, header.payloadBytes)),
          inputCursor_(ownedInput_.get()),
          remaining_(header.baseBytes) {
        if (!ownedInput_) good_ = false;
    }

    RansDecodeCursor(ArithmeticWorkspace& workspace,
                     internal::DecodeCursor& input,
                     std::size_t decodedBytes)
        : ws_(&workspace),
          inputCursor_(&input),
          remaining_(decodedBytes) {}

    std::uint64_t remaining() const noexcept override { return remaining_; }

    bool byte(std::uint8_t& out) override {
        if (!good_ || remaining_ == 0u) return false;
        if (chunkRemaining_ == 0u && !loadNextChunk()) return false;

        if (constantChunk_) {
            out = constSym_;
            --chunkRemaining_;
            --remaining_;
            return true;
        }

        const std::uint32_t slot = state_ & (math::kRansScale - 1u);
        const std::uint8_t sym = ws_->ransSlotToSym[slot];
        state_ = static_cast<std::uint32_t>(ws_->ransFreq[sym]) * (state_ >> math::kRansScaleBits)
            + slot - ws_->ransStart[sym];
        --chunkRemaining_;
        --remaining_;

        while (state_ < kRansL) {
            if (streamRemaining_ == 0u) return good_ = false;
            std::uint8_t b{};
            if (!inputCursor_->byte(b)) return good_ = false;
            --streamRemaining_;
            state_ = (state_ << 8u) | b;
        }

        if (chunkRemaining_ == 0u && (streamRemaining_ != 0u || state_ != kRansL)) {
            return good_ = false;
        }
        out = sym;
        return true;
    }

    bool finish() override {
        return good_ && remaining_ == 0u && chunkRemaining_ == 0u
            && streamRemaining_ == 0u && inputCursor_->remaining() == 0u;
    }
};

} // namespace

std::vector<std::uint8_t> arithmeticEncode(std::span<const std::uint8_t> input, EntropyKind kind) {
    std::vector<std::uint8_t> output;
    arithmeticEncodeBounded(input, kind, std::numeric_limits<std::size_t>::max(), output);
    return output;
}

bool arithmeticEncodeBounded(std::span<const std::uint8_t> input, EntropyKind kind,
                             std::size_t limit, std::vector<std::uint8_t>& output) {
    auto workspace = std::make_unique<ArithmeticWorkspace>();
    return arithmeticEncodeBounded(*workspace, input, kind, limit, output);
}

bool arithmeticEncodeBounded(ArithmeticWorkspace& workspace,
                             std::span<const std::uint8_t> input, EntropyKind kind,
                             std::size_t limit, std::vector<std::uint8_t>& output) {
    if (kind == EntropyKind::RansByteStatic) {
        return ransEncodeBounded(workspace, input, limit, output);
    }
    return shiftArithmeticEncodeBounded(workspace, input, profileFor(kind), limit, output);
}

bool arithmeticDecodeBytes(std::span<const std::uint8_t> input, EntropyKind kind,
                           std::span<std::uint8_t> output) {
    auto workspace = std::make_unique<ArithmeticWorkspace>();
    return arithmeticDecodeBytes(*workspace, input, kind, output);
}

bool arithmeticDecodeBytes(ArithmeticWorkspace& workspace,
                           std::span<const std::uint8_t> input, EntropyKind kind,
                           std::span<std::uint8_t> output) {
    class SpanCursor final : public internal::DecodeCursor {
        std::span<const std::uint8_t> bytes_;
    public:
        explicit SpanCursor(std::span<const std::uint8_t> bytes) : bytes_(bytes) {}
        bool byte(std::uint8_t& out) override {
            if (bytes_.empty()) return false;
            out = bytes_.front();
            bytes_ = bytes_.subspan(1u);
            return true;
        }
        std::uint64_t remaining() const noexcept override { return bytes_.size(); }
    };

    if (kind == EntropyKind::RansByteStatic) {
        SpanCursor source(input);
        RansDecodeCursor decoded(workspace, source, output.size());
        return decoded.read(output) && decoded.finish();
    }
    SpanCursor source(input), comparison(input);
    ShiftArithmeticCursor decoded(workspace, source, comparison, output.size(), kind);
    return decoded.read(output) && decoded.finish();
}

std::unique_ptr<internal::DecodeCursor> entropyCursor(const internal::DecodeSource& encoded,
                                                      const Header& header) {
    const auto kind = static_cast<EntropyKind>(header.entropy);
    if (kind == EntropyKind::RansByteStatic) {
        return std::make_unique<RansDecodeCursor>(encoded, header);
    }
    return std::make_unique<ShiftArithmeticCursor>(encoded, header, false);
}

Status verifyPayload(const internal::DecodeSource& file, Header& header) {
    auto status = readHeader(file, header);
    if (!isOk(status)) return status;
    auto payload = file.cursor(kHeaderBytes, header.payloadBytes);
    if (!payload) return Status::IoError;
    auto buffer = std::make_unique<std::array<std::uint8_t, 65536u>>();
    std::uint32_t payloadCrc = 0xffffffffu;
    while (payload->remaining() != 0u) {
        const auto count = static_cast<std::size_t>(
            std::min<std::uint64_t>(payload->remaining(), buffer->size()));
        auto bytes = std::span{*buffer}.first(count);
        if (!payload->read(bytes)) return Status::CorruptData;
        payloadCrc = format::updateCrc32c(payloadCrc, bytes);
    }
    if (~payloadCrc != header.payloadCrc32c) return Status::CorruptData;

    const auto kind = static_cast<EntropyKind>(header.entropy);
    std::unique_ptr<EntropyDecodeCursor> decoded;
    if (kind == EntropyKind::RansByteStatic) {
        decoded = std::make_unique<RansDecodeCursor>(file, header);
    } else {
        decoded = std::make_unique<ShiftArithmeticCursor>(file, header, true);
    }

    std::uint32_t decodedCrc = 0xffffffffu;
    while (decoded->remaining() != 0u) {
        const auto count = static_cast<std::size_t>(
            std::min<std::uint64_t>(decoded->remaining(), buffer->size()));
        auto bytes = std::span{*buffer}.first(count);
        if (!decoded->read(bytes)) return Status::CorruptData;
        decodedCrc = format::updateCrc32c(decodedCrc, bytes);
    }
    if (!decoded->finish() || ~decodedCrc != header.decodedCrc32c || !file.unchanged()) {
        return Status::CorruptData;
    }
    return Status::Ok;
}

} // namespace hft_compressor::codecs::entropy_hftmac::detail

#pragma once

#include <array>
#include <bit>
#include <cstddef>
#include <cstdint>
#include <limits>
#include <span>
#include <utility>

namespace hft_compressor::math {

inline constexpr std::size_t kMaxSupportedRecordStride = 1024u;
inline constexpr std::size_t kMaxSupportedRecordsPerBlock = 256u;
inline constexpr std::uint32_t kRansScaleBits = 12u;
inline constexpr std::uint32_t kRansScale = 1u << kRansScaleBits; // 4096

[[nodiscard]] inline constexpr std::uint64_t uAbsI64(std::int64_t value) noexcept {
    return value < 0
        ? (std::uint64_t{0u} - static_cast<std::uint64_t>(value))
        : static_cast<std::uint64_t>(value);
}

// Stein's binary GCD: uses trailing-zero counts, shifts and subtraction only (zero / or %).
[[nodiscard]] inline constexpr std::uint64_t binaryGcd(std::uint64_t u, std::uint64_t v) noexcept {
    if (u == 0u) return v;
    if (v == 0u) return u;
    const auto commonShift = static_cast<unsigned>(std::countr_zero(u | v));
    u >>= static_cast<unsigned>(std::countr_zero(u));
    do {
        v >>= static_cast<unsigned>(std::countr_zero(v));
        if (u > v) std::swap(u, v);
        v -= u;
    } while (v != 0u);
    return u << commonShift;
}

[[nodiscard]] inline constexpr std::int64_t binaryGcdAbs(std::int64_t a, std::int64_t b) noexcept {
    const std::uint64_t g = binaryGcd(uAbsI64(a), uAbsI64(b));
    return g > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())
        ? std::int64_t{1}
        : static_cast<std::int64_t>(g);
}

[[nodiscard]] inline constexpr std::int64_t safePositiveScale(std::int64_t scale) noexcept {
    return scale <= 0 ? std::int64_t{1} : scale;
}

// Exact division by a positive GCD scale: every dividend is proved to be an
// integer multiple of `scale`. Decomposes `scale = odd << shift` and computes
// the exact multiplicative inverse of `odd` modulo 2^64 via 4 Newton-Raphson
// steps (zero runtime / or % in both preparation and per-element division).
struct ExactScaleDivisor final {
    std::uint64_t scale{1u};
    unsigned shift{0u};
    std::uint64_t oddInverse{1u};

    [[nodiscard]] static constexpr ExactScaleDivisor prepare(std::int64_t signedScale) noexcept {
        if (signedScale <= 1) return {};
        const auto raw = static_cast<std::uint64_t>(signedScale);
        const auto tz = static_cast<unsigned>(std::countr_zero(raw));
        const std::uint64_t odd = raw >> tz;
        std::uint64_t inv = (odd * 3u) ^ 2u; // exact 5-bit modular inverse
        inv *= 2u - odd * inv;               // 10 bits
        inv *= 2u - odd * inv;               // 20 bits
        inv *= 2u - odd * inv;               // 40 bits
        inv *= 2u - odd * inv;               // 64 bits
        return {raw, tz, inv};
    }

    [[nodiscard]] constexpr std::uint64_t divideUnsignedExact(std::uint64_t multiple) const noexcept {
        return (multiple >> shift) * oddInverse;
    }

    [[nodiscard]] constexpr std::int64_t divideSignedExact(std::int64_t multiple) const noexcept {
        if (multiple >= 0) {
            return static_cast<std::int64_t>(
                divideUnsignedExact(static_cast<std::uint64_t>(multiple)));
        }
        const std::uint64_t mag = uAbsI64(multiple);
        return static_cast<std::int64_t>(std::uint64_t{0u} - divideUnsignedExact(mag));
    }
};

// Compile-time ceil(2^32 / stride) table for 1 <= stride <= 1024.
// For any byteSize <= 256 * 1024 = 262,144 that is a multiple of stride,
// ((byteSize * table[stride]) >> 32) equals byteSize / stride exactly;
// non-multiples are rejected by `count * stride == byteSize` without / or %.
consteval auto makeStrideReciprocals() noexcept {
    std::array<std::uint64_t, kMaxSupportedRecordStride + 1u> table{};
    table[0] = 0u;
    table[1] = std::uint64_t{1u} << 32u;
    for (std::size_t d = 2u; d <= kMaxSupportedRecordStride; ++d) {
        table[d] = ((std::uint64_t{1u} << 32u) + d - 1u) / d;
    }
    return table;
}

inline constexpr auto kStrideReciprocals = makeStrideReciprocals();

// Computes exact record count for any stride in [1, 1024] when byteSize <= maxRecords * stride.
// For larger buffers (e.g. multi-block slices), prepares an ExactScaleDivisor (mod 2^64)
// and verifies `count * stride == byteSize` using 128-bit overflow-safe multiplication.
[[nodiscard]] inline constexpr bool exactRecordCount(
    std::size_t byteSize,
    std::uint32_t recordBytes,
    std::size_t& outCount) noexcept {
    outCount = 0u;
    if (byteSize == 0u || recordBytes == 0u || recordBytes > kMaxSupportedRecordStride) {
        return false;
    }
    const auto stride = static_cast<std::size_t>(recordBytes);
    if (byteSize <= kMaxSupportedRecordsPerBlock * stride) {
        const auto q = static_cast<std::size_t>(
            (static_cast<std::uint64_t>(byteSize) * kStrideReciprocals[stride]) >> 32u);
        if (q == 0u || q * stride != byteSize) return false;
        outCount = q;
        return true;
    }
    // General arbitrary-length slice: if byteSize is a multiple of stride,
    // low trailing zeros of byteSize must be >= trailing zeros of stride, and
    // modular inverse mod 2^64 recovers the unique candidate quotient q.
    const auto tz = static_cast<unsigned>(std::countr_zero(stride));
    if ((byteSize & ((std::size_t{1u} << tz) - 1u)) != 0u) return false;
    const auto div = ExactScaleDivisor::prepare(static_cast<std::int64_t>(recordBytes));
    const std::uint64_t q = div.divideUnsignedExact(static_cast<std::uint64_t>(byteSize));
    if (q == 0u ||
        static_cast<unsigned __int128>(q) * static_cast<unsigned __int128>(stride) !=
            static_cast<unsigned __int128>(byteSize)) {
        return false;
    }
    outCount = static_cast<std::size_t>(q);
    return true;
}

// Compile-time floor((2^64 - 1) / f) reciprocals for 1 <= f <= 4096.
// For ALL x in [0, 2^31) and ALL f in [1, 4096]:
//   q = static_cast<uint32_t>((static_cast<unsigned __int128>(x + 1u) * kRansFreqReciprocals[f]) >> 64u)
// equals floor(x / f) bit-exactly in a single 64x64->128 multiply with zero branches.
consteval auto makeRansFreqReciprocals() noexcept {
    std::array<std::uint64_t, kRansScale + 1u> table{};
    table[0] = 0u;
    for (std::size_t f = 1u; f <= kRansScale; ++f) {
        table[f] = std::numeric_limits<std::uint64_t>::max() / f;
    }
    return table;
}

inline constexpr auto kRansFreqReciprocals = makeRansFreqReciprocals();

// Division-free normalization of raw byte counts (sum == totalCount > 0) onto
// M = 4096 slots, guaranteeing freq[s] >= 1 iff rawCount[s] > 0 and sum(freq) == 4096.
// Uses Q30 Newton-Raphson reciprocal approximation of totalCount (shifts + multiplies only).
inline void normalizeByteCounts4096(
    const std::array<std::uint32_t, 256u>& rawCount,
    std::size_t totalCount,
    std::array<std::uint16_t, 256u>& freq,
    std::size_t& activeSymbols,
    std::uint8_t& singleSymbol) noexcept {
    freq.fill(0u);
    activeSymbols = 0u;
    singleSymbol = 0u;
    if (totalCount == 0u) return;

    std::size_t maxSym = 0u;
    std::uint32_t maxRaw = 0u;
    for (std::size_t s = 0u; s < 256u; ++s) {
        if (rawCount[s] != 0u) {
            ++activeSymbols;
            singleSymbol = static_cast<std::uint8_t>(s);
            if (rawCount[s] > maxRaw) {
                maxRaw = rawCount[s];
                maxSym = s;
            }
        }
    }
    if (activeSymbols == 1u) {
        freq[singleSymbol] = static_cast<std::uint16_t>(kRansScale);
        return;
    }

    // Normalize totalCount into [2^29, 2^30) in 64-bit fixed point and compute
    // 1 / d via 4 Q30 Newton-Raphson iterations (zero / or %).
    const auto total64 = static_cast<std::uint64_t>(totalCount);
    const auto lz = static_cast<unsigned>(std::countl_zero(total64));
    // Shift total64 so MSB is at bit 29: d in [2^29, 2^30).
    const std::uint64_t d = (lz >= 34u) ? (total64 << (lz - 34u)) : (total64 >> (34u - lz));
    // Initial approximation in Q30 for d/2^30 in [0.5, 1.0): x0 = 3 - 2*(d/2^30) -> in Q30: (3<<30) - (d<<1).
    std::uint64_t invQ30 = (std::uint64_t{3u} << 30u) - (d << 1u);
    for (unsigned iter = 0u; iter < 4u; ++iter) {
        const std::uint64_t prod = (d * invQ30) >> 30u;
        invQ30 = (invQ30 * ((std::uint64_t{2u} << 30u) - prod)) >> 30u;
    }

    // rawCount[s] * 4096 / totalCount:
    // Since d = total64 * 2^(lz - 34), 1/total64 = (invQ30 / 2^60) * 2^(lz - 34).
    // Multiplying by 4096 = 2^12 gives shift right by (60 - 12 - (lz - 34)) = 82 - lz.
    const unsigned rshift = 82u - lz;
    const std::uint64_t half = (rshift > 0u && rshift <= 63u) ? (std::uint64_t{1u} << (rshift - 1u)) : 0u;

    std::int32_t sum = 0;
    for (std::size_t s = 0u; s < 256u; ++s) {
        if (rawCount[s] == 0u) continue;
        const auto scaled = static_cast<unsigned __int128>(rawCount[s]) * static_cast<unsigned __int128>(invQ30);
        auto q = static_cast<std::uint32_t>((scaled + half) >> rshift);
        if (q == 0u) q = 1u;
        if (q > kRansScale - 1u) q = kRansScale - 1u;
        freq[s] = static_cast<std::uint16_t>(q);
        sum += static_cast<std::int32_t>(q);
    }

    std::int32_t diff = static_cast<std::int32_t>(kRansScale) - sum;
    if (diff > 0) {
        freq[maxSym] = static_cast<std::uint16_t>(freq[maxSym] + static_cast<std::uint16_t>(diff));
    } else if (diff < 0) {
        std::uint32_t need = static_cast<std::uint32_t>(-diff);
        if (freq[maxSym] > need) {
            freq[maxSym] = static_cast<std::uint16_t>(freq[maxSym] - static_cast<std::uint16_t>(need));
        } else {
            // Drain excess across symbols with freq > 1 in descending frequency order.
            while (need != 0u) {
                std::size_t best = 256u;
                std::uint16_t bestFreq = 1u;
                for (std::size_t s = 0u; s < 256u; ++s) {
                    if (freq[s] > bestFreq) {
                        bestFreq = freq[s];
                        best = s;
                    }
                }
                if (best == 256u) break;
                const std::uint16_t take = (bestFreq - 1u >= need)
                    ? static_cast<std::uint16_t>(need)
                    : static_cast<std::uint16_t>(bestFreq - 1u);
                freq[best] = static_cast<std::uint16_t>(freq[best] - take);
                need -= take;
            }
        }
    }
}

} // namespace hft_compressor::math

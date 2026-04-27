#include "timing.hpp"

#include <chrono>

#if defined(__i386__) || defined(__x86_64__) || defined(_M_IX86) || defined(_M_X64)
#include <immintrin.h>
#endif

namespace hft_compressor::timing {

std::uint64_t nowNs() noexcept {
    const auto now = std::chrono::steady_clock::now().time_since_epoch();
    return static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::nanoseconds>(now).count());
}

std::uint64_t readCycles() noexcept {
#if defined(__i386__) || defined(__x86_64__) || defined(_M_IX86) || defined(_M_X64)
    unsigned int aux = 0;
    return __rdtscp(&aux);
#else
    return nowNs();
#endif
}

}  // namespace hft_compressor::timing

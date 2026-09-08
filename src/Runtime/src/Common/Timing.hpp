#pragma once

#include <cstdint>

namespace hft_compressor::timing {

std::uint64_t readCycles() noexcept;
std::uint64_t nowNs() noexcept;

}  // namespace hft_compressor::timing


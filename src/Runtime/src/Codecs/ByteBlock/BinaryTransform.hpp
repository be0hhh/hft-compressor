#pragma once

#include <bit>
#include <cstdint>
#include <span>
#include <vector>

#include "hft_compressor/ByteBlock.hpp"

namespace hft_compressor::codecs::byte_block {

inline std::uint64_t word(std::span<const std::uint8_t> bytes, std::size_t offset,
                          unsigned size=8) noexcept {
    std::uint64_t value{};
    for (unsigned i=0;i<size;++i) value|=std::uint64_t{bytes[offset+i]}<<(8u*i);
    return value;
}
inline void put(std::span<std::uint8_t> bytes, std::size_t offset, std::uint64_t value,
                unsigned size=8) noexcept {
    for (unsigned i=0;i<size;++i) bytes[offset+i]=static_cast<std::uint8_t>(value>>(8u*i));
}
// Modular differences retain the entire unsigned/signed integer domain without
// signed subtraction overflow, including INT64_MIN and regressing clocks.
inline std::uint64_t zigzagBits(std::uint64_t value) noexcept {
    return (value<<1u)^(std::uint64_t{0}-(value>>63u));
}
inline std::uint64_t unzigzagBits(std::uint64_t value) noexcept {
    return (value>>1u)^(std::uint64_t{0}-(value&1u));
}
void varint(std::vector<std::uint8_t>& bytes, std::uint64_t value);
struct Reader {
    std::span<const std::uint8_t> bytes;
    bool byte(std::uint8_t& out) noexcept {
        if (bytes.empty()) return false;
        out=bytes.front(); bytes=bytes.subspan(1); return true;
    }
    bool varint(std::uint64_t& out) noexcept {
        out=0;
        for (unsigned shift=0;shift<=63;shift+=7) {
            std::uint8_t value{};
            if (!byte(value) || (shift==63 && (value&0xfeu))) return false;
            out|=std::uint64_t{value&0x7fu}<<shift;
            if (!(value&0x80u)) return shift==0 || value!=0;
        }
        return false;
    }
};

void encodeTrades(const ByteBlockLayout&, std::span<std::uint8_t> records,
                  std::vector<std::uint8_t>& columns);
bool decodeTrades(const ByteBlockLayout&, std::span<std::uint8_t> records, Reader& columns);
void encodeBookTicker(const ByteBlockLayout&, std::span<std::uint8_t> records,
                      std::vector<std::uint8_t>& columns);
bool decodeBookTicker(const ByteBlockLayout&, std::span<std::uint8_t> records, Reader& columns);
void encodeDepth(const ByteBlockLayout&, std::span<std::uint8_t> records,
                 std::vector<std::uint8_t>& columns);
bool decodeDepth(const ByteBlockLayout&, std::span<std::uint8_t> records, Reader& columns);

} // namespace hft_compressor::codecs::byte_block

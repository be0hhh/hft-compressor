#include "../ByteBlock/BinaryTransform.hpp"

#include <array>

namespace hft_compressor::codecs::byte_block {
void encodeBookTicker(const ByteBlockLayout& layout, std::span<std::uint8_t> records,
                      std::vector<std::uint8_t>& columns) {
    std::array<std::uint64_t,4> previous{};
    for (std::size_t start=0;start<records.size();start+=layout.recordBytes) {
        const auto offset=start+layout.payloadOffset;
        const auto bid=word(records,offset);
        const std::array<std::uint64_t,4> state{
            bid,word(records,offset+16)-bid,word(records,offset+8),word(records,offset+24)};
        std::uint8_t mask{};
        for (unsigned i=0;i<4;++i) if (state[i]!=previous[i]) mask|=1u<<i;
        columns.push_back(mask);
        for (unsigned i=0;i<4;++i) {
            if (mask&(1u<<i)) varint(columns,zigzagBits(state[i]-previous[i]));
            previous[i]=state[i]; put(records,offset+8*i,0);
        }
    }
}
bool decodeBookTicker(const ByteBlockLayout& layout, std::span<std::uint8_t> records, Reader& columns) {
    std::array<std::uint64_t,4> state{};
    for (std::size_t start=0;start<records.size();start+=layout.recordBytes) {
        const auto offset=start+layout.payloadOffset;
        std::uint8_t mask{};
        if (!columns.byte(mask) || mask>15) return false;
        for (unsigned i=0;i<4;++i) {
            if (word(records,offset+8*i)!=0) return false;
            if (mask&(1u<<i)) {
                std::uint64_t delta{};
                if (!columns.varint(delta) || delta==0) return false;
                state[i]+=unzigzagBits(delta);
            }
        }
        put(records,offset,state[0]); put(records,offset+8,state[2]);
        put(records,offset+16,state[0]+state[1]); put(records,offset+24,state[3]);
    }
    return columns.bytes.empty();
}
} // namespace hft_compressor::codecs::byte_block

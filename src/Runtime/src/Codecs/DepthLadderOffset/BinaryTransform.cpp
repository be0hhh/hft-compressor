#include "../ByteBlock/BinaryTransform.hpp"

#include <array>

namespace hft_compressor::codecs::byte_block {
namespace {
struct Ladder {
    std::array<std::uint64_t,2> price{};
    std::array<bool,2> have{};
    std::uint64_t anchor(std::uint8_t side) const noexcept {
        return side<2 && have[side] ? price[side] : 0;
    }
    void level(std::uint8_t side,std::uint64_t value,std::uint64_t qty) noexcept {
        if (side>=2 || qty==0) return;
        if (!have[side] || (side==0 ? value>price[side] : value<price[side])) price[side]=value;
        have[side]=true;
    }
};
}
void encodeDepth(const ByteBlockLayout& layout, std::span<std::uint8_t> records,
                 std::vector<std::uint8_t>& columns) {
    Ladder previous{};
    for (std::size_t start=0;start<records.size();start+=layout.recordBytes) {
        Ladder next{};
        const auto count=word(records,start+layout.depthLevelCountOffset,2);
        for (std::size_t i=0;i<count;++i) {
            const auto offset=start+layout.depthLevelsOffset+i*layout.depthLevelStride;
            const auto price=word(records,offset),qty=word(records,offset+8);
            const auto side=records[offset+16];
            varint(columns,zigzagBits(price-previous.anchor(side)));
            varint(columns,qty);
            next.level(side,price,qty);
            put(records,offset,0); put(records,offset+8,0);
        }
        previous=next;
    }
}
bool decodeDepth(const ByteBlockLayout& layout, std::span<std::uint8_t> records, Reader& columns) {
    Ladder previous{};
    for (std::size_t start=0;start<records.size();start+=layout.recordBytes) {
        Ladder next{};
        const auto count=word(records,start+layout.depthLevelCountOffset,2);
        if (count>layout.depthLevelCapacity) return false;
        for (std::size_t i=0;i<count;++i) {
            const auto offset=start+layout.depthLevelsOffset+i*layout.depthLevelStride;
            if (word(records,offset)!=0 || word(records,offset+8)!=0) return false;
            std::uint64_t delta{},qty{};
            if (!columns.varint(delta) || !columns.varint(qty)) return false;
            const auto side=records[offset+16];
            const auto price=previous.anchor(side)+unzigzagBits(delta);
            put(records,offset,price); put(records,offset+8,qty);
            next.level(side,price,qty);
        }
        previous=next;
    }
    return columns.bytes.empty();
}
} // namespace hft_compressor::codecs::byte_block

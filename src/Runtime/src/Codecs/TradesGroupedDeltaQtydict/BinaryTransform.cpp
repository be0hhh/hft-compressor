#include "TradesGroupedDeltaQtyDictInternal.hpp"
#include "../ByteBlock/BinaryTransform.hpp"

#include <algorithm>
#include <array>
#include <utility>

namespace hft_compressor::codecs::byte_block {
namespace trades = trades_grouped_delta_qtydict::codec_detail;

void varint(std::vector<std::uint8_t>& bytes, std::uint64_t value) {
    trades::writeVarint(bytes,value);
}

void encodeTrades(const ByteBlockLayout& layout, std::span<std::uint8_t> records,
                  std::vector<std::uint8_t>& columns) {
    const auto count=records.size()/layout.recordBytes;
    std::array<trades::Trade,kByteBlockMaxRecords> rows{};
    for (std::size_t i=0;i<count;++i) {
        const auto offset=i*layout.recordBytes+layout.payloadOffset;
        rows[i]={std::bit_cast<std::int64_t>(word(records,offset)),
                        std::bit_cast<std::int64_t>(word(records,offset+8)),
                        records[offset+16],
                        std::bit_cast<std::int64_t>(word(records,i*layout.recordBytes+layout.timestampOffset))};
    }
    // Same channel-owned frequency ranking and signed-value tie break as
    // buildHotQty(false), with a fixed array for the 256-record byte block.
    std::array<std::pair<std::int64_t,std::uint32_t>,kByteBlockMaxRecords> quantities{};
    std::size_t unique{};
    for (std::size_t i=0;i<count;++i) {
        std::size_t index{};
        while (index<unique && quantities[index].first!=rows[i].qty) ++index;
        if (index==unique) quantities[unique++]={rows[i].qty,0};
        ++quantities[index].second;
    }
    std::sort(quantities.begin(),quantities.begin()+unique,[](const auto& a,const auto& b) {
        return a.second!=b.second ? a.second>b.second : a.first<b.first;
    });
    const auto hotCount=std::min<std::size_t>(unique,trades::kHotQtyCount);
    varint(columns,hotCount);
    for (std::size_t i=0;i<hotCount;++i)
        varint(columns,std::bit_cast<std::uint64_t>(quantities[i].first));
    std::uint64_t previousPrice{};
    for (std::size_t start=0;start<count;) {
        auto end=start+1;
        while (end<count && rows[end].tsNs==rows[start].tsNs &&
               rows[end].price==rows[start].price && rows[end].side==rows[start].side) ++end;
        varint(columns,end-start);
        const auto price=std::bit_cast<std::uint64_t>(rows[start].price);
        varint(columns,zigzagBits(price-previousPrice)); previousPrice=price;
        for (auto row=start;row<end;++row) {
            std::size_t code{};
            while (code<hotCount && quantities[code].first!=rows[row].qty) ++code;
            if (code==hotCount) code=trades::kHotQtyCount;
            varint(columns,code);
            if (code==trades::kHotQtyCount)
                varint(columns,std::bit_cast<std::uint64_t>(rows[row].qty));
            const auto offset=row*layout.recordBytes+layout.payloadOffset;
            put(records,offset,0); put(records,offset+8,0);
        }
        start=end;
    }
}

bool decodeTrades(const ByteBlockLayout& layout, std::span<std::uint8_t> records, Reader& columns) {
    const auto count=records.size()/layout.recordBytes;
    std::array<std::uint64_t,64> hot{};
    std::uint64_t hotCount{};
    if (!columns.varint(hotCount) || hotCount>hot.size()) return false;
    for (std::size_t i=0;i<hotCount;++i) {
        if (!columns.varint(hot[i])) return false;
        for (std::size_t j=0;j<i;++j) if (hot[i]==hot[j]) return false;
    }
    std::uint64_t previousPrice{};
    for (std::size_t start=0;start<count;) {
        std::uint64_t run{},delta{};
        if (!columns.varint(run) || run==0 || run>count-start || !columns.varint(delta)) return false;
        const auto price=previousPrice+unzigzagBits(delta); previousPrice=price;
        const auto timestamp=word(records,start*layout.recordBytes+layout.timestampOffset);
        const auto side=records[start*layout.recordBytes+layout.payloadOffset+16];
        for (std::size_t row=start;row<start+run;++row) {
            const auto offset=row*layout.recordBytes+layout.payloadOffset;
            if (word(records,offset)!=0 || word(records,offset+8)!=0 ||
                records[offset+16]!=side || word(records,row*layout.recordBytes+layout.timestampOffset)!=timestamp)
                return false;
            std::uint64_t code{},qty{};
            if (!columns.varint(code)) return false;
            if (code<hotCount) qty=hot[code];
            else if (code!=64 || !columns.varint(qty)) return false;
            put(records,offset,price); put(records,offset+8,qty);
        }
        start+=static_cast<std::size_t>(run);
    }
    return columns.bytes.empty();
}
} // namespace hft_compressor::codecs::byte_block

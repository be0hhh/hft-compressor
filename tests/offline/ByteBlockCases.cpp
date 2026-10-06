#include "OfflineCase.hpp"
#include "hft_compressor/ByteBlock.hpp"
#include "../../src/Runtime/src/Container/Hfc/Format.hpp"

#include <algorithm>
#include <array>
#include <limits>
#include <vector>

namespace byteblock_cases {
using namespace hft_compressor;

namespace {
ByteBlockLayout layout(StreamType type) {
    ByteBlockLayout out{};
    out.streamType=type; out.recordBytes=960; out.payloadOffset=128;
    out.timestampOffset=56;
    if (type==StreamType::Depth) {
        out.depthLevelsOffset=192; out.depthLevelStride=24;
        out.depthLevelCountOffset=164; out.depthLevelCapacity=32;
    }
    return out;
}
void put(std::vector<std::uint8_t>& bytes, std::size_t offset, std::uint64_t value) {
    for (unsigned i=0;i<8;++i) bytes.at(offset+i)=static_cast<std::uint8_t>(value>>(8*i));
}
void putSmall(std::vector<std::uint8_t>& bytes,std::size_t offset,unsigned count,std::uint64_t value) {
    for (unsigned i=0;i<count;++i) bytes.at(offset+i)=static_cast<std::uint8_t>(value>>(8*i));
}
void repairHeader(std::vector<std::uint8_t>& block) {
    putSmall(block,40,4,0);
    putSmall(block,40,4,format::crc32c(std::span{block}.first(kByteBlockHeaderBytes)));
}
std::vector<std::uint8_t> fixture(const ByteBlockLayout& shape, std::size_t count) {
    std::vector<std::uint8_t> raw(count*shape.recordBytes);
    for (std::size_t row=0;row<count;++row) {
        const auto start=row*shape.recordBytes;
        for (std::size_t i=0;i<shape.recordBytes;++i)
            raw[start+i]=static_cast<std::uint8_t>((i*17u+row*3u)%251u);
        // Negative, regressing and full-range clocks must remain bit-exact.
        constexpr std::uint64_t clocks[]{0,100,90,~std::uint64_t{0},std::uint64_t{1}<<63};
        put(raw,start+56,clocks[row%5]);
        put(raw,start+64,clocks[(row+2)%5]);
        if (shape.streamType==StreamType::Depth) {
            raw[start+164]=3; raw[start+165]=0;
            for (std::size_t level=0;level<3;++level) {
                const auto offset=start+192+level*24;
                put(raw,offset,(std::uint64_t{1}<<63)+row-level);
                put(raw,offset+8,level==1 ? 0 : 200+row);
                raw[offset+16]=static_cast<std::uint8_t>(level%2);
                raw[offset+17]=static_cast<std::uint8_t>(level%3);
            }
        } else if (shape.streamType!=StreamType::Unknown) {
            put(raw,start+128,(std::uint64_t{1}<<63)+row);
            put(raw,start+136,row%3 ? 42 : ~std::uint64_t{0});
            put(raw,start+144,row%2);
            if (shape.streamType==StreamType::BookTicker) {
                put(raw,start+144,(std::uint64_t{1}<<63)+row+10);
                put(raw,start+152,row%4);
            }
        }
    }
    return raw;
}
std::vector<std::uint8_t> encode(const ByteBlockLayout& shape,const std::vector<std::uint8_t>& raw) {
    const auto bound=byteBlockEncodeBound(shape,raw.size()/shape.recordBytes);
    CXET_CHECK(bound>=raw.size());
    std::vector<std::uint8_t> out(bound);
    std::size_t count=999;
    CXET_CHECK(encodeByteBlock(shape,raw,out,count)==Status::Ok);
    CXET_CHECK(count<=bound && count>=kByteBlockHeaderBytes);
    out.resize(count); return out;
}
}

void fullRecordBytesAndRegressingClocksRoundtrip() {
    for (auto type:{StreamType::Trades,StreamType::BookTicker,StreamType::Depth,StreamType::Unknown}) {
        const auto shape=layout(type); const auto raw=fixture(shape,19);
        const auto block=encode(shape,raw);
        std::vector<std::uint8_t> decoded(raw.size(),0x5a);
        std::size_t obtained=999;
        CXET_CHECK(decodeByteBlock(shape,block,decoded,obtained)==Status::Ok);
        CXET_CHECK(obtained==raw.size() && decoded==raw);
    }
}
void blocksResetWithoutPreviousState() {
    const auto shape=layout(StreamType::Trades);
    auto raw=fixture(shape,256); const auto first=encode(shape,raw);
    std::reverse(raw.begin(),raw.end()); const auto unrelated=encode(shape,raw);
    static_cast<void>(unrelated);
    raw=fixture(shape,256); const auto repeated=encode(shape,raw);
    CXET_CHECK(first==repeated);
    std::vector<std::uint8_t> decoded(raw.size()); std::size_t bytes{};
    CXET_CHECK(decodeByteBlock(shape,first,decoded,bytes)==Status::Ok && decoded==raw);
}
void corruptAndTruncatedBlocksNeverPublish() {
    const auto shape=layout(StreamType::BookTicker); const auto raw=fixture(shape,8);
    const auto block=encode(shape,raw);
    std::vector<std::uint8_t> output(raw.size(),0x5a); const auto before=output;
    for (std::size_t i=0;i<block.size();++i) {
        auto changed=block; changed[i]^=0x80; std::size_t bytes=999;
        CXET_CHECK(decodeByteBlock(shape,changed,output,bytes)==Status::CorruptData);
        CXET_CHECK(bytes==0 && output==before);
    }
    for (std::size_t count:{std::size_t{0},std::size_t{63},block.size()-1}) {
        std::size_t bytes=999;
        CXET_CHECK(decodeByteBlock(shape,std::span{block}.first(count),output,bytes)==Status::CorruptData);
        CXET_CHECK(bytes==0 && output==before);
    }
    for (auto field:std::array<std::array<std::uint64_t,3>,5>{{
            {4,2,99},{8,1,99},{10,2,257},{16,4,0xffffffffu},{20,4,0xffffffffu}}}) {
        auto forged=block; putSmall(forged,field[0],field[1],field[2]); repairHeader(forged);
        std::size_t bytes=999;
        CXET_CHECK(decodeByteBlock(shape,forged,output,bytes)==Status::CorruptData);
        CXET_CHECK(bytes==0 && output==before);
    }
    // CRC-valid entropy truncation must still fail its exact final-bit check.
    std::vector<std::uint8_t> zeros(256*shape.recordBytes);
    auto entropyBlock=encode(shape,zeros);
    CXET_CHECK(entropyBlock[8]==2);
    entropyBlock.pop_back();
    putSmall(entropyBlock,24,4,entropyBlock.size()-kByteBlockHeaderBytes);
    putSmall(entropyBlock,32,4,format::crc32c(std::span{entropyBlock}.subspan(kByteBlockHeaderBytes)));
    repairHeader(entropyBlock);
    output.assign(zeros.size(),0x5a); const auto untouched=output; std::size_t bytes=999;
    CXET_CHECK(decodeByteBlock(shape,entropyBlock,output,bytes)==Status::CorruptData);
    CXET_CHECK(bytes==0 && output==untouched);
}
void boundsAndLayoutFailClosed() {
    auto shape=layout(StreamType::Trades);
    CXET_CHECK(byteBlockEncodeBound(shape,0)==0 && byteBlockEncodeBound(shape,257)==0);
    const auto raw=fixture(shape,1); auto block=encode(shape,raw);
    std::vector<std::uint8_t> shortOutput(raw.size()-1,0x5a);
    const auto before=shortOutput; std::size_t bytes=999;
    CXET_CHECK(decodeByteBlock(shape,block,shortOutput,bytes)==Status::InvalidArgument);
    CXET_CHECK(bytes==0 && shortOutput==before);
    std::vector<std::uint8_t> destination(byteBlockEncodeBound(shape,1)-1,0x5a);
    const auto saved=destination;
    CXET_CHECK(encodeByteBlock(shape,raw,destination,bytes)==Status::InvalidArgument);
    CXET_CHECK(bytes==0 && destination==saved);
    shape.payloadOffset=959;
    CXET_CHECK(byteBlockEncodeBound(shape,1)==0);
    shape=layout(StreamType::Depth); shape.depthLevelCapacity=33;
    CXET_CHECK(byteBlockEncodeBound(shape,1)==0);
    shape=layout(StreamType::Trades); shape.timestampOffset=57;
    std::vector<std::uint8_t> output(raw.size(),0x5a);
    CXET_CHECK(decodeByteBlock(shape,block,output,bytes)==Status::CorruptData && bytes==0);
    shape=layout(StreamType::Trades);
    const auto outputBefore=output;
    CXET_CHECK(encodeByteBlock(shape,std::span{raw}.first(raw.size()-1),output,bytes)==Status::InvalidArgument);
    CXET_CHECK(bytes==0 && output==outputBefore);
    auto overlapping=block;
    overlapping.resize(std::max(overlapping.size(),raw.size()),0x5a);
    const auto overlapBefore=overlapping;
    CXET_CHECK(decodeByteBlock(shape,std::span{overlapping}.first(block.size()),std::span{overlapping}.first(raw.size()),bytes)==Status::InvalidArgument);
    CXET_CHECK(bytes==0 && overlapping==overlapBefore);
    shape=layout(StreamType::Depth);
    auto invalidDepth=fixture(shape,1); invalidDepth[164]=33;
    std::vector<std::uint8_t> depthOutput(byteBlockEncodeBound(shape,1),0x5a);
    const auto depthBefore=depthOutput;
    CXET_CHECK(encodeByteBlock(shape,invalidDepth,depthOutput,bytes)==Status::InvalidArgument);
    CXET_CHECK(bytes==0 && depthOutput==depthBefore);
}
void repetitiveBlocksUseCompression() {
    const auto shape=layout(StreamType::BookTicker);
    std::vector<std::uint8_t> raw(256*960);
    const auto block=encode(shape,raw);
    CXET_CHECK(block.size()<raw.size()/8);
    std::vector<std::uint8_t> output(raw.size(),0xff); std::size_t bytes{};
    CXET_CHECK(decodeByteBlock(shape,block,output,bytes)==Status::Ok && output==raw);
}
void extremeOpaqueBytesRespectWorstCaseBound() {
    const auto shape=layout(StreamType::Unknown); std::vector<std::uint8_t> raw(256*960);
    std::uint32_t state=0x12345678;
    for (auto& byte:raw) { state^=state<<13; state^=state>>17; state^=state<<5; byte=state; }
    const auto block=encode(shape,raw);
    CXET_CHECK(block.size()<=byteBlockEncodeBound(shape,256));
    std::vector<std::uint8_t> output(raw.size()); std::size_t bytes{};
    CXET_CHECK(decodeByteBlock(shape,block,output,bytes)==Status::Ok && output==raw);
}
} // namespace byteblock_cases

#include "hft_compressor/ByteBlock.hpp"
#include "BinaryTransform.hpp"
#include "../EntropyHftmac/EntropyHftMacInternal.hpp"
#include "../../Container/Hfc/Format.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <vector>

namespace hft_compressor {
namespace {
namespace binary=codecs::byte_block;
namespace entropy=codecs::entropy_hftmac::detail;
constexpr std::uint32_t kMagic=0x31434248u; // HBC1
enum class Storage : std::uint8_t { Opaque=0, Transform=1, Arithmetic=2 };

bool fits(std::size_t offset,std::size_t count,std::size_t bytes) noexcept {
    return offset<=bytes && count<=bytes-offset;
}
bool valid(const ByteBlockLayout& l) noexcept {
    if (l.recordBytes==0 || l.recordBytes>kByteBlockMaxRecordBytes || l.payloadOffset>l.recordBytes) return false;
    const bool noDepth=l.depthLevelsOffset==0 && l.depthLevelStride==0 &&
                       l.depthLevelCountOffset==0 && l.depthLevelCapacity==0;
    switch (l.streamType) {
    case StreamType::Unknown: return noDepth;
    case StreamType::Trades:
        return noDepth && fits(l.payloadOffset,17,l.recordBytes) && fits(l.timestampOffset,8,l.recordBytes) &&
               (l.timestampOffset+8<=l.payloadOffset || l.timestampOffset>=l.payloadOffset+17);
    case StreamType::BookTicker: return noDepth && fits(l.payloadOffset,32,l.recordBytes);
    case StreamType::Depth:
        return l.depthLevelCapacity>0 && l.depthLevelCapacity<=kByteBlockMaxDepthLevels &&
               l.depthLevelStride>=18 && l.depthLevelsOffset>=l.payloadOffset &&
               fits(l.depthLevelCountOffset,2,l.recordBytes) &&
               l.depthLevelCountOffset+2<=l.depthLevelsOffset &&
               fits(l.depthLevelsOffset,std::size_t{l.depthLevelStride}*l.depthLevelCapacity,l.recordBytes);
    }
    return false;
}
bool validRecords(const ByteBlockLayout& l,std::span<const std::uint8_t> records) noexcept {
    if (l.streamType!=StreamType::Depth) return true;
    for (std::size_t offset=0;offset<records.size();offset+=l.recordBytes)
        if (binary::word(records,offset+l.depthLevelCountOffset,2)>l.depthLevelCapacity) return false;
    return true;
}
std::uint32_t layoutCrc(const ByteBlockLayout& l) noexcept {
    const std::array<std::uint32_t,8> values{static_cast<std::uint32_t>(l.streamType),l.recordBytes,
        l.payloadOffset,l.timestampOffset,l.depthLevelsOffset,l.depthLevelStride,
        l.depthLevelCountOffset,l.depthLevelCapacity};
    std::array<std::uint8_t,32> bytes{};
    for (std::size_t i=0;i<values.size();++i) binary::put(bytes,i*4,values[i],4);
    return format::crc32c(bytes);
}
bool overlap(std::span<const std::uint8_t> input,std::span<std::uint8_t> output) noexcept {
    if (input.empty() || output.empty()) return false;
    const auto a=reinterpret_cast<std::uintptr_t>(input.data());
    const auto b=reinterpret_cast<std::uintptr_t>(output.data());
    return a<=b ? b-a<input.size() : a-b<output.size();
}
std::size_t transformBound(std::size_t rawBytes) noexcept { return rawBytes*3+1024; }

std::vector<std::uint8_t> transform(const ByteBlockLayout& l,std::span<const std::uint8_t> input) {
    std::vector<std::uint8_t> records(input.begin(),input.end());
    std::vector<std::uint8_t> columns;
    // At most 20 varint bytes per depth level, 64 dictionary words for
    // trades, and 41 bytes per BBO. Every source sequence is block-bounded.
    columns.reserve(transformBound(input.size())-input.size());
    switch (l.streamType) {
    case StreamType::Trades: binary::encodeTrades(l,records,columns); break;
    case StreamType::BookTicker: binary::encodeBookTicker(l,records,columns); break;
    case StreamType::Depth: binary::encodeDepth(l,records,columns); break;
    case StreamType::Unknown: break;
    }
    std::vector<std::uint8_t> result;
    result.reserve(transformBound(input.size()));
    if (l.streamType!=StreamType::Unknown) {
        result.resize(4); binary::put(result,0,columns.size(),4);
        result.insert(result.end(),columns.begin(),columns.end());
    }
    // Plane-wise modular byte deltas retain metadata and all inactive/padding
    // bytes. Numeric channel fields have been moved into owned columns.
    const auto count=records.size()/l.recordBytes;
    for (std::size_t column=0;column<l.recordBytes;++column) {
        std::uint8_t previous{};
        for (std::size_t row=0;row<count;++row) {
            const auto value=records[row*l.recordBytes+column];
            result.push_back(static_cast<std::uint8_t>(value-previous)); previous=value;
        }
    }
    return result;
}
bool restore(const ByteBlockLayout& l,std::span<const std::uint8_t> base,std::span<std::uint8_t> records) {
    std::span<const std::uint8_t> columns;
    if (l.streamType!=StreamType::Unknown) {
        if (base.size()<4) return false;
        const auto bytes=static_cast<std::size_t>(binary::word(base,0,4));
        if (!fits(4,bytes,base.size())) return false;
        columns=base.subspan(4,bytes); base=base.subspan(4+bytes);
    }
    if (base.size()!=records.size()) return false;
    const auto count=records.size()/l.recordBytes;
    for (std::size_t column=0;column<l.recordBytes;++column) {
        std::uint8_t previous{};
        for (std::size_t row=0;row<count;++row) {
            previous=static_cast<std::uint8_t>(previous+base[column*count+row]);
            records[row*l.recordBytes+column]=previous;
        }
    }
    if (!validRecords(l,records)) return false;
    binary::Reader reader{columns};
    switch (l.streamType) {
    case StreamType::Trades: return binary::decodeTrades(l,records,reader);
    case StreamType::BookTicker: return binary::decodeBookTicker(l,records,reader);
    case StreamType::Depth: return binary::decodeDepth(l,records,reader);
    case StreamType::Unknown: return true;
    }
    return false;
}
}

std::size_t byteBlockEncodeBound(const ByteBlockLayout& layout,std::size_t count) noexcept {
    return valid(layout) && count>0 && count<=kByteBlockMaxRecords
        ? kByteBlockHeaderBytes+count*layout.recordBytes : 0;
}
Status encodeByteBlock(const ByteBlockLayout& l,std::span<const std::uint8_t> records,
                      std::span<std::uint8_t> encoded,std::size_t& bytes) noexcept {
    bytes=0;
    if (!valid(l) || records.empty() || records.size()%l.recordBytes || overlap(records,encoded))
        return Status::InvalidArgument;
    const auto count=records.size()/l.recordBytes;
    const auto bound=byteBlockEncodeBound(l,count);
    if (!bound || encoded.size()<bound || !validRecords(l,records)) return Status::InvalidArgument;
    try {
        auto base=transform(l,records);
        if (base.size()>transformBound(records.size())) return Status::InvalidArgument;
        Storage storage=Storage::Opaque;
        auto payload=records;
        if (base.size()<=records.size()) { storage=Storage::Transform; payload=base; }
        std::vector<std::uint8_t> arithmetic;
        if (entropy::arithmeticEncodeBounded(base,entropy::EntropyKind::Ac16Ctx8,payload.size()-1,arithmetic)) {
            storage=Storage::Arithmetic; payload=arithmetic;
        }
        std::array<std::uint8_t,kByteBlockHeaderBytes> header{};
        binary::put(header,0,kMagic,4); binary::put(header,4,kByteBlockSchemaVersion,2);
        binary::put(header,6,kByteBlockCodec,2); header[8]=static_cast<std::uint8_t>(storage);
        header[9]=static_cast<std::uint8_t>(l.streamType); binary::put(header,10,count,2);
        binary::put(header,12,l.recordBytes,4); binary::put(header,16,records.size(),4);
        binary::put(header,20,storage==Storage::Opaque ? records.size() : base.size(),4);
        binary::put(header,24,payload.size(),4); binary::put(header,28,format::crc32c(records),4);
        binary::put(header,32,format::crc32c(payload),4); binary::put(header,36,layoutCrc(l),4);
        binary::put(header,40,format::crc32c(header),4);
        std::copy(header.begin(),header.end(),encoded.begin());
        std::copy(payload.begin(),payload.end(),encoded.begin()+kByteBlockHeaderBytes);
        bytes=kByteBlockHeaderBytes+payload.size(); return Status::Ok;
    } catch (...) { return Status::DecodeError; }
}
Status decodeByteBlock(const ByteBlockLayout& l,std::span<const std::uint8_t> encoded,
                      std::span<std::uint8_t> records,std::size_t& bytes) noexcept {
    bytes=0;
    if (!valid(l) || overlap(encoded,records)) return Status::InvalidArgument;
    if (encoded.size()<kByteBlockHeaderBytes) return Status::CorruptData;
    std::array<std::uint8_t,kByteBlockHeaderBytes> header{};
    std::copy_n(encoded.begin(),header.size(),header.begin());
    const auto crc=binary::word(header,40,4); binary::put(header,40,0,4);
    if (format::crc32c(header)!=crc || binary::word(header,0,4)!=kMagic ||
        binary::word(header,4,2)!=kByteBlockSchemaVersion || binary::word(header,6,2)!=kByteBlockCodec ||
        header[8]>static_cast<std::uint8_t>(Storage::Arithmetic) || header[9]!=static_cast<std::uint8_t>(l.streamType) ||
        binary::word(header,12,4)!=l.recordBytes || binary::word(header,36,4)!=layoutCrc(l) ||
        !std::all_of(header.begin()+44,header.end(),[](auto byte){return byte==0;})) return Status::CorruptData;
    const auto count=static_cast<std::size_t>(binary::word(header,10,2));
    const auto rawBytes=static_cast<std::size_t>(binary::word(header,16,4));
    const auto baseBytes=static_cast<std::size_t>(binary::word(header,20,4));
    const auto payloadBytes=static_cast<std::size_t>(binary::word(header,24,4));
    const auto bound=byteBlockEncodeBound(l,count);
    if (!bound || rawBytes!=count*l.recordBytes || baseBytes==0 || baseBytes>transformBound(rawBytes) ||
        payloadBytes==0 || payloadBytes>rawBytes || encoded.size()!=kByteBlockHeaderBytes+payloadBytes)
        return Status::CorruptData;
    const auto storage=static_cast<Storage>(header[8]);
    if ((storage==Storage::Opaque && (baseBytes!=rawBytes || payloadBytes!=rawBytes)) ||
        (storage==Storage::Transform && baseBytes!=payloadBytes)) return Status::CorruptData;
    if (records.size()<rawBytes) return Status::InvalidArgument;
    const auto payload=encoded.subspan(kByteBlockHeaderBytes);
    if (format::crc32c(payload)!=binary::word(header,32,4)) return Status::CorruptData;
    try {
        std::vector<std::uint8_t> raw(rawBytes);
        if (storage==Storage::Opaque) std::copy(payload.begin(),payload.end(),raw.begin());
        else {
            std::vector<std::uint8_t> base(baseBytes);
            if (storage==Storage::Arithmetic) {
                if (!entropy::arithmeticDecodeBytes(payload,entropy::EntropyKind::Ac16Ctx8,base)) return Status::CorruptData;
            } else std::copy(payload.begin(),payload.end(),base.begin());
            if (!restore(l,base,raw)) return Status::CorruptData;
        }
        if (!validRecords(l,raw) || format::crc32c(raw)!=binary::word(header,28,4)) return Status::CorruptData;
        std::copy(raw.begin(),raw.end(),records.begin()); bytes=rawBytes; return Status::Ok;
    } catch (...) { return Status::DecodeError; }
}
} // namespace hft_compressor

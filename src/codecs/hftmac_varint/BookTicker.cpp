#include "codecs/hftmac_varint/HftMacVarint.hpp"

#include "codecs/hftmac_common/HftMacCommon.hpp"

namespace hft_compressor::codecs::hftmac_varint {
namespace {

constexpr hftmac_common::BackendSpec kSpec{StreamType::BookTicker, hftmac_common::PayloadCodec::PlainVarint, "hftmac.bookticker.varint.v1"};

}  // namespace

CompressionResult compressBookTickerVarint(const CompressionRequest& request, const PipelineDescriptor& pipeline) noexcept {
    return hftmac_common::compressVarint(request, pipeline, kSpec);
}

ReplayArtifactInfo inspectBookTickerVarint(const std::filesystem::path& path, const PipelineDescriptor& pipeline) noexcept {
    return hftmac_common::inspectVarintArtifact(path, pipeline, kSpec);
}

Status decodeBookTickerVarintFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept {
    return hftmac_common::decodeVarintFile(path, onBlock, kSpec);
}

Status decodeBookTickerVarint(std::span<const std::uint8_t> bytes, const DecodedBlockCallback& onBlock) noexcept {
    return hftmac_common::decodeVarintBuffer(bytes, onBlock, kSpec);
}

}  // namespace hft_compressor::codecs::hftmac_varint

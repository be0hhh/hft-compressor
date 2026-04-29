#include "codecs/hftmac_varint/HftMacVarint.hpp"

#include "codecs/hftmac_common/HftMacCommon.hpp"

namespace hft_compressor::codecs::hftmac_varint {
namespace {

constexpr hftmac_common::BackendSpec kSpec{StreamType::Trades, hftmac_common::PayloadCodec::PlainVarint, "hftmac.trade.varint.v1"};

}  // namespace

CompressionResult compressTradeVarint(const CompressionRequest& request, const PipelineDescriptor& pipeline) noexcept {
    return hftmac_common::compressVarint(request, pipeline, kSpec);
}

ReplayArtifactInfo inspectTradeVarint(const std::filesystem::path& path, const PipelineDescriptor& pipeline) noexcept {
    return hftmac_common::inspectVarintArtifact(path, pipeline, kSpec);
}

Status decodeTradeVarintFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept {
    return hftmac_common::decodeVarintFile(path, onBlock, kSpec);
}

Status decodeTradeVarint(std::span<const std::uint8_t> bytes, const DecodedBlockCallback& onBlock) noexcept {
    return hftmac_common::decodeVarintBuffer(bytes, onBlock, kSpec);
}

}  // namespace hft_compressor::codecs::hftmac_varint

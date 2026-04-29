#include "codecs/hftmac_varint/HftMacVarint.hpp"

#include "codecs/hftmac_common/HftMacCommon.hpp"

namespace hft_compressor::codecs::hftmac_varint {
namespace {

constexpr hftmac_common::BackendSpec kSpec{StreamType::Depth, hftmac_common::PayloadCodec::PlainVarint, "hftmac.depth.varint.v1"};

}  // namespace

CompressionResult compressDepthVarint(const CompressionRequest& request, const PipelineDescriptor& pipeline) noexcept {
    return hftmac_common::compressVarint(request, pipeline, kSpec);
}

ReplayArtifactInfo inspectDepthVarint(const std::filesystem::path& path, const PipelineDescriptor& pipeline) noexcept {
    return hftmac_common::inspectVarintArtifact(path, pipeline, kSpec);
}

Status decodeDepthVarintFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept {
    return hftmac_common::decodeVarintFile(path, onBlock, kSpec);
}

Status decodeDepthVarint(std::span<const std::uint8_t> bytes, const DecodedBlockCallback& onBlock) noexcept {
    return hftmac_common::decodeVarintBuffer(bytes, onBlock, kSpec);
}

}  // namespace hft_compressor::codecs::hftmac_varint

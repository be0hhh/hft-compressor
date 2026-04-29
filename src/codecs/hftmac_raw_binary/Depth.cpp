#include "codecs/hftmac_raw_binary/HftMacRawBinary.hpp"

#include "codecs/hftmac_common/HftMacCommon.hpp"

namespace hft_compressor::codecs::hftmac_raw_binary {
namespace {

constexpr hftmac_common::BackendSpec kSpec{StreamType::Depth, hftmac_common::PayloadCodec::RawBinary, "hftmac.depth.raw_binary.v1"};

}  // namespace

CompressionResult compressDepthRawBinary(const CompressionRequest& request, const PipelineDescriptor& pipeline) noexcept {
    return hftmac_common::compressRawBinary(request, pipeline, kSpec);
}

ReplayArtifactInfo inspectDepthRawBinary(const std::filesystem::path& path, const PipelineDescriptor& pipeline) noexcept {
    return hftmac_common::inspectRawBinaryArtifact(path, pipeline, kSpec);
}

Status decodeDepthRawBinaryFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept {
    return hftmac_common::decodeRawBinaryFile(path, onBlock, kSpec);
}

Status decodeDepthRawBinary(std::span<const std::uint8_t> bytes, const DecodedBlockCallback& onBlock) noexcept {
    return hftmac_common::decodeRawBinaryBuffer(bytes, onBlock, kSpec);
}

}  // namespace hft_compressor::codecs::hftmac_raw_binary

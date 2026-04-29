#include "codecs/hftmac_raw_binary/HftMacRawBinary.hpp"

#include "codecs/hftmac_common/HftMacCommon.hpp"

namespace hft_compressor::codecs::hftmac_raw_binary {
namespace {

constexpr hftmac_common::BackendSpec kSpec{StreamType::Trades, hftmac_common::PayloadCodec::RawBinary, "hftmac.trade.raw_binary.v1"};

}  // namespace

CompressionResult compressTradeRawBinary(const CompressionRequest& request, const PipelineDescriptor& pipeline) noexcept {
    return hftmac_common::compressRawBinary(request, pipeline, kSpec);
}

ReplayArtifactInfo inspectTradeRawBinary(const std::filesystem::path& path, const PipelineDescriptor& pipeline) noexcept {
    return hftmac_common::inspectRawBinaryArtifact(path, pipeline, kSpec);
}

Status decodeTradeRawBinaryFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept {
    return hftmac_common::decodeRawBinaryFile(path, onBlock, kSpec);
}

Status decodeTradeRawBinary(std::span<const std::uint8_t> bytes, const DecodedBlockCallback& onBlock) noexcept {
    return hftmac_common::decodeRawBinaryBuffer(bytes, onBlock, kSpec);
}

}  // namespace hft_compressor::codecs::hftmac_raw_binary

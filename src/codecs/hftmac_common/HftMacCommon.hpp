#pragma once

#include <cstdint>
#include <filesystem>
#include <span>
#include <string_view>

#include "hft_compressor/compressor.hpp"
#include "hft_compressor/pipeline.hpp"

namespace hft_compressor::codecs::hftmac_common {

enum class PayloadCodec : std::uint16_t {
    PlainVarint = 1u,
    RawBinary = 2u,
};

struct BackendSpec {
    StreamType streamType{StreamType::Unknown};
    PayloadCodec payloadCodec{PayloadCodec::PlainVarint};
    std::string_view formatId{};
};

CompressionResult compressVarint(const CompressionRequest& request,
                                 const PipelineDescriptor& pipeline,
                                 const BackendSpec& spec) noexcept;
ReplayArtifactInfo inspectVarintArtifact(const std::filesystem::path& path,
                                         const PipelineDescriptor& pipeline,
                                         const BackendSpec& spec) noexcept;
Status decodeVarintFile(const std::filesystem::path& path,
                        const DecodedBlockCallback& onBlock,
                        const BackendSpec& spec) noexcept;
Status decodeVarintBuffer(std::span<const std::uint8_t> bytes,
                          const DecodedBlockCallback& onBlock,
                          const BackendSpec& spec) noexcept;

CompressionResult compressRawBinary(const CompressionRequest& request,
                                    const PipelineDescriptor& pipeline,
                                    const BackendSpec& spec) noexcept;
ReplayArtifactInfo inspectRawBinaryArtifact(const std::filesystem::path& path,
                                            const PipelineDescriptor& pipeline,
                                            const BackendSpec& spec) noexcept;
Status decodeRawBinaryFile(const std::filesystem::path& path,
                           const DecodedBlockCallback& onBlock,
                           const BackendSpec& spec) noexcept;
Status decodeRawBinaryBuffer(std::span<const std::uint8_t> bytes,
                             const DecodedBlockCallback& onBlock,
                             const BackendSpec& spec) noexcept;

}  // namespace hft_compressor::codecs::hftmac_common

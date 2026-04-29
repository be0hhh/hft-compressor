#pragma once

#include <cstdint>
#include <filesystem>
#include <span>
#include <string>
#include <vector>

#include "hft_compressor/compressor.hpp"
#include "hft_compressor/pipeline.hpp"

namespace hft_compressor::codecs::standard_jsonl_blocks {

using CompressPayloadFn = Status (*)(std::span<const std::uint8_t> plain,
                                     int level,
                                     std::vector<std::uint8_t>& compressed,
                                     std::string& error) noexcept;
using DecompressPayloadFn = Status (*)(std::span<const std::uint8_t> compressed,
                                       std::uint32_t plainBytes,
                                       std::vector<std::uint8_t>& decoded,
                                       std::string& error) noexcept;

struct CodecSpec {
    std::uint16_t codec{0};
    std::string_view formatId{};
    std::string_view dependencyError{};
    CompressPayloadFn compressPayload{nullptr};
    DecompressPayloadFn decompressPayload{nullptr};
};

CompressionResult compress(const CompressionRequest& request,
                           const PipelineDescriptor& pipeline,
                           const CodecSpec& spec) noexcept;
ReplayArtifactInfo inspectArtifact(const std::filesystem::path& path,
                                   const PipelineDescriptor& pipeline,
                                   const CodecSpec& spec) noexcept;
Status decode(std::span<const std::uint8_t> compressedFile,
              const DecodedBlockCallback& onBlock,
              const CodecSpec& spec) noexcept;
Status decodeFile(const std::filesystem::path& path,
                  const DecodedBlockCallback& onBlock,
                  const CodecSpec& spec) noexcept;

}  // namespace hft_compressor::codecs::standard_jsonl_blocks


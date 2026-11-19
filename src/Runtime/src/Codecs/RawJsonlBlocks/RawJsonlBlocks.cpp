#include "RawJsonlBlocks.hpp"

#include <string>
#include <vector>

#include "../StandardJsonlBlocks/StandardJsonlBlocks.hpp"
#include "../../Container/Hfc/Format.hpp"

namespace hft_compressor::codecs::raw_jsonl_blocks {
namespace {

constexpr std::uint16_t kCodecRawBinaryBlocks = 100u;

Status compressPayload(std::span<const std::uint8_t> plain,
                       int,
                       std::vector<std::uint8_t>& compressed,
                       std::string&) noexcept {
    compressed.assign(plain.begin(), plain.end());
    return Status::Ok;
}

Status decompressPayload(std::span<const std::uint8_t> input,
                         std::uint32_t plainBytes,
                         std::vector<std::uint8_t>& decoded,
                         std::string& error) noexcept {
    if (input.size() != plainBytes) {
        error = "raw block size mismatch";
        return Status::CorruptData;
    }
    decoded.assign(input.begin(), input.end());
    return Status::Ok;
}

const standard_jsonl_blocks::CodecSpec kSpec{
    kCodecRawBinaryBlocks,
    "hfr.raw_binary_blocks",
    "",
    compressPayload,
    decompressPayload,
};

}  // namespace

CompressionResult compress(const CompressionRequest& request, const PipelineDescriptor& pipeline) noexcept {
    return standard_jsonl_blocks::compress(request, pipeline, kSpec);
}

ReplayArtifactInfo inspectArtifact(const std::filesystem::path& path, const PipelineDescriptor& pipeline) noexcept {
    return standard_jsonl_blocks::inspectArtifact(path, pipeline, kSpec);
}

Status decode(std::span<const std::uint8_t> compressedFile, const DecodedBlockCallback& onBlock) noexcept {
    return standard_jsonl_blocks::decode(compressedFile, onBlock, kSpec);
}

Status decodeFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept {
    return standard_jsonl_blocks::decodeFile(path, onBlock, kSpec);
}

}  // namespace hft_compressor::codecs::raw_jsonl_blocks

#include "codecs/xz_jsonl_blocks/XzJsonlBlocks.hpp"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "codecs/standard_jsonl_blocks/StandardJsonlBlocks.hpp"
#include "container/hfc/format.hpp"

#if HFT_COMPRESSOR_WITH_LZMA
#include <lzma.h>
#endif

namespace hft_compressor::codecs::xz_jsonl_blocks {
namespace {

#if HFT_COMPRESSOR_WITH_LZMA
Status compressPayload(std::span<const std::uint8_t> plain,
                       int level,
                       std::vector<std::uint8_t>& compressed,
                       std::string& error) noexcept {
    const std::uint32_t preset = static_cast<std::uint32_t>(std::clamp(level, 0, 9));
    const std::size_t bound = lzma_stream_buffer_bound(plain.size());
    compressed.resize(bound);
    std::size_t outPos = 0;
    const lzma_ret ret = lzma_easy_buffer_encode(preset,
                                                 LZMA_CHECK_CRC64,
                                                 nullptr,
                                                 plain.data(),
                                                 plain.size(),
                                                 compressed.data(),
                                                 &outPos,
                                                 compressed.size());
    if (ret != LZMA_OK) {
        error = "lzma failed to compress block";
        return Status::DecodeError;
    }
    compressed.resize(outPos);
    return Status::Ok;
}

Status decompressPayload(std::span<const std::uint8_t> input,
                         std::uint32_t plainBytes,
                         std::vector<std::uint8_t>& decoded,
                         std::string& error) noexcept {
    decoded.resize(plainBytes);
    std::uint64_t memlimit = UINT64_MAX;
    std::size_t inPos = 0;
    std::size_t outPos = 0;
    const lzma_ret ret = lzma_stream_buffer_decode(&memlimit,
                                                   0,
                                                   nullptr,
                                                   input.data(),
                                                   &inPos,
                                                   input.size(),
                                                   decoded.data(),
                                                   &outPos,
                                                   decoded.size());
    if (ret != LZMA_OK || inPos != input.size() || outPos != plainBytes) {
        error = "lzma failed to decompress block";
        return Status::DecodeError;
    }
    return Status::Ok;
}
#endif

const standard_jsonl_blocks::CodecSpec kSpec{
    format::kCodecXzJsonlBlocksV1,
    "hfc.xz_jsonl_blocks_v1",
    "liblzma was not found at configure time",
#if HFT_COMPRESSOR_WITH_LZMA
    compressPayload,
    decompressPayload,
#else
    nullptr,
    nullptr,
#endif
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

}  // namespace hft_compressor::codecs::xz_jsonl_blocks

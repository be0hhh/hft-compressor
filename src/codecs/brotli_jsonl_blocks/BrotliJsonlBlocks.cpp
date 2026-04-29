#include "codecs/brotli_jsonl_blocks/BrotliJsonlBlocks.hpp"

#include <algorithm>
#include <string>
#include <vector>

#include "codecs/standard_jsonl_blocks/StandardJsonlBlocks.hpp"
#include "container/hfc/format.hpp"

#if HFT_COMPRESSOR_WITH_BROTLI
#include <brotli/decode.h>
#include <brotli/encode.h>
#endif

namespace hft_compressor::codecs::brotli_jsonl_blocks {
namespace {

#if HFT_COMPRESSOR_WITH_BROTLI
Status compressPayload(std::span<const std::uint8_t> plain,
                       int level,
                       std::vector<std::uint8_t>& compressed,
                       std::string& error) noexcept {
    const int quality = std::clamp(level, 1, 11);
    std::size_t encodedSize = BrotliEncoderMaxCompressedSize(plain.size());
    compressed.resize(encodedSize);
    if (BrotliEncoderCompress(quality,
                              BROTLI_DEFAULT_WINDOW,
                              BROTLI_MODE_GENERIC,
                              plain.size(),
                              plain.data(),
                              &encodedSize,
                              compressed.data()) == BROTLI_FALSE) {
        error = "brotli failed to compress block";
        return Status::DecodeError;
    }
    compressed.resize(encodedSize);
    return Status::Ok;
}

Status decompressPayload(std::span<const std::uint8_t> input,
                         std::uint32_t plainBytes,
                         std::vector<std::uint8_t>& decoded,
                         std::string& error) noexcept {
    decoded.resize(plainBytes);
    std::size_t decodedSize = decoded.size();
    const auto status = BrotliDecoderDecompress(input.size(), input.data(), &decodedSize, decoded.data());
    if (status != BROTLI_DECODER_RESULT_SUCCESS || decodedSize != plainBytes) {
        error = "brotli failed to decompress block";
        return Status::DecodeError;
    }
    return Status::Ok;
}
#endif

const standard_jsonl_blocks::CodecSpec kSpec{
    format::kCodecBrotliJsonlBlocksV1,
    "hfc.brotli_jsonl_blocks_v1",
    "brotli encoder/decoder libraries were not found at configure time",
#if HFT_COMPRESSOR_WITH_BROTLI
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

}  // namespace hft_compressor::codecs::brotli_jsonl_blocks

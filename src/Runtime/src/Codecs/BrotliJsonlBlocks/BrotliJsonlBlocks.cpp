#include "BrotliJsonlBlocks.hpp"
#include "../ExternalBlockPrimitives.hpp"

#include <algorithm>
#include <string>
#include <vector>

#include "../StandardJsonlBlocks/StandardJsonlBlocks.hpp"
#include "../../Container/Hfc/Format.hpp"

#if HFT_COMPRESSOR_WITH_BROTLI
#include <brotli/decode.h>
#include <brotli/encode.h>
#endif

namespace hft_compressor::codecs::external_primitives {

bool brotliAvailable() noexcept {
#if HFT_COMPRESSOR_WITH_BROTLI
    return true;
#else
    return false;
#endif
}

std::size_t brotliCompressBound(std::size_t inputBytes) noexcept {
#if HFT_COMPRESSOR_WITH_BROTLI
    return BrotliEncoderMaxCompressedSize(inputBytes);
#else
    (void)inputBytes;
    return 0u;
#endif
}

Status brotliCompressBlock(
    int quality,
    std::span<const std::uint8_t> src,
    std::span<std::uint8_t> dst,
    std::size_t& written) noexcept {
    written = 0u;
#if HFT_COMPRESSOR_WITH_BROTLI
    const int q = std::clamp(quality, 1, 11);
    std::size_t encodedSize = dst.size();
    if (BrotliEncoderCompress(
            q,
            BROTLI_DEFAULT_WINDOW,
            BROTLI_MODE_GENERIC,
            src.size(),
            src.data(),
            &encodedSize,
            dst.data()) == BROTLI_FALSE) {
        return Status::DecodeError;
    }
    written = encodedSize;
    return Status::Ok;
#else
    (void)quality;
    (void)src;
    (void)dst;
    return Status::DependencyUnavailable;
#endif
}

Status brotliDecompressBlock(
    std::span<const std::uint8_t> src,
    std::span<std::uint8_t> dst) noexcept {
#if HFT_COMPRESSOR_WITH_BROTLI
    std::size_t decodedSize = dst.size();
    const auto status = BrotliDecoderDecompress(src.size(), src.data(), &decodedSize, dst.data());
    if (status != BROTLI_DECODER_RESULT_SUCCESS || decodedSize != dst.size()) {
        return Status::CorruptData;
    }
    return Status::Ok;
#else
    (void)src;
    (void)dst;
    return Status::DependencyUnavailable;
#endif
}

} // namespace hft_compressor::codecs::external_primitives

namespace hft_compressor::codecs::brotli_jsonl_blocks {
namespace {

#if HFT_COMPRESSOR_WITH_BROTLI
Status compressPayload(std::span<const std::uint8_t> plain,
                       int level,
                       std::vector<std::uint8_t>& compressed,
                       std::string& error) noexcept {
    compressed.resize(external_primitives::brotliCompressBound(plain.size()));
    std::size_t written = 0u;
    const auto status = external_primitives::brotliCompressBlock(level, plain, compressed, written);
    if (!isOk(status)) {
        error = "brotli failed to compress block";
        return status;
    }
    compressed.resize(written);
    return Status::Ok;
}

Status decompressPayload(std::span<const std::uint8_t> input,
                         std::uint32_t plainBytes,
                         std::vector<std::uint8_t>& decoded,
                         std::string& error) noexcept {
    decoded.resize(plainBytes);
    const auto status = external_primitives::brotliDecompressBlock(input, decoded);
    if (!isOk(status)) {
        error = "brotli failed to decompress block";
        return Status::DecodeError;
    }
    return Status::Ok;
}
#endif

const standard_jsonl_blocks::CodecSpec kSpec{
    format::kCodecBrotliJsonlBlocks,
    "hfc.brotli_binary_blocks",
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

#include "XzJsonlBlocks.hpp"
#include "../ExternalBlockPrimitives.hpp"

#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

#include "../StandardJsonlBlocks/StandardJsonlBlocks.hpp"
#include "../../Container/Hfc/Format.hpp"

#if HFT_COMPRESSOR_WITH_LZMA
#include <lzma.h>
#endif

namespace hft_compressor::codecs::external_primitives {

bool xzLzmaAvailable() noexcept {
#if HFT_COMPRESSOR_WITH_LZMA
    return true;
#else
    return false;
#endif
}

std::size_t xzLzmaCompressBound(std::size_t inputBytes) noexcept {
#if HFT_COMPRESSOR_WITH_LZMA
    return lzma_stream_buffer_bound(inputBytes);
#else
    (void)inputBytes;
    return 0u;
#endif
}

Status xzLzmaCompressBlock(
    int preset,
    std::span<const std::uint8_t> src,
    std::span<std::uint8_t> dst,
    std::size_t& written) noexcept {
    written = 0u;
#if HFT_COMPRESSOR_WITH_LZMA
    const auto clamped = static_cast<std::uint32_t>(std::clamp(preset, 0, 9));
    std::size_t outPos = 0u;
    const lzma_ret ret = lzma_easy_buffer_encode(
        clamped,
        LZMA_CHECK_CRC64,
        nullptr,
        src.data(),
        src.size(),
        dst.data(),
        &outPos,
        dst.size());
    if (ret != LZMA_OK) return Status::DecodeError;
    written = outPos;
    return Status::Ok;
#else
    (void)preset;
    (void)src;
    (void)dst;
    return Status::DependencyUnavailable;
#endif
}

Status xzLzmaDecompressBlock(
    std::span<const std::uint8_t> src,
    std::span<std::uint8_t> dst) noexcept {
#if HFT_COMPRESSOR_WITH_LZMA
    std::uint64_t memlimit = UINT64_MAX;
    std::size_t inPos = 0u;
    std::size_t outPos = 0u;
    const lzma_ret ret = lzma_stream_buffer_decode(
        &memlimit,
        0,
        nullptr,
        src.data(),
        &inPos,
        src.size(),
        dst.data(),
        &outPos,
        dst.size());
    if (ret != LZMA_OK || inPos != src.size() || outPos != dst.size()) {
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

namespace hft_compressor::codecs::xz_jsonl_blocks {
namespace {

#if HFT_COMPRESSOR_WITH_LZMA
Status compressPayload(std::span<const std::uint8_t> plain,
                       int level,
                       std::vector<std::uint8_t>& compressed,
                       std::string& error) noexcept {
    compressed.resize(external_primitives::xzLzmaCompressBound(plain.size()));
    std::size_t written = 0u;
    const auto status = external_primitives::xzLzmaCompressBlock(level, plain, compressed, written);
    if (!isOk(status)) {
        error = "lzma failed to compress block";
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
    const auto status = external_primitives::xzLzmaDecompressBlock(input, decoded);
    if (!isOk(status)) {
        error = "lzma failed to decompress block";
        return Status::DecodeError;
    }
    return Status::Ok;
}
#endif

const standard_jsonl_blocks::CodecSpec kSpec{
    format::kCodecXzJsonlBlocks,
    "hfc.xz_binary_blocks",
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

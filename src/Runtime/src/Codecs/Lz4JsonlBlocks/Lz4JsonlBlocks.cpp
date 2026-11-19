#include "Lz4JsonlBlocks.hpp"
#include "../ExternalBlockPrimitives.hpp"

#include <limits>
#include <string>
#include <vector>

#include "../StandardJsonlBlocks/StandardJsonlBlocks.hpp"
#include "../../Container/Hfc/Format.hpp"

#if HFT_COMPRESSOR_WITH_LZ4
#include <lz4.h>
#endif

namespace hft_compressor::codecs::external_primitives {

bool lz4Available() noexcept {
#if HFT_COMPRESSOR_WITH_LZ4
    return true;
#else
    return false;
#endif
}

std::size_t lz4StateBytes() noexcept {
#if HFT_COMPRESSOR_WITH_LZ4
    const int n = LZ4_sizeofState();
    return n > 0 ? static_cast<std::size_t>(n) : 0u;
#else
    return 0u;
#endif
}

std::size_t lz4CompressBound(std::size_t inputBytes) noexcept {
#if HFT_COMPRESSOR_WITH_LZ4
    if (inputBytes > static_cast<std::size_t>(std::numeric_limits<int>::max())) return 0u;
    const int bound = LZ4_compressBound(static_cast<int>(inputBytes));
    return bound > 0 ? static_cast<std::size_t>(bound) : 0u;
#else
    (void)inputBytes;
    return 0u;
#endif
}

Status lz4CompressFast(
    std::span<std::uint8_t> state,
    std::span<const std::uint8_t> src,
    std::span<std::uint8_t> dst,
    std::size_t& written) noexcept {
    written = 0u;
#if HFT_COMPRESSOR_WITH_LZ4
    if (src.size() > static_cast<std::size_t>(std::numeric_limits<int>::max()) ||
        dst.size() > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
        return Status::InvalidArgument;
    }
    int n = 0;
    if (!state.empty() && state.size() >= lz4StateBytes()) {
        n = LZ4_compress_fast_extState(
            state.data(),
            reinterpret_cast<const char*>(src.data()),
            reinterpret_cast<char*>(dst.data()),
            static_cast<int>(src.size()),
            static_cast<int>(dst.size()),
            1);
    } else {
        n = LZ4_compress_default(
            reinterpret_cast<const char*>(src.data()),
            reinterpret_cast<char*>(dst.data()),
            static_cast<int>(src.size()),
            static_cast<int>(dst.size()));
    }
    if (n <= 0) return Status::DecodeError;
    written = static_cast<std::size_t>(n);
    return Status::Ok;
#else
    (void)state;
    (void)src;
    (void)dst;
    return Status::DependencyUnavailable;
#endif
}

Status lz4DecompressBlock(
    std::span<const std::uint8_t> src,
    std::span<std::uint8_t> dst) noexcept {
#if HFT_COMPRESSOR_WITH_LZ4
    if (src.size() > static_cast<std::size_t>(std::numeric_limits<int>::max()) ||
        dst.size() > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
        return Status::CorruptData;
    }
    const int n = LZ4_decompress_safe(
        reinterpret_cast<const char*>(src.data()),
        reinterpret_cast<char*>(dst.data()),
        static_cast<int>(src.size()),
        static_cast<int>(dst.size()));
    if (n < 0 || static_cast<std::size_t>(n) != dst.size()) return Status::CorruptData;
    return Status::Ok;
#else
    (void)src;
    (void)dst;
    return Status::DependencyUnavailable;
#endif
}

} // namespace hft_compressor::codecs::external_primitives

namespace hft_compressor::codecs::lz4_jsonl_blocks {
namespace {

#if HFT_COMPRESSOR_WITH_LZ4
Status compressPayload(std::span<const std::uint8_t> plain,
                       int,
                       std::vector<std::uint8_t>& compressed,
                       std::string& error) noexcept {
    const std::size_t bound = external_primitives::lz4CompressBound(plain.size());
    if (bound == 0u) {
        error = "lz4 block is too large";
        return Status::InvalidArgument;
    }
    compressed.resize(bound);
    std::size_t written = 0u;
    const auto status = external_primitives::lz4CompressFast({}, plain, compressed, written);
    if (!isOk(status)) {
        error = "lz4 failed to compress block";
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
    const auto status = external_primitives::lz4DecompressBlock(input, decoded);
    if (!isOk(status)) {
        error = "lz4 failed to decompress block";
        return Status::DecodeError;
    }
    return Status::Ok;
}
#endif

const standard_jsonl_blocks::CodecSpec kSpec{
    format::kCodecLz4JsonlBlocks,
    "hfc.lz4_binary_blocks",
    "liblz4 was not found at configure time",
#if HFT_COMPRESSOR_WITH_LZ4
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

}  // namespace hft_compressor::codecs::lz4_jsonl_blocks

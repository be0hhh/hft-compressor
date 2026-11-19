#include "ZstdJsonlBlocks.hpp"
#include "../ExternalBlockPrimitives.hpp"

#include <algorithm>
#include <cstddef>
#include <string>
#include <vector>

#include "../StandardJsonlBlocks/StandardJsonlBlocks.hpp"
#include "../../Container/Hfc/Format.hpp"

#if HFT_COMPRESSOR_WITH_ZSTD
#include <zstd.h>
#endif

namespace hft_compressor::codecs::external_primitives {

bool zstdAvailable() noexcept {
#if HFT_COMPRESSOR_WITH_ZSTD
    return true;
#else
    return false;
#endif
}

void* createZstdCCtx() noexcept {
#if HFT_COMPRESSOR_WITH_ZSTD
    return ZSTD_createCCtx();
#else
    return nullptr;
#endif
}

void freeZstdCCtx(void* ctx) noexcept {
#if HFT_COMPRESSOR_WITH_ZSTD
    if (ctx != nullptr) ZSTD_freeCCtx(static_cast<ZSTD_CCtx*>(ctx));
#else
    (void)ctx;
#endif
}

void* createZstdDCtx() noexcept {
#if HFT_COMPRESSOR_WITH_ZSTD
    return ZSTD_createDCtx();
#else
    return nullptr;
#endif
}

void freeZstdDCtx(void* ctx) noexcept {
#if HFT_COMPRESSOR_WITH_ZSTD
    if (ctx != nullptr) ZSTD_freeDCtx(static_cast<ZSTD_DCtx*>(ctx));
#else
    (void)ctx;
#endif
}

std::size_t zstdCompressBound(std::size_t inputBytes) noexcept {
#if HFT_COMPRESSOR_WITH_ZSTD
    return ZSTD_compressBound(inputBytes);
#else
    (void)inputBytes;
    return 0u;
#endif
}

Status zstdCompressWithCtx(
    void* cctx,
    int level,
    std::span<const std::uint8_t> src,
    std::span<std::uint8_t> dst,
    std::size_t& written) noexcept {
    written = 0u;
#if HFT_COMPRESSOR_WITH_ZSTD
    std::size_t n = 0u;
    if (cctx != nullptr) {
        n = ZSTD_compressCCtx(
            static_cast<ZSTD_CCtx*>(cctx),
            dst.data(),
            dst.size(),
            src.data(),
            src.size(),
            level);
    } else {
        n = ZSTD_compress(dst.data(), dst.size(), src.data(), src.size(), level);
    }
    if (ZSTD_isError(n)) return Status::DecodeError;
    written = n;
    return Status::Ok;
#else
    (void)cctx;
    (void)level;
    (void)src;
    (void)dst;
    return Status::DependencyUnavailable;
#endif
}

Status zstdDecompressWithCtx(
    void* dctx,
    std::span<const std::uint8_t> src,
    std::span<std::uint8_t> dst) noexcept {
#if HFT_COMPRESSOR_WITH_ZSTD
    std::size_t n = 0u;
    if (dctx != nullptr) {
        n = ZSTD_decompressDCtx(
            static_cast<ZSTD_DCtx*>(dctx),
            dst.data(),
            dst.size(),
            src.data(),
            src.size());
    } else {
        n = ZSTD_decompress(dst.data(), dst.size(), src.data(), src.size());
    }
    if (ZSTD_isError(n) || n != dst.size()) return Status::CorruptData;
    return Status::Ok;
#else
    (void)dctx;
    (void)src;
    (void)dst;
    return Status::DependencyUnavailable;
#endif
}

} // namespace hft_compressor::codecs::external_primitives

namespace hft_compressor::codecs::zstd_jsonl_blocks {
namespace {

#if HFT_COMPRESSOR_WITH_ZSTD
Status compressPayload(std::span<const std::uint8_t> plain,
                       int level,
                       std::vector<std::uint8_t>& compressed,
                       std::string& error) noexcept {
    compressed.resize(external_primitives::zstdCompressBound(plain.size()));
    std::size_t written = 0u;
    const auto status = external_primitives::zstdCompressWithCtx(nullptr, level, plain, compressed, written);
    if (!isOk(status)) {
        error = "zstd failed to compress block";
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
    const auto status = external_primitives::zstdDecompressWithCtx(nullptr, input, decoded);
    if (!isOk(status)) {
        error = "zstd failed to decompress block";
        return Status::DecodeError;
    }
    return Status::Ok;
}
#endif

const standard_jsonl_blocks::CodecSpec kSpec{
    format::kCodecZstdJsonlBlocks,
    "hfc.zstd_binary_blocks",
    "libzstd was not found at configure time",
#if HFT_COMPRESSOR_WITH_ZSTD
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

}  // namespace hft_compressor::codecs::zstd_jsonl_blocks

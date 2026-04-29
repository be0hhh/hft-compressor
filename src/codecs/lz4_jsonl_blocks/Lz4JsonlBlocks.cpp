#include "codecs/lz4_jsonl_blocks/Lz4JsonlBlocks.hpp"

#include <limits>
#include <string>
#include <vector>

#include "codecs/standard_jsonl_blocks/StandardJsonlBlocks.hpp"
#include "container/hfc/format.hpp"

#if HFT_COMPRESSOR_WITH_LZ4
#include <lz4.h>
#endif

namespace hft_compressor::codecs::lz4_jsonl_blocks {
namespace {

#if HFT_COMPRESSOR_WITH_LZ4
Status compressPayload(std::span<const std::uint8_t> plain,
                       int,
                       std::vector<std::uint8_t>& compressed,
                       std::string& error) noexcept {
    if (plain.size() > static_cast<std::size_t>(std::numeric_limits<int>::max())) {
        error = "lz4 block is too large";
        return Status::InvalidArgument;
    }
    const int bound = LZ4_compressBound(static_cast<int>(plain.size()));
    if (bound <= 0) {
        error = "lz4 failed to calculate bound";
        return Status::DecodeError;
    }
    compressed.resize(static_cast<std::size_t>(bound));
    const int written = LZ4_compress_default(reinterpret_cast<const char*>(plain.data()),
                                             reinterpret_cast<char*>(compressed.data()),
                                             static_cast<int>(plain.size()),
                                             bound);
    if (written <= 0) {
        error = "lz4 failed to compress block";
        return Status::DecodeError;
    }
    compressed.resize(static_cast<std::size_t>(written));
    return Status::Ok;
}

Status decompressPayload(std::span<const std::uint8_t> input,
                         std::uint32_t plainBytes,
                         std::vector<std::uint8_t>& decoded,
                         std::string& error) noexcept {
    if (input.size() > static_cast<std::size_t>(std::numeric_limits<int>::max()) || plainBytes > static_cast<std::uint32_t>(std::numeric_limits<int>::max())) {
        error = "lz4 block is too large";
        return Status::InvalidArgument;
    }
    decoded.resize(plainBytes);
    const int written = LZ4_decompress_safe(reinterpret_cast<const char*>(input.data()),
                                            reinterpret_cast<char*>(decoded.data()),
                                            static_cast<int>(input.size()),
                                            static_cast<int>(decoded.size()));
    if (written < 0 || static_cast<std::uint32_t>(written) != plainBytes) {
        error = "lz4 failed to decompress block";
        return Status::DecodeError;
    }
    return Status::Ok;
}
#endif

const standard_jsonl_blocks::CodecSpec kSpec{
    format::kCodecLz4JsonlBlocksV1,
    "hfc.lz4_jsonl_blocks_v1",
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

#include "codecs/gzip_jsonl_blocks/GzipJsonlBlocks.hpp"

#include <algorithm>
#include <limits>
#include <string>
#include <vector>

#include "codecs/standard_jsonl_blocks/StandardJsonlBlocks.hpp"
#include "container/hfc/format.hpp"

#if HFT_COMPRESSOR_WITH_ZLIB
#include <zlib.h>
#endif

namespace hft_compressor::codecs::gzip_jsonl_blocks {
namespace {

#if HFT_COMPRESSOR_WITH_ZLIB
Status compressPayload(std::span<const std::uint8_t> plain,
                       int level,
                       std::vector<std::uint8_t>& compressed,
                       std::string& error) noexcept {
    if (plain.size() > static_cast<std::size_t>(std::numeric_limits<uInt>::max())) {
        error = "gzip block is too large";
        return Status::InvalidArgument;
    }
    const int zlibLevel = std::clamp(level, 1, 9);
    z_stream stream{};
    if (deflateInit2(&stream, zlibLevel, Z_DEFLATED, 15 + 16, 8, Z_DEFAULT_STRATEGY) != Z_OK) {
        error = "zlib failed to initialize gzip encoder";
        return Status::DecodeError;
    }
    compressed.resize(compressBound(static_cast<uLong>(plain.size())) + 32u);
    stream.next_in = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(plain.data()));
    stream.avail_in = static_cast<uInt>(plain.size());
    stream.next_out = reinterpret_cast<Bytef*>(compressed.data());
    stream.avail_out = static_cast<uInt>(compressed.size());
    const int ret = deflate(&stream, Z_FINISH);
    if (ret != Z_STREAM_END) {
        deflateEnd(&stream);
        error = "zlib failed to compress gzip block";
        return Status::DecodeError;
    }
    compressed.resize(stream.total_out);
    deflateEnd(&stream);
    return Status::Ok;
}

Status decompressPayload(std::span<const std::uint8_t> input,
                         std::uint32_t plainBytes,
                         std::vector<std::uint8_t>& decoded,
                         std::string& error) noexcept {
    if (input.size() > static_cast<std::size_t>(std::numeric_limits<uInt>::max())) {
        error = "gzip block is too large";
        return Status::InvalidArgument;
    }
    decoded.resize(plainBytes);
    z_stream stream{};
    if (inflateInit2(&stream, 15 + 16) != Z_OK) {
        error = "zlib failed to initialize gzip decoder";
        return Status::DecodeError;
    }
    stream.next_in = const_cast<Bytef*>(reinterpret_cast<const Bytef*>(input.data()));
    stream.avail_in = static_cast<uInt>(input.size());
    stream.next_out = reinterpret_cast<Bytef*>(decoded.data());
    stream.avail_out = static_cast<uInt>(decoded.size());
    const int ret = inflate(&stream, Z_FINISH);
    if (ret != Z_STREAM_END || stream.total_out != plainBytes) {
        inflateEnd(&stream);
        error = "zlib failed to decompress gzip block";
        return Status::DecodeError;
    }
    inflateEnd(&stream);
    return Status::Ok;
}
#endif

const standard_jsonl_blocks::CodecSpec kSpec{
    format::kCodecGzipJsonlBlocksV1,
    "hfc.gzip_jsonl_blocks_v1",
    "zlib was not found at configure time",
#if HFT_COMPRESSOR_WITH_ZLIB
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

}  // namespace hft_compressor::codecs::gzip_jsonl_blocks

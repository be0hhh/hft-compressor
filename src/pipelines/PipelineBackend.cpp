#include "pipelines/PipelineBackend.hpp"

#include <array>

#include "codecs/brotli_jsonl_blocks/BrotliJsonlBlocks.hpp"
#include "codecs/gzip_jsonl_blocks/GzipJsonlBlocks.hpp"
#include "codecs/lz4_jsonl_blocks/Lz4JsonlBlocks.hpp"
#include "codecs/raw_jsonl_blocks/RawJsonlBlocks.hpp"
#include "codecs/xz_jsonl_blocks/XzJsonlBlocks.hpp"
#include "codecs/zstd_jsonl_blocks/ZstdJsonlBlocks.hpp"

namespace hft_compressor::pipelines {
namespace {

constexpr std::array<PipelineBackend, 6> kBackends{{
    {"std.zstd_jsonl_blocks_v1",
     "hfc.zstd_jsonl_blocks_v1",
     codecs::zstd_jsonl_blocks::compress,
     codecs::zstd_jsonl_blocks::inspectArtifact,
     codecs::zstd_jsonl_blocks::decodeFile,
     codecs::zstd_jsonl_blocks::decode},
    {"std.raw_jsonl_blocks_v1",
     "hfr.raw_jsonl_blocks_v1",
     codecs::raw_jsonl_blocks::compress,
     codecs::raw_jsonl_blocks::inspectArtifact,
     codecs::raw_jsonl_blocks::decodeFile,
     codecs::raw_jsonl_blocks::decode},
    {"std.lz4_jsonl_blocks_v1",
     "hfc.lz4_jsonl_blocks_v1",
     codecs::lz4_jsonl_blocks::compress,
     codecs::lz4_jsonl_blocks::inspectArtifact,
     codecs::lz4_jsonl_blocks::decodeFile,
     codecs::lz4_jsonl_blocks::decode},
    {"std.brotli_jsonl_blocks_v1",
     "hfc.brotli_jsonl_blocks_v1",
     codecs::brotli_jsonl_blocks::compress,
     codecs::brotli_jsonl_blocks::inspectArtifact,
     codecs::brotli_jsonl_blocks::decodeFile,
     codecs::brotli_jsonl_blocks::decode},
    {"std.xz_jsonl_blocks_v1",
     "hfc.xz_jsonl_blocks_v1",
     codecs::xz_jsonl_blocks::compress,
     codecs::xz_jsonl_blocks::inspectArtifact,
     codecs::xz_jsonl_blocks::decodeFile,
     codecs::xz_jsonl_blocks::decode},
    {"std.gzip_jsonl_blocks_v1",
     "hfc.gzip_jsonl_blocks_v1",
     codecs::gzip_jsonl_blocks::compress,
     codecs::gzip_jsonl_blocks::inspectArtifact,
     codecs::gzip_jsonl_blocks::decodeFile,
     codecs::gzip_jsonl_blocks::decode},
}};

}  // namespace

const PipelineBackend* findBackend(std::string_view pipelineId) noexcept {
    for (const auto& backend : kBackends) {
        if (backend.pipelineId == pipelineId) return &backend;
    }
    return nullptr;
}

}  // namespace hft_compressor::pipelines

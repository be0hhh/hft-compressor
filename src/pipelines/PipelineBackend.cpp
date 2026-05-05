#include "pipelines/PipelineBackend.hpp"

#include <array>

#include "codecs/bookticker_delta_mask/BookTickerDeltaMask.hpp"
#include "codecs/brotli_jsonl_blocks/BrotliJsonlBlocks.hpp"
#include "codecs/depth_ladder_offset/DepthLadderOffset.hpp"
#include "codecs/depth_ladder_offset/DepthLadderOffsetV2.hpp"
#include "codecs/entropy_hftmac/EntropyHftMac.hpp"
#include "codecs/gzip_jsonl_blocks/GzipJsonlBlocks.hpp"
#include "codecs/lz4_jsonl_blocks/Lz4JsonlBlocks.hpp"
#include "codecs/raw_jsonl_blocks/RawJsonlBlocks.hpp"
#include "codecs/trades_grouped_delta_qtydict/TradesGroupedDeltaQtyDict.hpp"
#include "codecs/xz_jsonl_blocks/XzJsonlBlocks.hpp"
#include "codecs/zstd_jsonl_blocks/ZstdJsonlBlocks.hpp"

namespace hft_compressor::pipelines {
namespace {

constexpr auto kBackends = std::to_array<PipelineBackend>({
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

    {"hftmac.trades_grouped_delta_qtydict_math_v3",
     "hftmac.trades_grouped_delta_qtydict.math.v3",
     codecs::trades_grouped_delta_qtydict::compress,
     codecs::trades_grouped_delta_qtydict::inspectArtifact,
     codecs::trades_grouped_delta_qtydict::decodeFile,
     codecs::trades_grouped_delta_qtydict::decode},
    {"hftmac.bookticker_delta_mask_v2",
     "hftmac.bookticker_delta_mask.v2",
     codecs::bookticker_delta_mask::compress,
     codecs::bookticker_delta_mask::inspectArtifact,
     codecs::bookticker_delta_mask::decodeFile,
     codecs::bookticker_delta_mask::decode},
    {"hftmac.depth_ladder_offset_v3",
     "hftmac.depth_ladder_offset.v3",
     codecs::depth_ladder_offset_v2::compress,
     codecs::depth_ladder_offset_v2::inspectArtifact,
     codecs::depth_ladder_offset_v2::decodeFile,
     codecs::depth_ladder_offset_v2::decode},
    {"hftmac.trades_grouped_delta_qtydict_ac16_ctx0_v1", "hftmac.trades_grouped_delta_qtydict.entropy.v1", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.trades_grouped_delta_qtydict_ac16_ctx8_v1", "hftmac.trades_grouped_delta_qtydict.entropy.v1", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.trades_grouped_delta_qtydict_ac16_ctx12_v1", "hftmac.trades_grouped_delta_qtydict.entropy.v1", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.trades_grouped_delta_qtydict_ac32_ctx8_v1", "hftmac.trades_grouped_delta_qtydict.entropy.v1", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.trades_grouped_delta_qtydict_range_byte_ctx8_v1", "hftmac.trades_grouped_delta_qtydict.entropy.v1", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.trades_grouped_delta_qtydict_rans_byte_static_v1", "hftmac.trades_grouped_delta_qtydict.entropy.v1", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.bookticker_delta_mask_ac16_ctx0_v1", "hftmac.bookticker_delta_mask.entropy.v1", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.bookticker_delta_mask_ac16_ctx8_v1", "hftmac.bookticker_delta_mask.entropy.v1", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.bookticker_delta_mask_ac16_ctx12_v1", "hftmac.bookticker_delta_mask.entropy.v1", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.bookticker_delta_mask_ac32_ctx8_v1", "hftmac.bookticker_delta_mask.entropy.v1", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.bookticker_delta_mask_range_byte_ctx8_v1", "hftmac.bookticker_delta_mask.entropy.v1", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.bookticker_delta_mask_rans_byte_static_v1", "hftmac.bookticker_delta_mask.entropy.v1", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.depth_ladder_offset_ac16_ctx0_v1", "hftmac.depth_ladder_offset.entropy.v1", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.depth_ladder_offset_ac16_ctx8_v1", "hftmac.depth_ladder_offset.entropy.v1", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.depth_ladder_offset_ac16_ctx12_v1", "hftmac.depth_ladder_offset.entropy.v1", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.depth_ladder_offset_ac32_ctx8_v1", "hftmac.depth_ladder_offset.entropy.v1", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.depth_ladder_offset_range_byte_ctx8_v1", "hftmac.depth_ladder_offset.entropy.v1", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.depth_ladder_offset_rans_byte_static_v1", "hftmac.depth_ladder_offset.entropy.v1", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
});

}  // namespace

const PipelineBackend* findBackend(std::string_view pipelineId) noexcept {
    for (const auto& backend : kBackends) {
        if (backend.pipelineId == pipelineId) return &backend;
    }
    return nullptr;
}

}  // namespace hft_compressor::pipelines

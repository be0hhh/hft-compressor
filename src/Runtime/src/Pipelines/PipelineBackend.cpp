#include "PipelineBackend.hpp"

#include <array>

#include "../Codecs/BooktickerDeltaMask/BookTickerDeltaMask.hpp"
#include "../Codecs/BrotliJsonlBlocks/BrotliJsonlBlocks.hpp"
#include "../Codecs/DepthLadderOffset/DepthLadderOffset.hpp"
#include "../Codecs/EntropyHftmac/EntropyHftMac.hpp"
#include "../Codecs/GzipJsonlBlocks/GzipJsonlBlocks.hpp"
#include "../Codecs/Lz4JsonlBlocks/Lz4JsonlBlocks.hpp"
#include "../Codecs/RawJsonlBlocks/RawJsonlBlocks.hpp"
#include "../Codecs/TradesGroupedDeltaQtydict/TradesGroupedDeltaQtyDict.hpp"
#include "../Codecs/XzJsonlBlocks/XzJsonlBlocks.hpp"
#include "../Codecs/ZstdJsonlBlocks/ZstdJsonlBlocks.hpp"

namespace hft_compressor::pipelines {
namespace {

constexpr auto kBackends = std::to_array<PipelineBackend>({
    {"std.zstd_jsonl_blocks",
     "hfc.zstd_jsonl_blocks",
     codecs::zstd_jsonl_blocks::compress,
     codecs::zstd_jsonl_blocks::inspectArtifact,
     codecs::zstd_jsonl_blocks::decodeFile,
     codecs::zstd_jsonl_blocks::decode},
    {"std.raw_jsonl_blocks",
     "hfr.raw_jsonl_blocks",
     codecs::raw_jsonl_blocks::compress,
     codecs::raw_jsonl_blocks::inspectArtifact,
     codecs::raw_jsonl_blocks::decodeFile,
     codecs::raw_jsonl_blocks::decode},
    {"std.lz4_jsonl_blocks",
     "hfc.lz4_jsonl_blocks",
     codecs::lz4_jsonl_blocks::compress,
     codecs::lz4_jsonl_blocks::inspectArtifact,
     codecs::lz4_jsonl_blocks::decodeFile,
     codecs::lz4_jsonl_blocks::decode},
    {"std.brotli_jsonl_blocks",
     "hfc.brotli_jsonl_blocks",
     codecs::brotli_jsonl_blocks::compress,
     codecs::brotli_jsonl_blocks::inspectArtifact,
     codecs::brotli_jsonl_blocks::decodeFile,
     codecs::brotli_jsonl_blocks::decode},
    {"std.xz_jsonl_blocks",
     "hfc.xz_jsonl_blocks",
     codecs::xz_jsonl_blocks::compress,
     codecs::xz_jsonl_blocks::inspectArtifact,
     codecs::xz_jsonl_blocks::decodeFile,
     codecs::xz_jsonl_blocks::decode},
    {"std.gzip_jsonl_blocks",
     "hfc.gzip_jsonl_blocks",
     codecs::gzip_jsonl_blocks::compress,
     codecs::gzip_jsonl_blocks::inspectArtifact,
     codecs::gzip_jsonl_blocks::decodeFile,
     codecs::gzip_jsonl_blocks::decode},

    {"hftmac.trades_grouped_delta_qtydict_math",
     "hftmac.trades_grouped_delta_qtydict.math",
     codecs::trades_grouped_delta_qtydict::compress,
     codecs::trades_grouped_delta_qtydict::inspectArtifact,
     codecs::trades_grouped_delta_qtydict::decodeFile,
     codecs::trades_grouped_delta_qtydict::decode},
    {"hftmac.bookticker_delta_mask",
     "hftmac.bookticker_delta_mask",
     codecs::bookticker_delta_mask::compress,
     codecs::bookticker_delta_mask::inspectArtifact,
     codecs::bookticker_delta_mask::decodeFile,
     codecs::bookticker_delta_mask::decode},
    {"hftmac.depth_ladder_offset",
     "hftmac.depth_ladder_offset",
     codecs::depth_ladder_offset::compress,
     codecs::depth_ladder_offset::inspectArtifact,
     codecs::depth_ladder_offset::decodeFile,
     codecs::depth_ladder_offset::decode},
    {"hftmac.trades_grouped_delta_qtydict_ac16_ctx0", "hftmac.trades_grouped_delta_qtydict.entropy", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.trades_grouped_delta_qtydict_ac16_ctx8", "hftmac.trades_grouped_delta_qtydict.entropy", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.trades_grouped_delta_qtydict_ac16_ctx12", "hftmac.trades_grouped_delta_qtydict.entropy", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.trades_grouped_delta_qtydict_ac32_ctx8", "hftmac.trades_grouped_delta_qtydict.entropy", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.trades_grouped_delta_qtydict_range_byte_ctx8", "hftmac.trades_grouped_delta_qtydict.entropy", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.trades_grouped_delta_qtydict_rans_byte_static", "hftmac.trades_grouped_delta_qtydict.entropy", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.bookticker_delta_mask_ac16_ctx0", "hftmac.bookticker_delta_mask.entropy", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.bookticker_delta_mask_ac16_ctx8", "hftmac.bookticker_delta_mask.entropy", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.bookticker_delta_mask_ac16_ctx12", "hftmac.bookticker_delta_mask.entropy", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.bookticker_delta_mask_ac32_ctx8", "hftmac.bookticker_delta_mask.entropy", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.bookticker_delta_mask_range_byte_ctx8", "hftmac.bookticker_delta_mask.entropy", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.bookticker_delta_mask_rans_byte_static", "hftmac.bookticker_delta_mask.entropy", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.depth_ladder_offset_ac16_ctx0", "hftmac.depth_ladder_offset.entropy", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.depth_ladder_offset_ac16_ctx8", "hftmac.depth_ladder_offset.entropy", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.depth_ladder_offset_ac16_ctx12", "hftmac.depth_ladder_offset.entropy", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.depth_ladder_offset_ac32_ctx8", "hftmac.depth_ladder_offset.entropy", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.depth_ladder_offset_range_byte_ctx8", "hftmac.depth_ladder_offset.entropy", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
    {"hftmac.depth_ladder_offset_rans_byte_static", "hftmac.depth_ladder_offset.entropy", codecs::entropy_hftmac::compress, codecs::entropy_hftmac::inspectArtifact, codecs::entropy_hftmac::decodeFile, codecs::entropy_hftmac::decode},
});

}  // namespace

const PipelineBackend* findBackend(std::string_view pipelineId) noexcept {
    for (const auto& backend : kBackends) {
        if (backend.pipelineId == pipelineId) return &backend;
    }
    return nullptr;
}

}  // namespace hft_compressor::pipelines

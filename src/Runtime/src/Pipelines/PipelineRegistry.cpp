#include "hft_compressor/Pipeline.hpp"

#include <algorithm>
#include <array>

namespace hft_compressor {
namespace {

constexpr PipelineAvailability zstdAvailability() noexcept {
#if HFT_COMPRESSOR_WITH_ZSTD
    return PipelineAvailability::Available;
#else
    return PipelineAvailability::DependencyUnavailable;
#endif
}

constexpr std::string_view zstdAvailabilityReason() noexcept {
#if HFT_COMPRESSOR_WITH_ZSTD
    return "";
#else
    return "libzstd was not found at configure time";
#endif
}

constexpr PipelineAvailability lz4Availability() noexcept {
#if HFT_COMPRESSOR_WITH_LZ4
    return PipelineAvailability::Available;
#else
    return PipelineAvailability::DependencyUnavailable;
#endif
}

constexpr std::string_view lz4AvailabilityReason() noexcept {
#if HFT_COMPRESSOR_WITH_LZ4
    return "";
#else
    return "liblz4 was not found at configure time";
#endif
}

constexpr PipelineAvailability brotliAvailability() noexcept {
#if HFT_COMPRESSOR_WITH_BROTLI
    return PipelineAvailability::Available;
#else
    return PipelineAvailability::DependencyUnavailable;
#endif
}

constexpr std::string_view brotliAvailabilityReason() noexcept {
#if HFT_COMPRESSOR_WITH_BROTLI
    return "";
#else
    return "brotli encoder/decoder libraries were not found at configure time";
#endif
}

constexpr PipelineAvailability lzmaAvailability() noexcept {
#if HFT_COMPRESSOR_WITH_LZMA
    return PipelineAvailability::Available;
#else
    return PipelineAvailability::DependencyUnavailable;
#endif
}

constexpr std::string_view lzmaAvailabilityReason() noexcept {
#if HFT_COMPRESSOR_WITH_LZMA
    return "";
#else
    return "liblzma was not found at configure time";
#endif
}

constexpr PipelineAvailability zlibAvailability() noexcept {
#if HFT_COMPRESSOR_WITH_ZLIB
    return PipelineAvailability::Available;
#else
    return PipelineAvailability::DependencyUnavailable;
#endif
}

constexpr std::string_view zlibAvailabilityReason() noexcept {
#if HFT_COMPRESSOR_WITH_ZLIB
    return "";
#else
    return "zlib was not found at configure time";
#endif
}

// Single-current binary corpus & lab pipelines:
// - Mandatory baselines (zstd, lz4, brotli, xz/lzma + raw/gzip) over binary slices
// -Custom HFT-MAC binary stream-family transforms + division-free shift arithmetic & byte rANS
constexpr auto kPipelines = std::to_array<PipelineDescriptor>({
    {"std.zstd_blocks", "Zstd binary blocks", "all", "binary_blocks", "raw_binary", "zstd", "archive", "c++", zstdAvailability(), zstdAvailabilityReason(), "zstd", ".hfc", "binary_blocks,record_batches,streaming_decode,file_streaming,baseline,archive,replay_ready,ranked_when_verified"},
    {"std.raw_blocks", "Raw binary blocks", "all", "binary_blocks", "raw_binary", "none", "research", "c++", PipelineAvailability::Available, "", "raw-binary", ".hfr", "binary_blocks,record_batches,streaming_decode,file_streaming,lab_baseline,replay_ready"},
    {"std.lz4_blocks", "LZ4 binary blocks", "all", "binary_blocks", "raw_binary", "lz4", "live", "c++", lz4Availability(), lz4AvailabilityReason(), "lz4", ".hfc", "binary_blocks,record_batches,streaming_decode,file_streaming,baseline,replay_ready,ranked_when_verified"},
    {"std.brotli_blocks", "Brotli binary blocks", "all", "binary_blocks", "raw_binary", "brotli", "archive", "c++", brotliAvailability(), brotliAvailabilityReason(), "brotli", ".hfc", "binary_blocks,record_batches,streaming_decode,file_streaming,baseline,replay_ready,ranked_when_verified"},
    {"std.xz_blocks", "XZ binary blocks", "all", "binary_blocks", "raw_binary", "xz", "archive", "c++", lzmaAvailability(), lzmaAvailabilityReason(), "xz", ".hfc", "binary_blocks,record_batches,streaming_decode,file_streaming,baseline,replay_ready,ranked_when_verified"},
    {"std.gzip_blocks", "Gzip binary blocks", "all", "binary_blocks", "raw_binary", "gzip", "archive", "c++", zlibAvailability(), zlibAvailabilityReason(), "gzip", ".hfc", "binary_blocks,record_batches,streaming_decode,file_streaming,baseline,replay_ready,ranked_when_verified"},
    {"hftmac.trades_grouped_delta_qtydict_math", "HFT-MAC trades grouped delta qtydict math", "trades", "grouped_delta_qtydict", "binary_gcd_scaled_timestamp_price_groups_bitflags_qtydict_plane_delta", "none", "replay", "c++", PipelineAvailability::Available, "", "trades-grouped-delta-qtydict-math", ".cxcef", "hftmac,math,gcd_scale,bitpacked_flags,price_group_count_one,encoded_binary,encoded_json,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.bookticker_delta_mask", "HFT-MAC bookticker delta mask", "bookticker", "delta_mask", "binary_base_state_bid_spread_packed_change_mask_zigzag_plane_delta", "none", "replay", "c++", PipelineAvailability::Available, "", "bookticker-delta-mask", ".cxcef", "hftmac,math,delta_mask,bid_spread,packed_change_mask,encoded_binary,encoded_json,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.depth_ladder_offset", "HFT-MAC depth ladder offset", "depth", "ladder_offset", "binary_side_runs_offset_gaps_delete_flat_book_hotqty_plane_delta", "none", "replay", "c++", PipelineAvailability::Available, "", "depth-ladder-offset", ".cxcef", "hftmac,math,ladder_offset,side_runs,first_offset_gaps,stateful_book,delete_bits,side_hot_qty,encoded_binary,encoded_json,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.trades_grouped_delta_qtydict_ac16_ctx0", "HFT-MAC trades shift-AC ctx0", "trades", "grouped_delta_qtydict_entropy", "binary_gcd_scaled_timestamp_price_groups_bitflags_qtydict_plane_delta", "ac16_ctx0", "archive", "c++", PipelineAvailability::Available, "", "trades-grouped-delta-qtydict-ac16-ctx0", ".cxcef", "hftmac,entropy,ac16,ctx0,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.trades_grouped_delta_qtydict_ac16_ctx8", "HFT-MAC trades shift-AC ctx8", "trades", "grouped_delta_qtydict_entropy", "binary_gcd_scaled_timestamp_price_groups_bitflags_qtydict_plane_delta", "ac16_ctx8", "archive", "c++", PipelineAvailability::Available, "", "trades-grouped-delta-qtydict-ac16-ctx8", ".cxcef", "hftmac,entropy,ac16,ctx8,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.trades_grouped_delta_qtydict_ac16_ctx12", "HFT-MAC trades shift-AC ctx12", "trades", "grouped_delta_qtydict_entropy", "binary_gcd_scaled_timestamp_price_groups_bitflags_qtydict_plane_delta", "ac16_ctx12", "archive", "c++", PipelineAvailability::Available, "", "trades-grouped-delta-qtydict-ac16-ctx12", ".cxcef", "hftmac,entropy,ac16,ctx12,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.trades_grouped_delta_qtydict_ac32_ctx8", "HFT-MAC trades shift-AC slow ctx8", "trades", "grouped_delta_qtydict_entropy", "binary_gcd_scaled_timestamp_price_groups_bitflags_qtydict_plane_delta", "ac32_ctx8", "archive", "c++", PipelineAvailability::Available, "", "trades-grouped-delta-qtydict-ac32-ctx8", ".cxcef", "hftmac,entropy,ac32,ctx8,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.trades_grouped_delta_qtydict_rans_byte_static", "HFT-MAC trades rANS byte static", "trades", "grouped_delta_qtydict_entropy", "binary_gcd_scaled_timestamp_price_groups_bitflags_qtydict_plane_delta", "rans_byte_static", "archive", "c++", PipelineAvailability::Available, "", "trades-grouped-delta-qtydict-rans-byte-static", ".cxcef", "hftmac,entropy,rans,static,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.bookticker_delta_mask_ac16_ctx0", "HFT-MAC bookticker shift-AC ctx0", "bookticker", "delta_mask_entropy", "binary_base_state_bid_spread_packed_change_mask_zigzag_plane_delta", "ac16_ctx0", "archive", "c++", PipelineAvailability::Available, "", "bookticker-delta-mask-ac16-ctx0", ".cxcef", "hftmac,entropy,ac16,ctx0,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.bookticker_delta_mask_ac16_ctx8", "HFT-MAC bookticker shift-AC ctx8", "bookticker", "delta_mask_entropy", "binary_base_state_bid_spread_packed_change_mask_zigzag_plane_delta", "ac16_ctx8", "archive", "c++", PipelineAvailability::Available, "", "bookticker-delta-mask-ac16-ctx8", ".cxcef", "hftmac,entropy,ac16,ctx8,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.bookticker_delta_mask_ac16_ctx12", "HFT-MAC bookticker shift-AC ctx12", "bookticker", "delta_mask_entropy", "binary_base_state_bid_spread_packed_change_mask_zigzag_plane_delta", "ac16_ctx12", "archive", "c++", PipelineAvailability::Available, "", "bookticker-delta-mask-ac16-ctx12", ".cxcef", "hftmac,entropy,ac16,ctx12,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.bookticker_delta_mask_ac32_ctx8", "HFT-MAC bookticker shift-AC slow ctx8", "bookticker", "delta_mask_entropy", "binary_base_state_bid_spread_packed_change_mask_zigzag_plane_delta", "ac32_ctx8", "archive", "c++", PipelineAvailability::Available, "", "bookticker-delta-mask-ac32-ctx8", ".cxcef", "hftmac,entropy,ac32,ctx8,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.bookticker_delta_mask_rans_byte_static", "HFT-MAC bookticker rANS byte static", "bookticker", "delta_mask_entropy", "binary_base_state_bid_spread_packed_change_mask_zigzag_plane_delta", "rans_byte_static", "archive", "c++", PipelineAvailability::Available, "", "bookticker-delta-mask-rans-byte-static", ".cxcef", "hftmac,entropy,rans,static,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.depth_ladder_offset_ac16_ctx0", "HFT-MAC depth shift-AC ctx0", "depth", "ladder_offset_entropy", "binary_side_runs_offset_gaps_delete_flat_book_hotqty_plane_delta", "ac16_ctx0", "archive", "c++", PipelineAvailability::Available, "", "depth-ladder-offset-ac16-ctx0", ".cxcef", "hftmac,entropy,ac16,ctx0,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.depth_ladder_offset_ac16_ctx8", "HFT-MAC depth shift-AC ctx8", "depth", "ladder_offset_entropy", "binary_side_runs_offset_gaps_delete_flat_book_hotqty_plane_delta", "ac16_ctx8", "archive", "c++", PipelineAvailability::Available, "", "depth-ladder-offset-ac16-ctx8", ".cxcef", "hftmac,entropy,ac16,ctx8,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.depth_ladder_offset_ac16_ctx12", "HFT-MAC depth shift-AC ctx12", "depth", "ladder_offset_entropy", "binary_side_runs_offset_gaps_delete_flat_book_hotqty_plane_delta", "ac16_ctx12", "archive", "c++", PipelineAvailability::Available, "", "depth-ladder-offset-ac16-ctx12", ".cxcef", "hftmac,entropy,ac16,ctx12,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.depth_ladder_offset_ac32_ctx8", "HFT-MAC depth shift-AC slow ctx8", "depth", "ladder_offset_entropy", "binary_side_runs_offset_gaps_delete_flat_book_hotqty_plane_delta", "ac32_ctx8", "archive", "c++", PipelineAvailability::Available, "", "depth-ladder-offset-ac32-ctx8", ".cxcef", "hftmac,entropy,ac32,ctx8,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.depth_ladder_offset_rans_byte_static", "HFT-MAC depth rANS byte static", "depth", "ladder_offset_entropy", "binary_side_runs_offset_gaps_delete_flat_book_hotqty_plane_delta", "rans_byte_static", "archive", "c++", PipelineAvailability::Available, "", "depth-ladder-offset-rans-byte-static", ".cxcef", "hftmac,entropy,rans,static,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
});

}  // namespace

std::span<const PipelineDescriptor> listPipelines() noexcept {
    return kPipelines;
}

const PipelineDescriptor* findPipeline(std::string_view id) noexcept {
    const auto it = std::find_if(kPipelines.begin(), kPipelines.end(), [id](const PipelineDescriptor& pipeline) {
        return pipeline.id == id;
    });
    return it == kPipelines.end() ? nullptr : &(*it);
}

std::string_view pipelineAvailabilityToString(PipelineAvailability availability) noexcept {
    switch (availability) {
        case PipelineAvailability::Available: return "available";
        case PipelineAvailability::DependencyUnavailable: return "dependency_unavailable";
        case PipelineAvailability::NotImplemented: return "not_implemented";
    }
    return "unknown";
}

}  // namespace hft_compressor

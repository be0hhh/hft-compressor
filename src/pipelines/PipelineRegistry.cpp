#include "hft_compressor/pipeline.hpp"

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
constexpr auto kPipelines = std::to_array<PipelineDescriptor>({
    {"std.zstd_jsonl_blocks_v1", "Zstd JSONL blocks v1", "all", "jsonl_blocks", "raw_jsonl", "zstd", "archive", "c++", zstdAvailability(), zstdAvailabilityReason(), "zstd", ".hfc", "jsonl_blocks,record_batches,streaming_decode,file_streaming,archive,replay_ready"},
    {"std.raw_jsonl_blocks_v1", "Raw JSONL blocks v1", "all", "jsonl_blocks", "raw_jsonl", "none", "research", "c++", PipelineAvailability::Available, "", "raw-jsonl", ".hfr", "jsonl_blocks,record_batches,streaming_decode,file_streaming,lab_baseline,replay_ready"},
    {"std.lz4_jsonl_blocks_v1", "LZ4 JSONL blocks v1", "all", "jsonl_blocks", "raw_jsonl", "lz4", "live", "c++", lz4Availability(), lz4AvailabilityReason(), "lz4", ".hfc", "jsonl_blocks,record_batches,streaming_decode,file_streaming,baseline,replay_ready,ranked_when_verified"},
    {"std.brotli_jsonl_blocks_v1", "Brotli JSONL blocks v1", "all", "jsonl_blocks", "raw_jsonl", "brotli", "archive", "c++", brotliAvailability(), brotliAvailabilityReason(), "brotli", ".hfc", "jsonl_blocks,record_batches,streaming_decode,file_streaming,baseline,replay_ready,ranked_when_verified"},
    {"std.xz_jsonl_blocks_v1", "XZ JSONL blocks v1", "all", "jsonl_blocks", "raw_jsonl", "xz", "archive", "c++", lzmaAvailability(), lzmaAvailabilityReason(), "xz", ".hfc", "jsonl_blocks,record_batches,streaming_decode,file_streaming,baseline,replay_ready,ranked_when_verified"},
    {"std.gzip_jsonl_blocks_v1", "Gzip JSONL blocks v1", "all", "jsonl_blocks", "raw_jsonl", "gzip", "archive", "c++", zlibAvailability(), zlibAvailabilityReason(), "gzip", ".hfc", "jsonl_blocks,record_batches,streaming_decode,file_streaming,baseline,replay_ready,ranked_when_verified"},
    {"hftmac.trades_grouped_delta_qtydict_math_v3", "HFT-MAC trades grouped delta qtydict math v3", "trades", "grouped_delta_qtydict", "gcd_scaled_timestamp_price_groups_bitflags_pgc1_qtydict", "none", "replay", "c++", PipelineAvailability::Available, "", "trades-grouped-delta-qtydict-math-v3", ".cxcef", "hftmac,math_v3,gcd_scale,bitpacked_flags,price_group_count_one,encoded_binary,encoded_json,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.bookticker_delta_mask_v2", "HFT-MAC bookticker delta mask v2", "bookticker", "delta_mask", "base_state_bid_spread_packed_change_mask_zigzag", "none", "replay", "c++", PipelineAvailability::Available, "", "bookticker-delta-mask-v2", ".cxcef", "hftmac,math_v2,delta_mask,bid_spread,packed_change_mask,encoded_binary,encoded_json,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.depth_ladder_offset_v3", "HFT-MAC depth ladder offset v3", "depth", "ladder_offset", "side_runs_first_offset_gaps_delete_side_hotqty", "none", "replay", "c++", PipelineAvailability::Available, "", "depth-ladder-offset-v3", ".cxcef", "hftmac,math_v3,ladder_offset,side_runs,first_offset_gaps,stateful_book,delete_bits,side_hot_qty,encoded_binary,encoded_json,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.trades_grouped_delta_qtydict_ac16_ctx0_v1", "HFT-MAC trades AC16 ctx0 v1", "trades", "grouped_delta_qtydict_entropy", "gcd_scaled_timestamp_price_groups_bitflags_pgc1_qtydict", "ac16_ctx0", "archive", "c++", PipelineAvailability::Available, "", "trades-grouped-delta-qtydict-ac16-ctx0", ".cxcef", "hftmac,entropy,ac16,ctx0,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.trades_grouped_delta_qtydict_ac16_ctx8_v1", "HFT-MAC trades AC16 ctx8 v1", "trades", "grouped_delta_qtydict_entropy", "gcd_scaled_timestamp_price_groups_bitflags_pgc1_qtydict", "ac16_ctx8", "archive", "c++", PipelineAvailability::Available, "", "trades-grouped-delta-qtydict-ac16-ctx8", ".cxcef", "hftmac,entropy,ac16,ctx8,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.trades_grouped_delta_qtydict_ac16_ctx12_v1", "HFT-MAC trades AC16 ctx12 v1", "trades", "grouped_delta_qtydict_entropy", "gcd_scaled_timestamp_price_groups_bitflags_pgc1_qtydict", "ac16_ctx12", "archive", "c++", PipelineAvailability::Available, "", "trades-grouped-delta-qtydict-ac16-ctx12", ".cxcef", "hftmac,entropy,ac16,ctx12,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.trades_grouped_delta_qtydict_ac32_ctx8_v1", "HFT-MAC trades AC32 ctx8 v1", "trades", "grouped_delta_qtydict_entropy", "gcd_scaled_timestamp_price_groups_bitflags_pgc1_qtydict", "ac32_ctx8", "archive", "c++", PipelineAvailability::Available, "", "trades-grouped-delta-qtydict-ac32-ctx8", ".cxcef", "hftmac,entropy,ac32,ctx8,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.trades_grouped_delta_qtydict_range_byte_ctx8_v1", "HFT-MAC trades range byte ctx8 v1", "trades", "grouped_delta_qtydict_entropy", "gcd_scaled_timestamp_price_groups_bitflags_pgc1_qtydict", "range_byte_ctx8", "archive", "c++", PipelineAvailability::Available, "", "trades-grouped-delta-qtydict-range-byte-ctx8", ".cxcef", "hftmac,entropy,range,ctx8,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.trades_grouped_delta_qtydict_rans_byte_static_v1", "HFT-MAC trades rANS byte static v1", "trades", "grouped_delta_qtydict_entropy", "gcd_scaled_timestamp_price_groups_bitflags_pgc1_qtydict", "rans_byte_static", "archive", "c++", PipelineAvailability::Available, "", "trades-grouped-delta-qtydict-rans-byte-static", ".cxcef", "hftmac,entropy,rans,static,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.bookticker_delta_mask_ac16_ctx0_v1", "HFT-MAC bookticker AC16 ctx0 v1", "bookticker", "delta_mask_entropy", "base_state_bid_spread_packed_change_mask_zigzag", "ac16_ctx0", "archive", "c++", PipelineAvailability::Available, "", "bookticker-delta-mask-ac16-ctx0", ".cxcef", "hftmac,entropy,ac16,ctx0,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.bookticker_delta_mask_ac16_ctx8_v1", "HFT-MAC bookticker AC16 ctx8 v1", "bookticker", "delta_mask_entropy", "base_state_bid_spread_packed_change_mask_zigzag", "ac16_ctx8", "archive", "c++", PipelineAvailability::Available, "", "bookticker-delta-mask-ac16-ctx8", ".cxcef", "hftmac,entropy,ac16,ctx8,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.bookticker_delta_mask_ac16_ctx12_v1", "HFT-MAC bookticker AC16 ctx12 v1", "bookticker", "delta_mask_entropy", "base_state_bid_spread_packed_change_mask_zigzag", "ac16_ctx12", "archive", "c++", PipelineAvailability::Available, "", "bookticker-delta-mask-ac16-ctx12", ".cxcef", "hftmac,entropy,ac16,ctx12,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.bookticker_delta_mask_ac32_ctx8_v1", "HFT-MAC bookticker AC32 ctx8 v1", "bookticker", "delta_mask_entropy", "base_state_bid_spread_packed_change_mask_zigzag", "ac32_ctx8", "archive", "c++", PipelineAvailability::Available, "", "bookticker-delta-mask-ac32-ctx8", ".cxcef", "hftmac,entropy,ac32,ctx8,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.bookticker_delta_mask_range_byte_ctx8_v1", "HFT-MAC bookticker range byte ctx8 v1", "bookticker", "delta_mask_entropy", "base_state_bid_spread_packed_change_mask_zigzag", "range_byte_ctx8", "archive", "c++", PipelineAvailability::Available, "", "bookticker-delta-mask-range-byte-ctx8", ".cxcef", "hftmac,entropy,range,ctx8,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.bookticker_delta_mask_rans_byte_static_v1", "HFT-MAC bookticker rANS byte static v1", "bookticker", "delta_mask_entropy", "base_state_bid_spread_packed_change_mask_zigzag", "rans_byte_static", "archive", "c++", PipelineAvailability::Available, "", "bookticker-delta-mask-rans-byte-static", ".cxcef", "hftmac,entropy,rans,static,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.depth_ladder_offset_ac16_ctx0_v1", "HFT-MAC depth AC16 ctx0 v1", "depth", "ladder_offset_entropy", "side_runs_first_offset_gaps_delete_side_hotqty", "ac16_ctx0", "archive", "c++", PipelineAvailability::Available, "", "depth-ladder-offset-ac16-ctx0", ".cxcef", "hftmac,entropy,ac16,ctx0,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.depth_ladder_offset_ac16_ctx8_v1", "HFT-MAC depth AC16 ctx8 v1", "depth", "ladder_offset_entropy", "side_runs_first_offset_gaps_delete_side_hotqty", "ac16_ctx8", "archive", "c++", PipelineAvailability::Available, "", "depth-ladder-offset-ac16-ctx8", ".cxcef", "hftmac,entropy,ac16,ctx8,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.depth_ladder_offset_ac16_ctx12_v1", "HFT-MAC depth AC16 ctx12 v1", "depth", "ladder_offset_entropy", "side_runs_first_offset_gaps_delete_side_hotqty", "ac16_ctx12", "archive", "c++", PipelineAvailability::Available, "", "depth-ladder-offset-ac16-ctx12", ".cxcef", "hftmac,entropy,ac16,ctx12,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.depth_ladder_offset_ac32_ctx8_v1", "HFT-MAC depth AC32 ctx8 v1", "depth", "ladder_offset_entropy", "side_runs_first_offset_gaps_delete_side_hotqty", "ac32_ctx8", "archive", "c++", PipelineAvailability::Available, "", "depth-ladder-offset-ac32-ctx8", ".cxcef", "hftmac,entropy,ac32,ctx8,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.depth_ladder_offset_range_byte_ctx8_v1", "HFT-MAC depth range byte ctx8 v1", "depth", "ladder_offset_entropy", "side_runs_first_offset_gaps_delete_side_hotqty", "range_byte_ctx8", "archive", "c++", PipelineAvailability::Available, "", "depth-ladder-offset-range-byte-ctx8", ".cxcef", "hftmac,entropy,range,ctx8,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.depth_ladder_offset_rans_byte_static_v1", "HFT-MAC depth rANS byte static v1", "depth", "ladder_offset_entropy", "side_runs_first_offset_gaps_delete_side_hotqty", "rans_byte_static", "archive", "c++", PipelineAvailability::Available, "", "depth-ladder-offset-rans-byte-static", ".cxcef", "hftmac,entropy,rans,static,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"py.zstandard_jsonl_v1", "Python zstandard JSONL v1", "all", "jsonl_blocks", "raw_jsonl", "zstandard", "research", "python", PipelineAvailability::NotImplemented, "python lab runner is planned", "py-zstandard"},
    {"py.lz4_jsonl_v1", "Python lz4 JSONL v1", "all", "jsonl_blocks", "raw_jsonl", "lz4", "research", "python", PipelineAvailability::NotImplemented, "python lab runner is planned", "py-lz4"},
    {"py.lzma_jsonl_v1", "Python lzma JSONL v1", "all", "jsonl_blocks", "raw_jsonl", "lzma", "research", "python", PipelineAvailability::NotImplemented, "python lab runner is planned", "py-lzma"},
    {"trade.delta_varint_v1", "Trade delta VarInt v1", "trades", "trade_delta", "delta_varint", "none", "live", "c++", PipelineAvailability::NotImplemented, "trade transform is planned", "trade-delta-varint"},
    {"l1.delta_varint_v1", "L1 delta VarInt v1", "bookticker", "l1_delta", "delta_varint", "none", "live", "c++", PipelineAvailability::NotImplemented, "l1 transform is planned", "l1-delta-varint"},
    {"depth.delta_varint_v1", "Depth delta VarInt v1", "depth", "depth_delta", "delta_varint", "none", "live", "c++", PipelineAvailability::NotImplemented, "depth transform is planned", "depth-delta-varint"},
    {"trade.delta_zstd_v1", "Trade delta + zstd v1", "trades", "trade_delta", "delta", "zstd", "archive", "c++", PipelineAvailability::NotImplemented, "trade delta pipeline is planned", "trade-delta-zstd"},
    {"l1.delta_zstd_v1", "L1 delta + zstd v1", "bookticker", "l1_delta", "delta", "zstd", "archive", "c++", PipelineAvailability::NotImplemented, "l1 delta pipeline is planned", "l1-delta-zstd"},
    {"depth.delta_zstd_v1", "Depth delta + zstd v1", "depth", "depth_delta", "delta", "zstd", "archive", "c++", PipelineAvailability::NotImplemented, "depth delta pipeline is planned", "depth-delta-zstd"},
    {"trade.columnar_delta_zstd_v1", "Trade columnar delta + zstd v1", "trades", "trade_columnar", "columnar_delta", "zstd", "archive", "c++", PipelineAvailability::NotImplemented, "trade columnar pipeline is planned", "trade-columnar-delta-zstd"},
    {"l1.columnar_delta_zstd_v1", "L1 columnar delta + zstd v1", "bookticker", "l1_columnar", "columnar_delta", "zstd", "archive", "c++", PipelineAvailability::NotImplemented, "l1 columnar pipeline is planned", "l1-columnar-delta-zstd"},
    {"depth.columnar_delta_zstd_v1", "Depth columnar delta + zstd v1", "depth", "depth_columnar", "columnar_delta", "zstd", "archive", "c++", PipelineAvailability::NotImplemented, "depth columnar pipeline is planned", "depth-columnar-delta-zstd"},
    {"trade.delta_lz4_v1", "Trade delta + lz4 v1", "trades", "trade_delta", "delta", "lz4", "live", "c++", PipelineAvailability::NotImplemented, "trade lz4 pipeline is planned", "trade-delta-lz4"},
    {"l1.delta_lz4_v1", "L1 delta + lz4 v1", "bookticker", "l1_delta", "delta", "lz4", "live", "c++", PipelineAvailability::NotImplemented, "l1 lz4 pipeline is planned", "l1-delta-lz4"},
    {"depth.delta_lz4_v1", "Depth delta + lz4 v1", "depth", "depth_delta", "delta", "lz4", "live", "c++", PipelineAvailability::NotImplemented, "depth lz4 pipeline is planned", "depth-delta-lz4"},
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



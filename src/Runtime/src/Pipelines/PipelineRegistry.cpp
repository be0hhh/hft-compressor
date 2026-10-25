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
constexpr auto kPipelines = std::to_array<PipelineDescriptor>({
    {"std.zstd_jsonl_blocks", "Zstd JSONL blocks", "all", "jsonl_blocks", "raw_jsonl", "zstd", "archive", "c++", zstdAvailability(), zstdAvailabilityReason(), "zstd", ".hfc", "jsonl_blocks,record_batches,streaming_decode,file_streaming,archive,replay_ready"},
    {"std.raw_jsonl_blocks", "Raw JSONL blocks", "all", "jsonl_blocks", "raw_jsonl", "none", "research", "c++", PipelineAvailability::Available, "", "raw-jsonl", ".hfr", "jsonl_blocks,record_batches,streaming_decode,file_streaming,lab_baseline,replay_ready"},
    {"std.lz4_jsonl_blocks", "LZ4 JSONL blocks", "all", "jsonl_blocks", "raw_jsonl", "lz4", "live", "c++", lz4Availability(), lz4AvailabilityReason(), "lz4", ".hfc", "jsonl_blocks,record_batches,streaming_decode,file_streaming,baseline,replay_ready,ranked_when_verified"},
    {"std.brotli_jsonl_blocks", "Brotli JSONL blocks", "all", "jsonl_blocks", "raw_jsonl", "brotli", "archive", "c++", brotliAvailability(), brotliAvailabilityReason(), "brotli", ".hfc", "jsonl_blocks,record_batches,streaming_decode,file_streaming,baseline,replay_ready,ranked_when_verified"},
    {"std.xz_jsonl_blocks", "XZ JSONL blocks", "all", "jsonl_blocks", "raw_jsonl", "xz", "archive", "c++", lzmaAvailability(), lzmaAvailabilityReason(), "xz", ".hfc", "jsonl_blocks,record_batches,streaming_decode,file_streaming,baseline,replay_ready,ranked_when_verified"},
    {"std.gzip_jsonl_blocks", "Gzip JSONL blocks", "all", "jsonl_blocks", "raw_jsonl", "gzip", "archive", "c++", zlibAvailability(), zlibAvailabilityReason(), "gzip", ".hfc", "jsonl_blocks,record_batches,streaming_decode,file_streaming,baseline,replay_ready,ranked_when_verified"},
    {"hftmac.trades_grouped_delta_qtydict_math", "HFT-MAC trades grouped delta qtydict math", "trades", "grouped_delta_qtydict", "gcd_scaled_timestamp_price_groups_bitflags_pgc1_qtydict", "none", "replay", "c++", PipelineAvailability::Available, "", "trades-grouped-delta-qtydict-math", ".cxcef", "hftmac,math,gcd_scale,bitpacked_flags,price_group_count_one,encoded_binary,encoded_json,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.bookticker_delta_mask", "HFT-MAC bookticker delta mask", "bookticker", "delta_mask", "base_state_bid_spread_packed_change_mask_zigzag", "none", "replay", "c++", PipelineAvailability::Available, "", "bookticker-delta-mask", ".cxcef", "hftmac,math,delta_mask,bid_spread,packed_change_mask,encoded_binary,encoded_json,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.depth_ladder_offset", "HFT-MAC depth ladder offset", "depth", "ladder_offset", "side_runs_first_offset_gaps_delete_side_hotqty", "none", "replay", "c++", PipelineAvailability::Available, "", "depth-ladder-offset", ".cxcef", "hftmac,math,ladder_offset,side_runs,first_offset_gaps,stateful_book,delete_bits,side_hot_qty,encoded_binary,encoded_json,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.trades_grouped_delta_qtydict_ac16_ctx0", "HFT-MAC trades AC16 ctx0", "trades", "grouped_delta_qtydict_entropy", "gcd_scaled_timestamp_price_groups_bitflags_pgc1_qtydict", "ac16_ctx0", "archive", "c++", PipelineAvailability::Available, "", "trades-grouped-delta-qtydict-ac16-ctx0", ".cxcef", "hftmac,entropy,ac16,ctx0,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.trades_grouped_delta_qtydict_ac16_ctx8", "HFT-MAC trades AC16 ctx8", "trades", "grouped_delta_qtydict_entropy", "gcd_scaled_timestamp_price_groups_bitflags_pgc1_qtydict", "ac16_ctx8", "archive", "c++", PipelineAvailability::Available, "", "trades-grouped-delta-qtydict-ac16-ctx8", ".cxcef", "hftmac,entropy,ac16,ctx8,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.trades_grouped_delta_qtydict_ac16_ctx12", "HFT-MAC trades AC16 ctx12", "trades", "grouped_delta_qtydict_entropy", "gcd_scaled_timestamp_price_groups_bitflags_pgc1_qtydict", "ac16_ctx12", "archive", "c++", PipelineAvailability::Available, "", "trades-grouped-delta-qtydict-ac16-ctx12", ".cxcef", "hftmac,entropy,ac16,ctx12,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.trades_grouped_delta_qtydict_ac32_ctx8", "HFT-MAC trades AC32 ctx8", "trades", "grouped_delta_qtydict_entropy", "gcd_scaled_timestamp_price_groups_bitflags_pgc1_qtydict", "ac32_ctx8", "archive", "c++", PipelineAvailability::Available, "", "trades-grouped-delta-qtydict-ac32-ctx8", ".cxcef", "hftmac,entropy,ac32,ctx8,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.trades_grouped_delta_qtydict_range_byte_ctx8", "HFT-MAC trades range byte ctx8", "trades", "grouped_delta_qtydict_entropy", "gcd_scaled_timestamp_price_groups_bitflags_pgc1_qtydict", "range_byte_ctx8", "archive", "c++", PipelineAvailability::Available, "", "trades-grouped-delta-qtydict-range-byte-ctx8", ".cxcef", "hftmac,entropy,range,ctx8,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.trades_grouped_delta_qtydict_rans_byte_static", "HFT-MAC trades rANS byte static", "trades", "grouped_delta_qtydict_entropy", "gcd_scaled_timestamp_price_groups_bitflags_pgc1_qtydict", "rans_byte_static", "archive", "c++", PipelineAvailability::Available, "", "trades-grouped-delta-qtydict-rans-byte-static", ".cxcef", "hftmac,entropy,rans,static,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.bookticker_delta_mask_ac16_ctx0", "HFT-MAC bookticker AC16 ctx0", "bookticker", "delta_mask_entropy", "base_state_bid_spread_packed_change_mask_zigzag", "ac16_ctx0", "archive", "c++", PipelineAvailability::Available, "", "bookticker-delta-mask-ac16-ctx0", ".cxcef", "hftmac,entropy,ac16,ctx0,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.bookticker_delta_mask_ac16_ctx8", "HFT-MAC bookticker AC16 ctx8", "bookticker", "delta_mask_entropy", "base_state_bid_spread_packed_change_mask_zigzag", "ac16_ctx8", "archive", "c++", PipelineAvailability::Available, "", "bookticker-delta-mask-ac16-ctx8", ".cxcef", "hftmac,entropy,ac16,ctx8,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.bookticker_delta_mask_ac16_ctx12", "HFT-MAC bookticker AC16 ctx12", "bookticker", "delta_mask_entropy", "base_state_bid_spread_packed_change_mask_zigzag", "ac16_ctx12", "archive", "c++", PipelineAvailability::Available, "", "bookticker-delta-mask-ac16-ctx12", ".cxcef", "hftmac,entropy,ac16,ctx12,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.bookticker_delta_mask_ac32_ctx8", "HFT-MAC bookticker AC32 ctx8", "bookticker", "delta_mask_entropy", "base_state_bid_spread_packed_change_mask_zigzag", "ac32_ctx8", "archive", "c++", PipelineAvailability::Available, "", "bookticker-delta-mask-ac32-ctx8", ".cxcef", "hftmac,entropy,ac32,ctx8,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.bookticker_delta_mask_range_byte_ctx8", "HFT-MAC bookticker range byte ctx8", "bookticker", "delta_mask_entropy", "base_state_bid_spread_packed_change_mask_zigzag", "range_byte_ctx8", "archive", "c++", PipelineAvailability::Available, "", "bookticker-delta-mask-range-byte-ctx8", ".cxcef", "hftmac,entropy,range,ctx8,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.bookticker_delta_mask_rans_byte_static", "HFT-MAC bookticker rANS byte static", "bookticker", "delta_mask_entropy", "base_state_bid_spread_packed_change_mask_zigzag", "rans_byte_static", "archive", "c++", PipelineAvailability::Available, "", "bookticker-delta-mask-rans-byte-static", ".cxcef", "hftmac,entropy,rans,static,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.depth_ladder_offset_ac16_ctx0", "HFT-MAC depth AC16 ctx0", "depth", "ladder_offset_entropy", "side_runs_first_offset_gaps_delete_side_hotqty", "ac16_ctx0", "archive", "c++", PipelineAvailability::Available, "", "depth-ladder-offset-ac16-ctx0", ".cxcef", "hftmac,entropy,ac16,ctx0,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.depth_ladder_offset_ac16_ctx8", "HFT-MAC depth AC16 ctx8", "depth", "ladder_offset_entropy", "side_runs_first_offset_gaps_delete_side_hotqty", "ac16_ctx8", "archive", "c++", PipelineAvailability::Available, "", "depth-ladder-offset-ac16-ctx8", ".cxcef", "hftmac,entropy,ac16,ctx8,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.depth_ladder_offset_ac16_ctx12", "HFT-MAC depth AC16 ctx12", "depth", "ladder_offset_entropy", "side_runs_first_offset_gaps_delete_side_hotqty", "ac16_ctx12", "archive", "c++", PipelineAvailability::Available, "", "depth-ladder-offset-ac16-ctx12", ".cxcef", "hftmac,entropy,ac16,ctx12,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.depth_ladder_offset_ac32_ctx8", "HFT-MAC depth AC32 ctx8", "depth", "ladder_offset_entropy", "side_runs_first_offset_gaps_delete_side_hotqty", "ac32_ctx8", "archive", "c++", PipelineAvailability::Available, "", "depth-ladder-offset-ac32-ctx8", ".cxcef", "hftmac,entropy,ac32,ctx8,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.depth_ladder_offset_range_byte_ctx8", "HFT-MAC depth range byte ctx8", "depth", "ladder_offset_entropy", "side_runs_first_offset_gaps_delete_side_hotqty", "range_byte_ctx8", "archive", "c++", PipelineAvailability::Available, "", "depth-ladder-offset-range-byte-ctx8", ".cxcef", "hftmac,entropy,range,ctx8,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"hftmac.depth_ladder_offset_rans_byte_static", "HFT-MAC depth rANS byte static", "depth", "ladder_offset_entropy", "side_runs_first_offset_gaps_delete_side_hotqty", "rans_byte_static", "archive", "c++", PipelineAvailability::Available, "", "depth-ladder-offset-rans-byte-static", ".cxcef", "hftmac,entropy,rans,static,record_batches,streaming_decode,file_streaming,replay_ready,ranked_when_verified"},
    {"py.zstandard_jsonl", "Python zstandard JSONL", "all", "jsonl_blocks", "raw_jsonl", "zstandard", "research", "python", PipelineAvailability::NotImplemented, "python lab runner is planned", "py-zstandard"},
    {"py.lz4_jsonl", "Python lz4 JSONL", "all", "jsonl_blocks", "raw_jsonl", "lz4", "research", "python", PipelineAvailability::NotImplemented, "python lab runner is planned", "py-lz4"},
    {"py.lzma_jsonl", "Python lzma JSONL", "all", "jsonl_blocks", "raw_jsonl", "lzma", "research", "python", PipelineAvailability::NotImplemented, "python lab runner is planned", "py-lzma"},
    {"trade.delta_varint", "Trade delta VarInt", "trades", "trade_delta", "delta_varint", "none", "live", "c++", PipelineAvailability::NotImplemented, "trade transform is planned", "trade-delta-varint"},
    {"l1.delta_varint", "L1 delta VarInt", "bookticker", "l1_delta", "delta_varint", "none", "live", "c++", PipelineAvailability::NotImplemented, "l1 transform is planned", "l1-delta-varint"},
    {"depth.delta_varint", "Depth delta VarInt", "depth", "depth_delta", "delta_varint", "none", "live", "c++", PipelineAvailability::NotImplemented, "depth transform is planned", "depth-delta-varint"},
    {"trade.delta_zstd", "Trade delta + zstd", "trades", "trade_delta", "delta", "zstd", "archive", "c++", PipelineAvailability::NotImplemented, "trade delta pipeline is planned", "trade-delta-zstd"},
    {"l1.delta_zstd", "L1 delta + zstd", "bookticker", "l1_delta", "delta", "zstd", "archive", "c++", PipelineAvailability::NotImplemented, "l1 delta pipeline is planned", "l1-delta-zstd"},
    {"depth.delta_zstd", "Depth delta + zstd", "depth", "depth_delta", "delta", "zstd", "archive", "c++", PipelineAvailability::NotImplemented, "depth delta pipeline is planned", "depth-delta-zstd"},
    {"trade.columnar_delta_zstd", "Trade columnar delta + zstd", "trades", "trade_columnar", "columnar_delta", "zstd", "archive", "c++", PipelineAvailability::NotImplemented, "trade columnar pipeline is planned", "trade-columnar-delta-zstd"},
    {"l1.columnar_delta_zstd", "L1 columnar delta + zstd", "bookticker", "l1_columnar", "columnar_delta", "zstd", "archive", "c++", PipelineAvailability::NotImplemented, "l1 columnar pipeline is planned", "l1-columnar-delta-zstd"},
    {"depth.columnar_delta_zstd", "Depth columnar delta + zstd", "depth", "depth_columnar", "columnar_delta", "zstd", "archive", "c++", PipelineAvailability::NotImplemented, "depth columnar pipeline is planned", "depth-columnar-delta-zstd"},
    {"trade.delta_lz4", "Trade delta + lz4", "trades", "trade_delta", "delta", "lz4", "live", "c++", PipelineAvailability::NotImplemented, "trade lz4 pipeline is planned", "trade-delta-lz4"},
    {"l1.delta_lz4", "L1 delta + lz4", "bookticker", "l1_delta", "delta", "lz4", "live", "c++", PipelineAvailability::NotImplemented, "l1 lz4 pipeline is planned", "l1-delta-lz4"},
    {"depth.delta_lz4", "Depth delta + lz4", "depth", "depth_delta", "delta", "lz4", "live", "c++", PipelineAvailability::NotImplemented, "depth lz4 pipeline is planned", "depth-delta-lz4"},
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



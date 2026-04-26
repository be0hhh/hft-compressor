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

constexpr std::array<PipelineDescriptor, 26> kPipelines{{
    {"std.zstd_jsonl_blocks_v1", "Zstd JSONL blocks v1", "all", "jsonl_blocks", "raw_jsonl", "zstd", "archive", "c++", zstdAvailability(), zstdAvailabilityReason(), "zstd"},
    {"std.lz4_jsonl_blocks_v1", "LZ4 JSONL blocks v1", "all", "jsonl_blocks", "raw_jsonl", "lz4", "live", "c++", PipelineAvailability::NotImplemented, "lz4 baseline is planned", "lz4"},
    {"std.brotli_jsonl_blocks_v1", "Brotli JSONL blocks v1", "all", "jsonl_blocks", "raw_jsonl", "brotli", "archive", "c++", PipelineAvailability::NotImplemented, "brotli baseline is planned", "brotli"},
    {"std.xz_jsonl_blocks_v1", "XZ JSONL blocks v1", "all", "jsonl_blocks", "raw_jsonl", "xz", "archive", "c++", PipelineAvailability::NotImplemented, "xz baseline is planned", "xz"},
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
    {"custom.ac_bin16_ctx0_v1", "AC BIN16 CTX0 v1", "all", "delta_varint_bits", "delta_varint", "ac_bin16_ctx0", "archive", "c++", PipelineAvailability::NotImplemented, "arithmetic coder is planned", "ac-bin16-ctx0"},
    {"custom.ac_bin16_ctx8_v1", "AC BIN16 CTX8 v1", "all", "delta_varint_bits", "delta_varint", "ac_bin16_ctx8", "archive", "c++", PipelineAvailability::NotImplemented, "arithmetic coder is planned", "ac-bin16-ctx8"},
    {"custom.ac_bin16_ctx12_v1", "AC BIN16 CTX12 v1", "all", "delta_varint_bits", "delta_varint", "ac_bin16_ctx12", "archive", "c++", PipelineAvailability::NotImplemented, "arithmetic coder is planned", "ac-bin16-ctx12"},
    {"custom.ac_bin32_ctx8_v1", "AC BIN32 CTX8 v1", "all", "delta_varint_bits", "delta_varint", "ac_bin32_ctx8", "archive", "c++", PipelineAvailability::NotImplemented, "32-bit arithmetic coder is planned", "ac-bin32-ctx8"},
    {"custom.range_ctx8_v1", "Range CTX8 v1", "all", "delta_varint_bits", "delta_varint", "range_ctx8", "live", "c++", PipelineAvailability::NotImplemented, "range coder is planned", "range-ctx8"},
    {"custom.rans_ctx8_v1", "rANS CTX8 v1", "all", "delta_varint_bits", "delta_varint", "rans_ctx8", "replay", "c++", PipelineAvailability::NotImplemented, "rans coder is planned", "rans-ctx8"},
    {"depth.keyframe_delta_rans_v1", "Depth keyframe delta + rANS v1", "depth", "depth_keyframe_delta", "orderbook_delta", "rans_ctx8", "replay", "c++", PipelineAvailability::NotImplemented, "orderbook-specific rans pipeline is planned", "depth-keyframe-delta-rans"},
}};

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

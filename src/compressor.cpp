#include "hft_compressor/compressor.hpp"

#include <array>
#include <sstream>
#include <string_view>

#include "common/CompressionInternals.hpp"
#include "hft_compressor/metrics.hpp"
#include "pipelines/PipelineBackend.hpp"

namespace hft_compressor {
std::filesystem::path defaultOutputRoot() {
    const auto cwd = std::filesystem::current_path();
    if (cwd.filename() == "hft-recorder") return cwd.parent_path() / "hft-compressor" / "compressedData";
    if (cwd.filename() == "hft-compressor") return cwd / "compressedData";
    return cwd / "compressedData";
}

double ratio(const CompressionResult& result) noexcept {
    if (result.outputBytes == 0u) return 0.0;
    return static_cast<double>(result.inputBytes) / static_cast<double>(result.outputBytes);
}

double encodeMbPerSec(const CompressionResult& result) noexcept {
    if (result.encodeNs == 0u) return 0.0;
    return (static_cast<double>(result.inputBytes) / (1024.0 * 1024.0))
        / (static_cast<double>(result.encodeNs) / 1000000000.0);
}

double decodeMbPerSec(const CompressionResult& result) noexcept {
    if (result.decodeNs == 0u) return 0.0;
    return (static_cast<double>(result.inputBytes) / (1024.0 * 1024.0))
        / (static_cast<double>(result.decodeNs) / 1000000000.0);
}

double decodeMbPerSec(const DecodeVerifyResult& result) noexcept {
    if (result.decodeNs == 0u) return 0.0;
    return (static_cast<double>(result.decodedBytes) / (1024.0 * 1024.0))
        / (static_cast<double>(result.decodeNs) / 1000000000.0);
}

std::string toMetricsJson(const CompressionResult& result) {
    const char q = char(34);
    std::ostringstream out;
    out << '{' << '\n';
    out << "  " << q << "status" << q << ": " << q << statusToString(result.status) << q << ',' << '\n';
    out << "  " << q << "pipeline_id" << q << ": " << q << result.pipelineId << q << ',' << '\n';
    out << "  " << q << "representation" << q << ": " << q << result.representation << q << ',' << '\n';
    out << "  " << q << "transform" << q << ": " << q << result.transform << q << ',' << '\n';
    out << "  " << q << "entropy" << q << ": " << q << result.entropy << q << ',' << '\n';
    out << "  " << q << "profile" << q << ": " << q << result.profile << q << ',' << '\n';
    out << "  " << q << "stream" << q << ": " << q << streamTypeToString(result.streamType) << q << ',' << '\n';
    out << "  " << q << "input_bytes" << q << ": " << result.inputBytes << ',' << '\n';
    out << "  " << q << "output_bytes" << q << ": " << result.outputBytes << ',' << '\n';
    out << "  " << q << "compression_ratio" << q << ": " << ratio(result) << ',' << '\n';
    out << "  " << q << "line_count" << q << ": " << result.lineCount << ',' << '\n';
    out << "  " << q << "block_count" << q << ": " << result.blockCount << ',' << '\n';
    out << "  " << q << "encode_ns" << q << ": " << result.encodeNs << ',' << '\n';
    out << "  " << q << "decode_ns" << q << ": " << result.decodeNs << ',' << '\n';
    out << "  " << q << "read_ns" << q << ": " << result.readNs << ',' << '\n';
    out << "  " << q << "parse_ns" << q << ": " << result.parseNs << ',' << '\n';
    out << "  " << q << "encode_core_ns" << q << ": " << result.encodeCoreNs << ',' << '\n';
    out << "  " << q << "write_ns" << q << ": " << result.writeNs << ',' << '\n';
    out << "  " << q << "decode_core_ns" << q << ": " << result.decodeCoreNs << ',' << '\n';
    out << "  " << q << "verify_ns" << q << ": " << result.verifyNs << ',' << '\n';
    out << "  " << q << "encode_cycles" << q << ": " << result.encodeCycles << ',' << '\n';
    out << "  " << q << "decode_cycles" << q << ": " << result.decodeCycles << ',' << '\n';
    out << "  " << q << "encode_mb_per_sec" << q << ": " << encodeMbPerSec(result) << ',' << '\n';
    out << "  " << q << "decode_mb_per_sec" << q << ": " << decodeMbPerSec(result) << ',' << '\n';
    out << "  " << q << "encode_core_mb_per_sec" << q << ": " << (result.encodeCoreNs == 0u ? 0.0 : (static_cast<double>(result.inputBytes) / (1024.0 * 1024.0)) / (static_cast<double>(result.encodeCoreNs) / 1000000000.0)) << ',' << '\n';
    out << "  " << q << "decode_core_mb_per_sec" << q << ": " << (result.decodeCoreNs == 0u ? 0.0 : (static_cast<double>(result.inputBytes) / (1024.0 * 1024.0)) / (static_cast<double>(result.decodeCoreNs) / 1000000000.0)) << ',' << '\n';
    out << "  " << q << "roundtrip_ok" << q << ": " << (result.roundtripOk ? "true" : "false") << '\n';
    out << '}' << '\n';
    return out.str();
}

CompressionResult compress(const CompressionRequest& request) noexcept {
    const auto* pipeline = findPipeline(request.pipelineId);
    if (pipeline == nullptr) {
        auto result = internal::fail(Status::UnsupportedPipeline, request, nullptr, "unknown pipeline id");
        metrics::recordRun(result);
        return result;
    }
    if (pipeline->availability == PipelineAvailability::DependencyUnavailable) {
        auto result = internal::fail(Status::DependencyUnavailable, request, pipeline, std::string{pipeline->availabilityReason});
        metrics::recordRun(result);
        return result;
    }
    if (pipeline->availability == PipelineAvailability::NotImplemented) {
        auto result = internal::fail(Status::NotImplemented, request, pipeline, std::string{pipeline->availabilityReason});
        metrics::recordRun(result);
        return result;
    }
    if (const auto* backend = pipelines::findBackend(pipeline->id); backend != nullptr && backend->compress != nullptr) {
        return backend->compress(request, *pipeline);
    }

    auto result = internal::fail(Status::UnsupportedPipeline, request, pipeline, "pipeline has no compressor implementation");
    metrics::recordRun(result);
    return result;
}

Status decodeHfcBuffer(std::span<const std::uint8_t> compressedFile,
                    const DecodedBlockCallback& onBlock) noexcept {
    const auto* backend = pipelines::findBackend("std.zstd_jsonl_blocks_v1");
    return backend != nullptr && backend->decodeBuffer != nullptr ? backend->decodeBuffer(compressedFile, onBlock) : Status::NotImplemented;
}

Status decodeHfcFile(const std::filesystem::path& path,
                    const DecodedBlockCallback& onBlock) noexcept {
    const auto* backend = pipelines::findBackend("std.zstd_jsonl_blocks_v1");
    return backend != nullptr && backend->decodeJsonl != nullptr ? backend->decodeJsonl(path, onBlock) : Status::NotImplemented;
}

}  // namespace hft_compressor



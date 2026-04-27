#include "hft_compressor/metrics.hpp"

#include <cstdio>
#include <mutex>
#include <string_view>
#include <vector>

namespace hft_compressor::metrics {
namespace {

std::mutex& mutex() noexcept {
    static std::mutex m;
    return m;
}

CompressionResult& state() noexcept {
    static CompressionResult last{};
    return last;
}

std::vector<CompressionResult>& matrixState() noexcept {
    static std::vector<CompressionResult> rows{};
    return rows;
}

std::string labelsFor(const CompressionResult& result) {
    std::string labels{"{pipeline_id=\""};
    labels += result.pipelineId.empty() ? "unknown" : result.pipelineId;
    labels += "\",stream=\"";
    labels += std::string{streamTypeToString(result.streamType)};
    labels += "\",profile=\"";
    labels += result.profile.empty() ? "unknown" : result.profile;
    labels += "\",status=\"";
    labels += std::string{statusToString(result.status)};
    labels += "\"}";
    return labels;
}

bool sameMatrixSlot(const CompressionResult& lhs, const CompressionResult& rhs) noexcept {
    return lhs.pipelineId == rhs.pipelineId && lhs.streamType == rhs.streamType;
}

void appendMetric(std::string& out, const char* name, std::uint64_t value) {
    out += name;
    out += ' ';
    out += std::to_string(value);
    out += '\n';
}

void appendMetricDouble(std::string& out, const char* name, double value) {
    char buf[128];
    std::snprintf(buf, sizeof(buf), "%s %.6f\n", name, value);
    out += buf;
}

void appendLabeledMetric(std::string& out, const char* name, const CompressionResult& result, std::uint64_t value) {
    out += name;
    out += labelsFor(result);
    out += ' ';
    out += std::to_string(value);
    out += '\n';
}

void appendLabeledMetricDouble(std::string& out, const char* name, const CompressionResult& result, double value) {
    char buf[128];
    std::snprintf(buf, sizeof(buf), " %.6f\n", value);
    out += name;
    out += labelsFor(result);
    out += buf;
}

}  // namespace

void recordRun(const CompressionResult& result) noexcept {
    std::lock_guard<std::mutex> lock(mutex());
    state() = result;
    auto& rows = matrixState();
    for (auto& row : rows) {
        if (sameMatrixSlot(row, result)) {
            row = result;
            return;
        }
    }
    rows.push_back(result);
}

CompressionResult lastRun() noexcept {
    std::lock_guard<std::mutex> lock(mutex());
    return state();
}

void renderPrometheus(std::string& out) noexcept {
    std::vector<CompressionResult> rows;
    CompressionResult result;
    {
        std::lock_guard<std::mutex> lock(mutex());
        result = state();
        rows = matrixState();
    }

    out += "hft_compressor_last_run_success ";
    out += isOk(result.status) ? "1\n" : "0\n";
    appendMetric(out, "hft_compressor_input_bytes", result.inputBytes);
    appendMetric(out, "hft_compressor_output_bytes", result.outputBytes);
    appendMetric(out, "hft_compressor_lines_total", result.lineCount);
    appendMetric(out, "hft_compressor_blocks_total", result.blockCount);
    appendMetric(out, "hft_compressor_encode_ns", result.encodeNs);
    appendMetric(out, "hft_compressor_decode_ns", result.decodeNs);
    appendMetric(out, "hft_compressor_encode_cycles", result.encodeCycles);
    appendMetric(out, "hft_compressor_decode_cycles", result.decodeCycles);
    appendMetricDouble(out, "hft_compressor_ratio", ratio(result));
    appendMetricDouble(out, "hft_compressor_encode_mb_per_sec", encodeMbPerSec(result));
    appendMetricDouble(out, "hft_compressor_decode_mb_per_sec", decodeMbPerSec(result));

    for (const auto& row : rows) {
        appendLabeledMetric(out, "hft_compressor_run_success", row, isOk(row.status) ? 1u : 0u);
        appendLabeledMetric(out, "hft_compressor_run_input_bytes", row, row.inputBytes);
        appendLabeledMetric(out, "hft_compressor_run_output_bytes", row, row.outputBytes);
        appendLabeledMetric(out, "hft_compressor_run_lines_total", row, row.lineCount);
        appendLabeledMetric(out, "hft_compressor_run_blocks_total", row, row.blockCount);
        appendLabeledMetric(out, "hft_compressor_run_encode_ns", row, row.encodeNs);
        appendLabeledMetric(out, "hft_compressor_run_decode_ns", row, row.decodeNs);
        appendLabeledMetric(out, "hft_compressor_run_encode_cycles", row, row.encodeCycles);
        appendLabeledMetric(out, "hft_compressor_run_decode_cycles", row, row.decodeCycles);
        appendLabeledMetric(out, "hft_compressor_run_roundtrip_ok", row, row.roundtripOk ? 1u : 0u);
        appendLabeledMetricDouble(out, "hft_compressor_run_ratio", row, ratio(row));
        appendLabeledMetricDouble(out, "hft_compressor_run_encode_mb_per_sec", row, encodeMbPerSec(row));
        appendLabeledMetricDouble(out, "hft_compressor_run_decode_mb_per_sec", row, decodeMbPerSec(row));
    }

}

}  // namespace hft_compressor::metrics

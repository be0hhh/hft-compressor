#include "hft_compressor/metrics.hpp"

#include <cstdio>
#include <mutex>

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

}  // namespace

void recordRun(const CompressionResult& result) noexcept {
    std::lock_guard<std::mutex> lock(mutex());
    state() = result;
}

CompressionResult lastRun() noexcept {
    std::lock_guard<std::mutex> lock(mutex());
    return state();
}

void renderPrometheus(std::string& out) noexcept {
    const auto result = lastRun();
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
}

}  // namespace hft_compressor::metrics


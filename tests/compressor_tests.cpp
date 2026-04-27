#include <cassert>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>

#include "hft_compressor/compressor.hpp"
#include "hft_compressor/metrics.hpp"

namespace fs = std::filesystem;

namespace {

void writeFile(const fs::path& path, const std::string& text) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out << text;
}

}  // namespace

int main() {
    using hft_compressor::StreamType;
    assert(hft_compressor::inferStreamTypeFromPath("trades.jsonl") == StreamType::Trades);
    assert(hft_compressor::inferStreamTypeFromPath("bookticker.jsonl") == StreamType::BookTicker);
    assert(hft_compressor::inferStreamTypeFromPath("depth.jsonl") == StreamType::Depth);

    const auto pipelines = hft_compressor::listPipelines();
    assert(!pipelines.empty());
    assert(hft_compressor::findPipeline("std.zstd_jsonl_blocks_v1") != nullptr);
    assert(hft_compressor::findPipeline("custom.ac_bin16_ctx8_v1") != nullptr);
    assert(hft_compressor::findPipeline("missing.pipeline") == nullptr);

    const auto dir = fs::temp_directory_path() / "hft_compressor_tests";
    fs::create_directories(dir);
    const auto input = dir / "trades.jsonl";
    writeFile(input, "[1,2,1,100]\n[2,3,0,200]\n");

    hft_compressor::CompressionRequest missingPipeline{};
    missingPipeline.inputPath = input;
    missingPipeline.pipelineId = "missing.pipeline";
    assert(hft_compressor::compress(missingPipeline).status == hft_compressor::Status::UnsupportedPipeline);

    hft_compressor::CompressionRequest placeholder{};
    placeholder.inputPath = input;
    placeholder.outputRoot = dir / "placeholder_output";
    placeholder.pipelineId = "custom.ac_bin16_ctx8_v1";
    const auto placeholderResult = hft_compressor::compress(placeholder);
    assert(placeholderResult.status == hft_compressor::Status::NotImplemented);
    assert(placeholderResult.pipelineId == "custom.ac_bin16_ctx8_v1");
    assert(!fs::exists(placeholder.outputRoot));

    hft_compressor::CompressionRequest request{};
    request.inputPath = input;
    request.outputRoot = dir / "compressedData";
    request.pipelineId = "std.zstd_jsonl_blocks_v1";
    request.blockBytes = 16;
    const auto result = hft_compressor::compress(request);
#if HFT_COMPRESSOR_WITH_ZSTD
    assert(hft_compressor::isOk(result.status));
    assert(result.pipelineId == "std.zstd_jsonl_blocks_v1");
    assert(result.representation == "jsonl_blocks");
    assert(result.transform == "raw_jsonl");
    assert(result.entropy == "zstd");
    assert(result.lineCount == 2u);
    assert(result.blockCount >= 1u);
    assert(fs::exists(result.outputPath));
    assert(result.outputPath == dir / "compressedData" / "zstd" / "sessions" / "hft_compressor_tests" / "trades.hfc");
    assert(fs::exists(result.metricsPath));
    std::ifstream in(result.outputPath, std::ios::binary);
    std::vector<unsigned char> data((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    std::string decoded;
    const auto status = hft_compressor::decodeHfcBuffer(data, [&decoded](std::span<const std::uint8_t> block) {
        decoded.append(reinterpret_cast<const char*>(block.data()), block.size());
        return true;
    });
    assert(hft_compressor::isOk(status));
    assert(decoded == "[1,2,1,100]\n[2,3,0,200]\n");

    hft_compressor::DecodeVerifyRequest verifyRequest{};
    verifyRequest.compressedPath = result.outputPath;
    verifyRequest.canonicalPath = input;
    verifyRequest.pipelineId = "std.zstd_jsonl_blocks_v1";
    const auto verifyResult = hft_compressor::decodeAndVerify(verifyRequest);
    assert(hft_compressor::isOk(verifyResult.status));
    assert(verifyResult.verified);
    assert(verifyResult.decodedBytes == result.inputBytes);
    assert(verifyResult.canonicalBytes == result.inputBytes);
    assert(fs::exists(verifyResult.metricsPath));

    const auto changedCanonical = dir / "trades_changed.jsonl";
    writeFile(changedCanonical, "[1,2,1,100]\n[2,3,0,201]\n");
    verifyRequest.canonicalPath = changedCanonical;
    const auto mismatchResult = hft_compressor::decodeAndVerify(verifyRequest);
    assert(mismatchResult.status == hft_compressor::Status::VerificationFailed);
    assert(!mismatchResult.verified);
    assert(mismatchResult.mismatchBytes > 0u);
    assert(mismatchResult.mismatchPercent > 0.0);

    verifyRequest.compressedPath = dir / "missing.hfc";
    verifyRequest.canonicalPath = input;
    assert(hft_compressor::decodeAndVerify(verifyRequest).status == hft_compressor::Status::IoError);

    hft_compressor::CompressionRequest exactRequest{};
    exactRequest.inputPath = input;
    exactRequest.outputPathOverride = dir / "session" / "compressed" / "zstd" / "trades.hfc";
    exactRequest.pipelineId = "std.zstd_jsonl_blocks_v1";
    const auto exactResult = hft_compressor::compress(exactRequest);
    assert(hft_compressor::isOk(exactResult.status));
    assert(exactResult.outputPath == exactRequest.outputPathOverride);
    assert(exactResult.metricsPath == exactRequest.outputPathOverride.parent_path() / "trades.metrics.json");
    assert(fs::exists(exactResult.outputPath));
    assert(fs::exists(exactResult.metricsPath));
    const auto bookTickerInput = dir / "bookticker.jsonl";
    writeFile(bookTickerInput, "[1,2,3,4]\n[2,3,4,5]\n");
    hft_compressor::CompressionRequest bookTickerRequest{};
    bookTickerRequest.inputPath = bookTickerInput;
    bookTickerRequest.outputRoot = dir / "compressedData";
    bookTickerRequest.pipelineId = "std.zstd_jsonl_blocks_v1";
    const auto bookTickerResult = hft_compressor::compress(bookTickerRequest);
    assert(hft_compressor::isOk(bookTickerResult.status));

    std::string prometheus;
    hft_compressor::metrics::renderPrometheus(prometheus);
    assert(prometheus.find("hft_compressor_run_ratio") != std::string::npos);
    assert(prometheus.find("pipeline_id=\"std.zstd_jsonl_blocks_v1\"") != std::string::npos);
    assert(prometheus.find("stream=\"trades\"") != std::string::npos);
    assert(prometheus.find("stream=\"bookticker\"") != std::string::npos);
    assert(prometheus.find("hft_compressor_verify") == std::string::npos);
#else
    assert(result.status == hft_compressor::Status::DependencyUnavailable);
#endif
    return 0;
}




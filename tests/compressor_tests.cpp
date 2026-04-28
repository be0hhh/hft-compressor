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

void writeBytes(const fs::path& path, const std::vector<unsigned char>& bytes) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(reinterpret_cast<const char*>(bytes.data()), static_cast<std::streamsize>(bytes.size()));
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

    std::string decodedFromFile;
    const auto fileStatus = hft_compressor::decodeHfcFile(result.outputPath, [&decodedFromFile](std::span<const std::uint8_t> block) {
        decodedFromFile.append(reinterpret_cast<const char*>(block.data()), block.size());
        return true;
    });
    assert(hft_compressor::isOk(fileStatus));
    assert(decodedFromFile == decoded);

    auto v1Compatible = data;
    v1Compatible[4] = 1u;
    v1Compatible[5] = 0u;
    const auto v1CompatiblePath = dir / "v1_compatible.hfc";
    writeBytes(v1CompatiblePath, v1Compatible);
    std::string decodedV1;
    assert(hft_compressor::isOk(hft_compressor::decodeHfcFile(v1CompatiblePath, [&decodedV1](std::span<const std::uint8_t> block) {
        decodedV1.append(reinterpret_cast<const char*>(block.data()), block.size());
        return true;
    })));
    assert(decodedV1 == decoded);

    const auto hfcInfo = hft_compressor::openHfcFile(result.outputPath);
    assert(hft_compressor::isOk(hfcInfo.status));
    assert(hfcInfo.version == 2u);
    assert(hfcInfo.blockCount == result.blockCount);
    assert(hfcInfo.blocks.size() == result.blockCount);

    hft_compressor::ReplayArtifactRequest artifactRequest{};
    artifactRequest.compressedRoot = request.outputRoot;
    artifactRequest.sessionDir = input.parent_path();
    artifactRequest.streamType = hft_compressor::StreamType::Trades;
    artifactRequest.preference = hft_compressor::ArtifactPreference::CurrentBaseline;
    const auto artifact = hft_compressor::discoverReplayArtifact(artifactRequest);
    assert(hft_compressor::isOk(artifact.status));
    assert(artifact.found);
    assert(artifact.path == result.outputPath);
    assert(artifact.formatId == "hfc.zstd_jsonl_blocks_v1");
    assert(artifact.pipelineId == "std.zstd_jsonl_blocks_v1");
    assert(artifact.transform == "raw_jsonl");
    assert(artifact.entropy == "zstd");

    std::string decodedReplayArtifact;
    assert(hft_compressor::isOk(hft_compressor::decodeReplayArtifactJsonl(artifact, [&decodedReplayArtifact](std::span<const std::uint8_t> block) {
        decodedReplayArtifact.append(reinterpret_cast<const char*>(block.data()), block.size());
        return true;
    })));
    assert(decodedReplayArtifact == decoded);
    assert(hft_compressor::decodeReplayRecords(artifactRequest, [](const hft_compressor::ReplayRecord&) {
        return true;
    }) == hft_compressor::Status::NotImplemented);

    artifactRequest.preferredPipelineId = "custom.rans_ctx8_v1";
    assert(hft_compressor::discoverReplayArtifact(artifactRequest).status == hft_compressor::Status::NotImplemented);
    artifactRequest.preferredPipelineId.clear();
    artifactRequest.sessionDir.clear();
    artifactRequest.sessionId = "missing_session";
    artifactRequest.streamType = hft_compressor::StreamType::Depth;
    const auto missingArtifact = hft_compressor::discoverReplayArtifact(artifactRequest);
    assert(hft_compressor::isOk(missingArtifact.status));
    assert(!missingArtifact.found);

    std::size_t callbackCount = 0;
    assert(hft_compressor::isOk(hft_compressor::decodeHfcFile(result.outputPath, [&callbackCount](std::span<const std::uint8_t>) {
        ++callbackCount;
        return false;
    })));
    assert(callbackCount == 1u);

    const auto truncatedHeader = dir / "truncated_header.hfc";
    writeBytes(truncatedHeader, std::vector<unsigned char>(data.begin(), data.begin() + 8));
    assert(hft_compressor::decodeHfcFile(truncatedHeader, [](std::span<const std::uint8_t>) { return true; }) == hft_compressor::Status::CorruptData);

    auto badBlockMagic = data;
    badBlockMagic[64] ^= 0xffu;
    const auto badBlockMagicPath = dir / "bad_block_magic.hfc";
    writeBytes(badBlockMagicPath, badBlockMagic);
    assert(hft_compressor::decodeHfcFile(badBlockMagicPath, [](std::span<const std::uint8_t>) { return true; }) == hft_compressor::Status::CorruptData);

    auto truncatedPayload = data;
    truncatedPayload.pop_back();
    const auto truncatedPayloadPath = dir / "truncated_payload.hfc";
    writeBytes(truncatedPayloadPath, truncatedPayload);
    assert(hft_compressor::decodeHfcFile(truncatedPayloadPath, [](std::span<const std::uint8_t>) { return true; }) == hft_compressor::Status::CorruptData);

    auto badPayload = data;
    badPayload.back() ^= 0x5au;
    const auto badPayloadPath = dir / "bad_payload.hfc";
    writeBytes(badPayloadPath, badPayload);
    assert(hft_compressor::decodeHfcFile(badPayloadPath, [](std::span<const std::uint8_t>) { return true; }) != hft_compressor::Status::Ok);

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

    verifyRequest.canonicalPath = dir / "missing_canonical.jsonl";
    assert(hft_compressor::decodeAndVerify(verifyRequest).status == hft_compressor::Status::IoError);

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




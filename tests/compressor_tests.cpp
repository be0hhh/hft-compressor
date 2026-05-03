#include <cassert>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>

#include "hft_compressor/c_api.h"
#include "hft_compressor/compressor.hpp"
#include "hft_compressor/metrics.hpp"
#include "hft_compressor/replay_decode.hpp"

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

struct StandardCodecCase {
    const char* pipelineId;
    const char* outputSlug;
    const char* formatId;
};

struct HftMacCase {
    const char* pipelineId;
    const char* inputName;
    const char* jsonl;
    const char* formatId;
    const char* transform;
    const char* entropy;
    const char* outputSlug;
    hft_compressor::StreamType streamType;
};

void runHftMacCase(const HftMacCase& codec, const fs::path& dir) {
    const auto input = dir / codec.inputName;
    writeFile(input, codec.jsonl);
    hft_compressor::CompressionRequest request{};
    request.inputPath = input;
    request.outputRoot = dir / "hftmacCompressedData";
    request.pipelineId = codec.pipelineId;
    request.blockBytes = 16;

    const auto result = hft_compressor::compress(request);
    assert(hft_compressor::isOk(result.status));
    assert(result.pipelineId == codec.pipelineId);
    assert(result.transform == codec.transform);
    assert(result.entropy == codec.entropy);
    assert(result.roundtripOk);
    assert(result.outputPath == request.outputRoot / codec.outputSlug / "sessions" / "hft_compressor_tests" / (std::string{codec.inputName}.substr(0, std::string{codec.inputName}.find('.')) + ".hfc"));

    hft_compressor::ReplayArtifactRequest artifactRequest{};
    artifactRequest.compressedRoot = request.outputRoot;
    artifactRequest.sessionDir = input.parent_path();
    artifactRequest.streamType = codec.streamType;
    artifactRequest.preferredPipelineId = codec.pipelineId;
    const auto artifact = hft_compressor::discoverReplayArtifact(artifactRequest);
    assert(hft_compressor::isOk(artifact.status));
    assert(artifact.found);
    assert(artifact.formatId == codec.formatId);
    assert(artifact.pipelineId == codec.pipelineId);

    std::string decoded;
    assert(hft_compressor::isOk(hft_compressor::decodeReplayArtifactJsonl(artifact, [&decoded](std::span<const std::uint8_t> block) {
        decoded.append(reinterpret_cast<const char*>(block.data()), block.size());
        return true;
    })));
    assert(decoded == codec.jsonl);

    hft_compressor::DecodeVerifyRequest verifyRequest{};
    verifyRequest.compressedPath = result.outputPath;
    verifyRequest.canonicalPath = input;
    verifyRequest.pipelineId = codec.pipelineId;
    verifyRequest.verifyMode = hft_compressor::VerifyMode::Both;
    const auto verifyResult = hft_compressor::decodeAndVerify(verifyRequest);
    assert(hft_compressor::isOk(verifyResult.status));
    assert(verifyResult.byteExact);
    assert(verifyResult.recordExact);
}

void runStandardCodecCase(const StandardCodecCase& codec, const fs::path& input, const fs::path& dir) {
    const auto* pipeline = hft_compressor::findPipeline(codec.pipelineId);
    assert(pipeline != nullptr);

    hft_compressor::CompressionRequest request{};
    request.inputPath = input;
    request.outputRoot = dir / "standardCompressedData";
    request.pipelineId = codec.pipelineId;
    request.blockBytes = 16;

    const auto result = hft_compressor::compress(request);
    if (pipeline->availability == hft_compressor::PipelineAvailability::DependencyUnavailable) {
        assert(result.status == hft_compressor::Status::DependencyUnavailable);
        return;
    }

    assert(hft_compressor::isOk(result.status));
    assert(result.pipelineId == codec.pipelineId);
    assert(result.outputPath == request.outputRoot / codec.outputSlug / "sessions" / "hft_compressor_tests" / "trades.hfc");
    assert(result.lineCount == 2u);
    assert(result.blockCount >= 1u);
    assert(result.roundtripOk);

    hft_compressor::ReplayArtifactRequest artifactRequest{};
    artifactRequest.compressedRoot = request.outputRoot;
    artifactRequest.sessionDir = input.parent_path();
    artifactRequest.streamType = hft_compressor::StreamType::Trades;
    artifactRequest.preferredPipelineId = codec.pipelineId;
    const auto artifact = hft_compressor::discoverReplayArtifact(artifactRequest);
    assert(hft_compressor::isOk(artifact.status));
    assert(artifact.found);
    assert(artifact.path == result.outputPath);
    assert(artifact.formatId == codec.formatId);
    assert(artifact.pipelineId == codec.pipelineId);

    std::string decoded;
    assert(hft_compressor::isOk(hft_compressor::decodeReplayArtifactJsonl(artifact, [&decoded](std::span<const std::uint8_t> block) {
        decoded.append(reinterpret_cast<const char*>(block.data()), block.size());
        return true;
    })));
    assert(decoded == "[1,2,1,100]\n[2,3,0,200]\n");

    hft_compressor::DecodeVerifyRequest verifyRequest{};
    verifyRequest.compressedPath = result.outputPath;
    verifyRequest.canonicalPath = input;
    verifyRequest.pipelineId = codec.pipelineId;
    verifyRequest.verifyMode = hft_compressor::VerifyMode::Both;
    const auto verifyResult = hft_compressor::decodeAndVerify(verifyRequest);
    assert(hft_compressor::isOk(verifyResult.status));
    assert(verifyResult.verified);
    assert(verifyResult.byteExact);
    assert(verifyResult.recordExact);
    assert(verifyResult.decodedRecordCount == 2u);

    std::size_t tradeCount = 0;
    hft_compressor::ReplayDecodeRequest decodeRequest{};
    decodeRequest.artifact = artifactRequest;
    decodeRequest.maxRecordsPerBatch = 1;
    assert(hft_compressor::isOk(hft_compressor::decodeReplayRecordBatches(decodeRequest, [&](const hft_compressor::ReplayRecordBatchV1& batch) {
        tradeCount += batch.trades.size();
        assert(batch.streamType == hft_compressor::StreamType::Trades);
        return true;
    })));
    assert(tradeCount == 2u);
}
int countCRecords(const hftc_record_batch_v1* batch, void* userData) {
    auto* count = static_cast<std::size_t*>(userData);
    *count += batch->trade_count + batch->bookticker_count + batch->depth_count;
    return 1;
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
    assert(hft_compressor::findPipeline("std.raw_jsonl_blocks_v1") != nullptr);
    assert(hft_compressor::findPipeline("trade.hftmac_varint_v1") != nullptr);
    assert(hft_compressor::findPipeline("bookticker.hftmac_varint_v1") != nullptr);
    assert(hft_compressor::findPipeline("depth.hftmac_varint_v1") != nullptr);
    assert(hft_compressor::findPipeline("trade.hftmac_raw_binary_v1") != nullptr);
    assert(hft_compressor::findPipeline("bookticker.hftmac_raw_binary_v1") != nullptr);
    assert(hft_compressor::findPipeline("depth.hftmac_raw_binary_v1") != nullptr);
    assert(hft_compressor::findPipeline("trade.hftmac_ac16_ctx8_v1") != nullptr);
    assert(hft_compressor::findPipeline("custom.ac_bin16_ctx8_v1") != nullptr);
    assert(hft_compressor::findPipeline("hftmac.trades_grouped_delta_qtydict_math_v2") == nullptr);
    assert(hft_compressor::findPipeline("hftmac.trades_grouped_delta_qtydict_math_v3") != nullptr);
    assert(hft_compressor::findPipeline("hftmac.bookticker_delta_mask_v1") != nullptr);
    assert(hft_compressor::findPipeline("hftmac.depth_ladder_offset_v1") != nullptr);
    assert(hft_compressor::findPipeline("hftmac.trades_grouped_delta_qtydict_v1") == nullptr);
    assert(hft_compressor::findPipeline("missing.pipeline") == nullptr);

    const auto dir = fs::temp_directory_path() / "hft_compressor_tests";
    fs::create_directories(dir);
    const auto input = dir / "trades.jsonl";
    writeFile(input, "[1,2,1,100]\n[2,3,0,200]\n");

    runHftMacCase({"trade.hftmac_varint_v1", "trades.jsonl", "[1,2,1,100]\n[2,3,0,200]\n", "hftmac.trade.varint.v1", "hftmac_varint", "none", "hftmac-varint", hft_compressor::StreamType::Trades}, dir);
    runHftMacCase({"bookticker.hftmac_varint_v1", "bookticker.jsonl", "[1,2,3,4,100]\n[2,3,4,5,200]\n", "hftmac.bookticker.varint.v1", "hftmac_varint", "none", "hftmac-varint", hft_compressor::StreamType::BookTicker}, dir);
    runHftMacCase({"depth.hftmac_varint_v1", "depth.jsonl", "[[100,1,0],[101,2,1],100]\n[[100,0,0],[102,3,1],200]\n", "hftmac.depth.varint.v1", "hftmac_varint", "none", "hftmac-varint", hft_compressor::StreamType::Depth}, dir);
    runHftMacCase({"trade.hftmac_raw_binary_v1", "trades.jsonl", "[1,2,1,100]\n[2,3,0,200]\n", "hftmac.trade.raw_binary.v1", "hftmac_raw_binary", "none", "hftmac-raw-binary", hft_compressor::StreamType::Trades}, dir);
    runHftMacCase({"bookticker.hftmac_raw_binary_v1", "bookticker.jsonl", "[1,2,3,4,100]\n[2,3,4,5,200]\n", "hftmac.bookticker.raw_binary.v1", "hftmac_raw_binary", "none", "hftmac-raw-binary", hft_compressor::StreamType::BookTicker}, dir);
    runHftMacCase({"depth.hftmac_raw_binary_v1", "depth.jsonl", "[[100,1,0],[101,2,1],100]\n[[100,0,0],[102,3,1],200]\n", "hftmac.depth.raw_binary.v1", "hftmac_raw_binary", "none", "hftmac-raw-binary", hft_compressor::StreamType::Depth}, dir);

    const auto spacedHftMacInput = dir / "hftmac_spaced" / "trades.jsonl";
    fs::create_directories(spacedHftMacInput.parent_path());
    writeFile(spacedHftMacInput, "[1, 2, 1, 100]\n");
    hft_compressor::CompressionRequest spacedHftMacRequest{};
    spacedHftMacRequest.inputPath = spacedHftMacInput;
    spacedHftMacRequest.outputRoot = dir / "hftmac_spaced_out";
    spacedHftMacRequest.pipelineId = "trade.hftmac_varint_v1";
    assert(hft_compressor::compress(spacedHftMacRequest).status == hft_compressor::Status::CorruptData);

    hft_compressor::CompressionRequest tradeMathV3Request{};
    tradeMathV3Request.inputPath = input;
    tradeMathV3Request.outputRoot = dir / "trade_math_v3_out";
    tradeMathV3Request.pipelineId = "hftmac.trades_grouped_delta_qtydict_math_v3";
    const auto tradeMathV3Result = hft_compressor::compress(tradeMathV3Request);
    assert(hft_compressor::isOk(tradeMathV3Result.status));
    assert(tradeMathV3Result.roundtripOk);
    hft_compressor::ReplayArtifactRequest tradeMathV3ArtifactRequest{};
    tradeMathV3ArtifactRequest.compressedRoot = tradeMathV3Request.outputRoot;
    tradeMathV3ArtifactRequest.sessionDir = input.parent_path();
    tradeMathV3ArtifactRequest.streamType = hft_compressor::StreamType::Trades;
    tradeMathV3ArtifactRequest.preferredPipelineId = "hftmac.trades_grouped_delta_qtydict_math_v3";
    const auto tradeMathV3Artifact = hft_compressor::discoverReplayArtifact(tradeMathV3ArtifactRequest);
    assert(hft_compressor::isOk(tradeMathV3Artifact.status));
    assert(tradeMathV3Artifact.formatId == "hftmac.trades_grouped_delta_qtydict.math.v3");
    std::string tradeMathV3Decoded;
    assert(hft_compressor::isOk(hft_compressor::decodeReplayArtifactJsonl(tradeMathV3Artifact, [&tradeMathV3Decoded](std::span<const std::uint8_t> block) {
        tradeMathV3Decoded.append(reinterpret_cast<const char*>(block.data()), block.size());
        return true;
    })));
    assert(tradeMathV3Decoded == "[1,2,1,100]\n[2,3,0,200]\n");

    const auto bookMathInput = dir / "bookticker.jsonl";
    writeFile(bookMathInput, "[100,10,110,20,1000]\n[100,9,110,20,1001]\n[101,9,111,22,1001]\n");
    hft_compressor::CompressionRequest bookMathRequest{};
    bookMathRequest.inputPath = bookMathInput;
    bookMathRequest.outputRoot = dir / "book_math_out";
    bookMathRequest.pipelineId = "hftmac.bookticker_delta_mask_v1";
    const auto bookMathResult = hft_compressor::compress(bookMathRequest);
    assert(hft_compressor::isOk(bookMathResult.status));
    assert(bookMathResult.roundtripOk);
    hft_compressor::ReplayArtifactRequest bookMathArtifactRequest{};
    bookMathArtifactRequest.compressedRoot = bookMathRequest.outputRoot;
    bookMathArtifactRequest.sessionDir = bookMathInput.parent_path();
    bookMathArtifactRequest.streamType = hft_compressor::StreamType::BookTicker;
    bookMathArtifactRequest.preferredPipelineId = "hftmac.bookticker_delta_mask_v1";
    const auto bookMathArtifact = hft_compressor::discoverReplayArtifact(bookMathArtifactRequest);
    assert(hft_compressor::isOk(bookMathArtifact.status));
    assert(bookMathArtifact.formatId == "hftmac.bookticker_delta_mask.v1");

    const auto depthMathInput = dir / "depth.jsonl";
    writeFile(depthMathInput, "[[100,10,0],[110,20,1],1000]\n[[100,9,0],[111,22,1],1001]\n[[100,0,0],[99,7,0],1002]\n");
    hft_compressor::CompressionRequest depthMathRequest{};
    depthMathRequest.inputPath = depthMathInput;
    depthMathRequest.outputRoot = dir / "depth_math_out";
    depthMathRequest.pipelineId = "hftmac.depth_ladder_offset_v1";
    const auto depthMathResult = hft_compressor::compress(depthMathRequest);
    assert(hft_compressor::isOk(depthMathResult.status));
    assert(depthMathResult.roundtripOk);
    hft_compressor::ReplayArtifactRequest depthMathArtifactRequest{};
    depthMathArtifactRequest.compressedRoot = depthMathRequest.outputRoot;
    depthMathArtifactRequest.sessionDir = depthMathInput.parent_path();
    depthMathArtifactRequest.streamType = hft_compressor::StreamType::Depth;
    depthMathArtifactRequest.preferredPipelineId = "hftmac.depth_ladder_offset_v1";
    const auto depthMathArtifact = hft_compressor::discoverReplayArtifact(depthMathArtifactRequest);
    assert(hft_compressor::isOk(depthMathArtifact.status));
    assert(depthMathArtifact.formatId == "hftmac.depth_ladder_offset.v1");
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

    hft_compressor::CompressionRequest rawRequest{};
    rawRequest.inputPath = input;
    rawRequest.outputRoot = dir / "compressedData";
    rawRequest.pipelineId = "std.raw_jsonl_blocks_v1";
    rawRequest.blockBytes = 16;
    const auto rawResult = hft_compressor::compress(rawRequest);
    assert(hft_compressor::isOk(rawResult.status));
    assert(rawResult.pipelineId == "std.raw_jsonl_blocks_v1");
    assert(rawResult.entropy == "none");
    assert(rawResult.outputPath == dir / "compressedData" / "raw-jsonl" / "sessions" / "hft_compressor_tests" / "trades.hfr");
    assert(rawResult.lineCount == 2u);
    assert(rawResult.blockCount >= 1u);

    hft_compressor::ReplayArtifactRequest rawArtifactRequest{};
    rawArtifactRequest.compressedRoot = rawRequest.outputRoot;
    rawArtifactRequest.sessionDir = input.parent_path();
    rawArtifactRequest.streamType = hft_compressor::StreamType::Trades;
    rawArtifactRequest.preferredPipelineId = "std.raw_jsonl_blocks_v1";
    const auto rawArtifact = hft_compressor::discoverReplayArtifact(rawArtifactRequest);
    assert(hft_compressor::isOk(rawArtifact.status));
    assert(rawArtifact.found);
    assert(rawArtifact.path == rawResult.outputPath);
    assert(rawArtifact.formatId == "hfr.raw_jsonl_blocks_v1");
    assert(rawArtifact.pipelineId == "std.raw_jsonl_blocks_v1");

    std::string decodedRawArtifact;
    assert(hft_compressor::isOk(hft_compressor::decodeReplayArtifactJsonl(rawArtifact, [&decodedRawArtifact](std::span<const std::uint8_t> block) {
        decodedRawArtifact.append(reinterpret_cast<const char*>(block.data()), block.size());
        return true;
    })));
    assert(decodedRawArtifact == "[1,2,1,100]\n[2,3,0,200]\n");

    std::size_t rawTradeCount = 0;
    hft_compressor::ReplayDecodeRequest rawDecodeRequest{};
    rawDecodeRequest.artifact = rawArtifactRequest;
    rawDecodeRequest.maxRecordsPerBatch = 2;
    assert(hft_compressor::isOk(hft_compressor::decodeReplayRecordBatches(rawDecodeRequest, [&](const hft_compressor::ReplayRecordBatchV1& batch) {
        rawTradeCount += batch.trades.size();
        assert(batch.streamType == hft_compressor::StreamType::Trades);
        return true;
    })));
    assert(rawTradeCount == 2u);

    hft_compressor::DecodeVerifyRequest rawVerifyRequest{};
    rawVerifyRequest.compressedPath = rawResult.outputPath;
    rawVerifyRequest.canonicalPath = input;
    rawVerifyRequest.pipelineId = "std.raw_jsonl_blocks_v1";
    const auto rawVerifyResult = hft_compressor::decodeAndVerify(rawVerifyRequest);
    assert(hft_compressor::isOk(rawVerifyResult.status));
    assert(rawVerifyResult.byteExact);
    assert(rawVerifyResult.recordExact);
    assert(rawVerifyResult.decodedRecordCount == 2u);

    runStandardCodecCase({"std.lz4_jsonl_blocks_v1", "lz4", "hfc.lz4_jsonl_blocks_v1"}, input, dir);
    runStandardCodecCase({"std.brotli_jsonl_blocks_v1", "brotli", "hfc.brotli_jsonl_blocks_v1"}, input, dir);
    runStandardCodecCase({"std.xz_jsonl_blocks_v1", "xz", "hfc.xz_jsonl_blocks_v1"}, input, dir);
    runStandardCodecCase({"std.gzip_jsonl_blocks_v1", "gzip", "hfc.gzip_jsonl_blocks_v1"}, input, dir);

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

    std::size_t batchCount = 0;
    std::size_t tradeCount = 0;
    hft_compressor::ReplayDecodeRequest decodeRequest{};
    decodeRequest.artifact = artifactRequest;
    decodeRequest.maxRecordsPerBatch = 1;
    assert(hft_compressor::isOk(hft_compressor::decodeReplayRecordBatches(decodeRequest, [&](const hft_compressor::ReplayRecordBatchV1& batch) {
        ++batchCount;
        tradeCount += batch.trades.size();
        assert(batch.streamType == hft_compressor::StreamType::Trades);
        assert(batch.bookTickers.empty());
        assert(batch.depths.empty());
        assert(batch.recordCount() == 1u);
        return true;
    })));
    assert(batchCount == 2u);
    assert(tradeCount == 2u);
    assert(hft_compressor::decodeReplayRecordBatches(decodeRequest, [](const hft_compressor::ReplayRecordBatchV1&) {
        return false;
    }) == hft_compressor::Status::CallbackStopped);

    std::size_t replayRecordCount = 0;
    assert(hft_compressor::isOk(hft_compressor::decodeReplayRecords(artifactRequest, [&replayRecordCount](const hft_compressor::ReplayRecord& record) {
        assert(record.kind == hft_compressor::ReplayRecordKind::Trade);
        ++replayRecordCount;
        return true;
    })));
    assert(replayRecordCount == 2u);

    hftc_replay_decode_request cRequest{};
    const auto cRoot = request.outputRoot.string();
    const auto cSessionDir = input.parent_path().string();
    cRequest.compressed_root = cRoot.c_str();
    cRequest.session_dir = cSessionDir.c_str();
    cRequest.stream_type = HFTC_STREAM_TRADES;
    cRequest.artifact_preference = HFTC_ARTIFACT_CURRENT_BASELINE;
    cRequest.max_records_per_batch = 4096u;
    hftc_decoder* cDecoder = nullptr;
    assert(hftc_decoder_open(&cRequest, &cDecoder) == HFTC_STATUS_OK);
    std::size_t cRecordCount = 0;
    assert(hftc_decoder_decode_all(cDecoder, countCRecords, &cRecordCount) == HFTC_STATUS_OK);
    assert(cRecordCount == 2u);
    hftc_decoder_close(cDecoder);

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
    assert(verifyResult.byteExact);
    assert(verifyResult.recordExact);
    assert(verifyResult.decodedBytes == result.inputBytes);
    assert(verifyResult.canonicalBytes == result.inputBytes);
    assert(verifyResult.decodedRecordCount == 2u);
    assert(verifyResult.canonicalRecordCount == 2u);
    assert(verifyResult.decodedByteHash == verifyResult.canonicalByteHash);
    assert(verifyResult.decodedRecordHash == verifyResult.canonicalRecordHash);
    assert(verifyResult.replayTimeSpanNs == 100u);
    assert(verifyResult.estimatedReplayMultiplier > 0.0);
    assert(fs::exists(verifyResult.metricsPath));

    const auto spacedCanonical = dir / "trades_spaced.jsonl";
    writeFile(spacedCanonical, "[1, 2, 1, 100]\n[2, 3, 0, 200]\n");
    verifyRequest.canonicalPath = spacedCanonical;
    const auto spacedResult = hft_compressor::decodeAndVerify(verifyRequest);
    assert(spacedResult.status == hft_compressor::Status::VerificationFailed);
    assert(!spacedResult.byteExact);
    assert(spacedResult.recordExact);
    assert(spacedResult.firstMismatchField == "byte");

    auto recordOnlyRequest = verifyRequest;
    recordOnlyRequest.verifyBytes = false;
    recordOnlyRequest.verifyMode = hft_compressor::VerifyMode::RecordExact;
    const auto recordOnlyResult = hft_compressor::decodeAndVerify(recordOnlyRequest);
    assert(hft_compressor::isOk(recordOnlyResult.status));
    assert(recordOnlyResult.verified);
    assert(!recordOnlyResult.byteExact);
    assert(recordOnlyResult.recordExact);

    const auto changedCanonical = dir / "trades_changed.jsonl";
    writeFile(changedCanonical, "[1,2,1,100]\n[2,3,0,201]\n");
    verifyRequest.canonicalPath = changedCanonical;
    const auto mismatchResult = hft_compressor::decodeAndVerify(verifyRequest);
    assert(mismatchResult.status == hft_compressor::Status::VerificationFailed);
    assert(!mismatchResult.verified);
    assert(!mismatchResult.recordExact);
    assert(mismatchResult.firstMismatchLine == 2u);
    assert(mismatchResult.firstMismatchField == "ts_ns");
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
    writeFile(bookTickerInput, "[1,2,3,4,100]\n[2,3,4,5,200]\n");
    hft_compressor::CompressionRequest bookTickerRequest{};
    bookTickerRequest.inputPath = bookTickerInput;
    bookTickerRequest.outputRoot = dir / "compressedData";
    bookTickerRequest.pipelineId = "std.zstd_jsonl_blocks_v1";
    const auto bookTickerResult = hft_compressor::compress(bookTickerRequest);
    assert(hft_compressor::isOk(bookTickerResult.status));

    const auto depthInput = dir / "depth.jsonl";
    writeFile(depthInput, "[[100,1,0],[101,2,1],123]\n");
    hft_compressor::CompressionRequest depthRequest{};
    depthRequest.inputPath = depthInput;
    depthRequest.outputRoot = dir / "compressedData";
    depthRequest.pipelineId = "std.zstd_jsonl_blocks_v1";
    const auto depthResult = hft_compressor::compress(depthRequest);
    assert(hft_compressor::isOk(depthResult.status));
    assert(depthResult.outputPath == dir / "compressedData" / "zstd" / "sessions" / "hft_compressor_tests" / "depth.hfc");

    hft_compressor::ReplayArtifactRequest depthArtifactRequest{};
    depthArtifactRequest.compressedRoot = depthRequest.outputRoot;
    depthArtifactRequest.sessionDir = depthInput.parent_path();
    depthArtifactRequest.streamType = hft_compressor::StreamType::Depth;
    const auto depthArtifact = hft_compressor::discoverReplayArtifact(depthArtifactRequest);
    assert(hft_compressor::isOk(depthArtifact.status));
    assert(depthArtifact.found);
    assert(depthArtifact.streamType == hft_compressor::StreamType::Depth);
    assert(depthArtifact.path == depthResult.outputPath);

    std::size_t depthRecordCount = 0;
    hft_compressor::ReplayDecodeRequest depthDecodeRequest{};
    depthDecodeRequest.artifact = depthArtifactRequest;
    assert(hft_compressor::isOk(hft_compressor::decodeReplayRecordBatches(depthDecodeRequest, [&](const hft_compressor::ReplayRecordBatchV1& batch) {
        assert(batch.streamType == hft_compressor::StreamType::Depth);
        assert(batch.depths.size() == 1u);
        assert(batch.depthLevels.size() == 2u);
        assert(batch.depths.front().tsNs == 123);
        assert(batch.depthLevels.front().priceE8 == 100);
        depthRecordCount += batch.depths.size();
        return true;
    })));
    assert(depthRecordCount == 1u);

    const auto oldDepthInput = dir / "old_depth" / "depth.jsonl";
    fs::create_directories(oldDepthInput.parent_path());
    writeFile(oldDepthInput, "[[[100,1,0,0]],[[101,2,1,0]],123]\n");
    hft_compressor::CompressionRequest oldDepthRequest{};
    oldDepthRequest.inputPath = oldDepthInput;
    oldDepthRequest.outputRoot = dir / "old_compressedData";
    oldDepthRequest.pipelineId = "std.zstd_jsonl_blocks_v1";
    assert(hft_compressor::compress(oldDepthRequest).status == hft_compressor::Status::CorruptData);

    std::string prometheus;
    hft_compressor::metrics::renderPrometheus(prometheus);
    assert(prometheus.find("hft_compressor_run_ratio") != std::string::npos);
    assert(prometheus.find("pipeline_id=\"std.zstd_jsonl_blocks_v1\"") != std::string::npos);
    assert(prometheus.find("stream=\"trades\"") != std::string::npos);
    assert(prometheus.find("stream=\"bookticker\"") != std::string::npos);
    assert(prometheus.find("stream=\"depth\"") != std::string::npos);
    assert(prometheus.find("hft_compressor_verify") == std::string::npos);
#else
    assert(result.status == hft_compressor::Status::DependencyUnavailable);
#endif
    return 0;
}



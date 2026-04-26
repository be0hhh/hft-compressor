#include "hft_compressor/compressor.hpp"

#include <algorithm>
#include <cstddef>
#include <fstream>
#include <sstream>
#include <system_error>
#include <utility>
#include <vector>

#include "format.hpp"
#include "hft_compressor/metrics.hpp"
#include "timing.hpp"

#if HFT_COMPRESSOR_WITH_ZSTD
#include <zstd.h>
#endif

namespace hft_compressor {
namespace {

void applyPipeline(CompressionResult& result, const PipelineDescriptor* pipeline) {
    if (pipeline == nullptr) return;
    result.pipelineId = std::string{pipeline->id};
    result.representation = std::string{pipeline->representation};
    result.transform = std::string{pipeline->transform};
    result.entropy = std::string{pipeline->entropy};
    result.profile = std::string{pipeline->profile};
    result.implementationKind = std::string{pipeline->implementationKind};
}

CompressionResult fail(Status status, const CompressionRequest& request, const PipelineDescriptor* pipeline, std::string error) {
    CompressionResult result{};
    result.status = status;
    result.error = std::move(error);
    result.inputPath = request.inputPath;
    result.streamType = inferStreamTypeFromPath(request.inputPath);
    applyPipeline(result, pipeline);
    return result;
}

std::uint64_t countLines(std::span<const std::uint8_t> data, bool isLastBlock) noexcept {
    std::uint64_t count = 0;
    for (const auto c : data) if (c == static_cast<std::uint8_t>('\n')) ++count;
    if (isLastBlock && !data.empty() && data.back() != static_cast<std::uint8_t>('\n')) ++count;
    return count;
}

std::string sessionIdForInput(const std::filesystem::path& inputPath) {
    const auto parent = inputPath.parent_path();
    if (!parent.empty()) return parent.filename().string();
    return std::string{"manual_"} + inputPath.stem().string();
}

bool readFileBytes(const std::filesystem::path& path, std::vector<std::uint8_t>& out) {
    std::ifstream in(path, std::ios::binary);
    if (!in) return false;
    in.seekg(0, std::ios::end);
    const auto size = in.tellg();
    if (size < 0) return false;
    in.seekg(0, std::ios::beg);
    out.resize(static_cast<std::size_t>(size));
    if (!out.empty()) in.read(reinterpret_cast<char*>(out.data()), static_cast<std::streamsize>(out.size()));
    return static_cast<bool>(in) || in.eof();
}

bool writeTextFile(const std::filesystem::path& path, const std::string& text) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    if (!out) return false;
    out.write(text.data(), static_cast<std::streamsize>(text.size()));
    return static_cast<bool>(out);
}

std::filesystem::path outputPathFor(const CompressionRequest& request, const PipelineDescriptor& pipeline, StreamType streamType) {
    const auto root = request.outputRoot.empty() ? defaultOutputRoot() : request.outputRoot;
    return root / std::string{pipeline.outputSlug} / "sessions" / sessionIdForInput(request.inputPath)
        / (std::string{streamTypeChannelName(streamType)} + ".hfc");
}

}  // namespace

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
    out << "  " << q << "encode_cycles" << q << ": " << result.encodeCycles << ',' << '\n';
    out << "  " << q << "decode_cycles" << q << ": " << result.decodeCycles << ',' << '\n';
    out << "  " << q << "encode_mb_per_sec" << q << ": " << encodeMbPerSec(result) << ',' << '\n';
    out << "  " << q << "decode_mb_per_sec" << q << ": " << decodeMbPerSec(result) << ',' << '\n';
    out << "  " << q << "roundtrip_ok" << q << ": " << (result.roundtripOk ? "true" : "false") << '\n';
    out << '}' << '\n';
    return out.str();
}

CompressionResult compress(const CompressionRequest& request) noexcept {
    const auto* pipeline = findPipeline(request.pipelineId);
    if (pipeline == nullptr) {
        auto result = fail(Status::UnsupportedPipeline, request, nullptr, "unknown pipeline id");
        metrics::recordRun(result);
        return result;
    }
    if (pipeline->availability == PipelineAvailability::DependencyUnavailable) {
        auto result = fail(Status::DependencyUnavailable, request, pipeline, std::string{pipeline->availabilityReason});
        metrics::recordRun(result);
        return result;
    }
    if (pipeline->availability == PipelineAvailability::NotImplemented) {
        auto result = fail(Status::NotImplemented, request, pipeline, std::string{pipeline->availabilityReason});
        metrics::recordRun(result);
        return result;
    }
    if (pipeline->id != std::string_view{"std.zstd_jsonl_blocks_v1"}) {
        auto result = fail(Status::UnsupportedPipeline, request, pipeline, "pipeline has no compressor implementation");
        metrics::recordRun(result);
        return result;
    }

#if !HFT_COMPRESSOR_WITH_ZSTD
    auto result = fail(Status::DependencyUnavailable, request, pipeline, "libzstd was not found at configure time");
    metrics::recordRun(result);
    return result;
#else
    if (request.inputPath.empty()) {
        auto result = fail(Status::InvalidArgument, request, pipeline, "input path is empty");
        metrics::recordRun(result);
        return result;
    }
    const StreamType streamType = inferStreamTypeFromPath(request.inputPath);
    if (streamType == StreamType::Unknown) {
        auto result = fail(Status::UnsupportedStream, request, pipeline, "expected trades.jsonl, bookticker.jsonl, or depth.jsonl");
        metrics::recordRun(result);
        return result;
    }
    std::vector<std::uint8_t> input;
    if (!readFileBytes(request.inputPath, input)) {
        auto result = fail(Status::IoError, request, pipeline, "failed to read input file");
        metrics::recordRun(result);
        return result;
    }
    const auto blockBytes = std::max<std::uint32_t>(request.blockBytes, 4096u);
    const auto outputPath = outputPathFor(request, *pipeline, streamType);
    std::error_code ec;
    std::filesystem::create_directories(outputPath.parent_path(), ec);
    if (ec) {
        auto result = fail(Status::IoError, request, pipeline, "failed to create output directory");
        metrics::recordRun(result);
        return result;
    }

    CompressionResult result{};
    applyPipeline(result, pipeline);
    result.streamType = streamType;
    result.inputPath = request.inputPath;
    result.outputPath = outputPath;
    result.metricsPath = outputPath.parent_path() / (outputPath.stem().string() + ".metrics.json");
    result.inputBytes = static_cast<std::uint64_t>(input.size());

    std::ofstream out(outputPath, std::ios::binary | std::ios::trunc);
    if (!out) {
        auto failed = fail(Status::IoError, request, pipeline, "failed to open output file");
        metrics::recordRun(failed);
        return failed;
    }

    format::FileHeader fileHeader{};
    fileHeader.stream = format::streamToWire(streamType);
    fileHeader.blockBytes = blockBytes;
    fileHeader.inputBytes = result.inputBytes;
    const auto placeholderHeader = format::serializeFileHeader(fileHeader);
    out.write(reinterpret_cast<const char*>(placeholderHeader.data()), static_cast<std::streamsize>(placeholderHeader.size()));

    const auto encodeStartNs = timing::nowNs();
    const auto encodeStartCycles = timing::readCycles();
    std::vector<std::uint8_t> compressed;
    for (std::size_t offset = 0; offset < input.size(); offset += blockBytes) {
        const auto remaining = input.size() - offset;
        const auto plainSize = std::min<std::size_t>(remaining, blockBytes);
        const bool isLast = offset + plainSize >= input.size();
        const auto bound = ZSTD_compressBound(plainSize);
        compressed.resize(bound);
        const auto written = ZSTD_compress(compressed.data(), compressed.size(), input.data() + offset, plainSize, request.zstdLevel);
        if (ZSTD_isError(written)) {
            auto failed = fail(Status::DecodeError, request, pipeline, ZSTD_getErrorName(written));
            metrics::recordRun(failed);
            return failed;
        }
        format::BlockHeader block{};
        block.uncompressedBytes = static_cast<std::uint32_t>(plainSize);
        block.compressedBytes = static_cast<std::uint32_t>(written);
        block.lineCount = static_cast<std::uint32_t>(countLines({input.data() + offset, plainSize}, isLast));
        block.firstByteOffset = static_cast<std::uint64_t>(offset);
        const auto blockHeader = format::serializeBlockHeader(block);
        out.write(reinterpret_cast<const char*>(blockHeader.data()), static_cast<std::streamsize>(blockHeader.size()));
        out.write(reinterpret_cast<const char*>(compressed.data()), static_cast<std::streamsize>(written));
        result.outputBytes += format::kBlockHeaderBytes + static_cast<std::uint64_t>(written);
        result.lineCount += block.lineCount;
        ++result.blockCount;
    }
    result.outputBytes += format::kFileHeaderBytes;
    result.encodeCycles = timing::readCycles() - encodeStartCycles;
    result.encodeNs = timing::nowNs() - encodeStartNs;

    fileHeader.outputBytes = result.outputBytes;
    fileHeader.lineCount = result.lineCount;
    fileHeader.blockCount = result.blockCount;
    const auto finalHeader = format::serializeFileHeader(fileHeader);
    out.seekp(0, std::ios::beg);
    out.write(reinterpret_cast<const char*>(finalHeader.data()), static_cast<std::streamsize>(finalHeader.size()));
    out.close();
    if (!out) {
        auto failed = fail(Status::IoError, request, pipeline, "failed to write compressed file");
        metrics::recordRun(failed);
        return failed;
    }

    std::vector<std::uint8_t> compressedFile;
    if (readFileBytes(outputPath, compressedFile)) {
        std::size_t decodedOffset = 0;
        bool decodedMatchesInput = true;
        const auto decodeStartNs = timing::nowNs();
        const auto decodeStartCycles = timing::readCycles();
        const auto decodeStatus = decodeHfcBuffer(compressedFile, [&](std::span<const std::uint8_t> block) {
            if (decodedOffset + block.size() > input.size()) {
                decodedMatchesInput = false;
                return false;
            }
            decodedMatchesInput = std::equal(block.begin(), block.end(), input.begin() + static_cast<std::ptrdiff_t>(decodedOffset));
            decodedOffset += block.size();
            return decodedMatchesInput;
        });
        result.decodeCycles = timing::readCycles() - decodeStartCycles;
        result.decodeNs = timing::nowNs() - decodeStartNs;
        result.roundtripOk = isOk(decodeStatus) && decodedMatchesInput && decodedOffset == input.size();
    }
    result.status = result.roundtripOk ? Status::Ok : Status::DecodeError;
    if (!result.roundtripOk) result.error = "roundtrip check failed";

    (void)writeTextFile(result.metricsPath, toMetricsJson(result));
    metrics::recordRun(result);
    return result;
#endif
}

Status decodeHfcBuffer(std::span<const std::uint8_t> compressedFile,
                    const DecodedBlockCallback& onBlock) noexcept {
#if !HFT_COMPRESSOR_WITH_ZSTD
    (void)compressedFile;
    (void)onBlock;
    return Status::DependencyUnavailable;
#else
    if (compressedFile.size() < format::kFileHeaderBytes || !onBlock) return Status::InvalidArgument;
    format::FileHeader fileHeader{};
    if (!format::parseFileHeader(compressedFile.data(), compressedFile.size(), fileHeader)) return Status::CorruptData;
    if (fileHeader.magic != format::kFileMagic || fileHeader.version != format::kVersion) return Status::CorruptData;
    if (fileHeader.codec != format::kCodecZstdJsonlBlocksV1) return Status::CorruptData;

    std::size_t offset = format::kFileHeaderBytes;
    std::vector<std::uint8_t> decoded;
    for (std::uint64_t blockIndex = 0; blockIndex < fileHeader.blockCount; ++blockIndex) {
        if (compressedFile.size() - offset < format::kBlockHeaderBytes) return Status::CorruptData;
        format::BlockHeader block{};
        if (!format::parseBlockHeader(compressedFile.data() + offset, compressedFile.size() - offset, block)) return Status::CorruptData;
        if (block.magic != format::kBlockMagic) return Status::CorruptData;
        offset += format::kBlockHeaderBytes;
        if (compressedFile.size() - offset < block.compressedBytes) return Status::CorruptData;
        decoded.resize(block.uncompressedBytes);
        const auto written = ZSTD_decompress(decoded.data(), decoded.size(), compressedFile.data() + offset, block.compressedBytes);
        if (ZSTD_isError(written) || written != block.uncompressedBytes) return Status::DecodeError;
        if (!onBlock(decoded)) return Status::Ok;
        offset += block.compressedBytes;
    }
    return Status::Ok;
#endif
}

}  // namespace hft_compressor

#include "codecs/zstd_jsonl_blocks/ZstdJsonlBlocks.hpp"

#include <algorithm>
#include <cstddef>
#include <fstream>
#include <system_error>
#include <vector>

#include "common/CompressionInternals.hpp"
#include "common/timing.hpp"
#include "container/hfc/format.hpp"
#include "hft_compressor/metrics.hpp"

#if HFT_COMPRESSOR_WITH_ZSTD
#include <zstd.h>
#endif

namespace hft_compressor::codecs::zstd_jsonl_blocks {

CompressionResult compress(const CompressionRequest& request, const PipelineDescriptor& pipeline) noexcept {
#if !HFT_COMPRESSOR_WITH_ZSTD
    auto result = internal::fail(Status::DependencyUnavailable, request, &pipeline, "libzstd was not found at configure time");
    metrics::recordRun(result);
    return result;
#else
    if (request.inputPath.empty()) {
        auto result = internal::fail(Status::InvalidArgument, request, &pipeline, "input path is empty");
        metrics::recordRun(result);
        return result;
    }
    const StreamType streamType = inferStreamTypeFromPath(request.inputPath);
    if (streamType == StreamType::Unknown) {
        auto result = internal::fail(Status::UnsupportedStream, request, &pipeline, "expected trades.jsonl, bookticker.jsonl, or depth.jsonl");
        metrics::recordRun(result);
        return result;
    }
    std::vector<std::uint8_t> input;
    if (!internal::readFileBytes(request.inputPath, input)) {
        auto result = internal::fail(Status::IoError, request, &pipeline, "failed to read input file");
        metrics::recordRun(result);
        return result;
    }
    const auto blockBytes = std::max<std::uint32_t>(request.blockBytes, 4096u);
    const auto outputPath = internal::outputPathFor(request, pipeline, streamType);
    std::error_code ec;
    std::filesystem::create_directories(outputPath.parent_path(), ec);
    if (ec) {
        auto result = internal::fail(Status::IoError, request, &pipeline, "failed to create output directory");
        metrics::recordRun(result);
        return result;
    }

    CompressionResult result{};
    internal::applyPipeline(result, &pipeline);
    result.streamType = streamType;
    result.inputPath = request.inputPath;
    result.outputPath = outputPath;
    result.metricsPath = outputPath.parent_path() / (outputPath.stem().string() + ".metrics.json");
    result.inputBytes = static_cast<std::uint64_t>(input.size());

    std::ofstream out(outputPath, std::ios::binary | std::ios::trunc);
    if (!out) {
        auto failed = internal::fail(Status::IoError, request, &pipeline, "failed to open output file");
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
            auto failed = internal::fail(Status::DecodeError, request, &pipeline, ZSTD_getErrorName(written));
            metrics::recordRun(failed);
            return failed;
        }
        format::BlockHeader block{};
        block.uncompressedBytes = static_cast<std::uint32_t>(plainSize);
        block.compressedBytes = static_cast<std::uint32_t>(written);
        block.lineCount = static_cast<std::uint32_t>(internal::countLines({input.data() + offset, plainSize}, isLast));
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
        auto failed = internal::fail(Status::IoError, request, &pipeline, "failed to write compressed file");
        metrics::recordRun(failed);
        return failed;
    }

    std::vector<std::uint8_t> compressedFile;
    if (internal::readFileBytes(outputPath, compressedFile)) {
        std::size_t decodedOffset = 0;
        bool decodedMatchesInput = true;
        const auto decodeStartNs = timing::nowNs();
        const auto decodeStartCycles = timing::readCycles();
        const auto decodeStatus = decode(compressedFile, [&](std::span<const std::uint8_t> block) {
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

    (void)internal::writeTextFile(result.metricsPath, toMetricsJson(result));
    metrics::recordRun(result);
    return result;
#endif
}

Status decode(std::span<const std::uint8_t> compressedFile, const DecodedBlockCallback& onBlock) noexcept {
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

}  // namespace hft_compressor::codecs::zstd_jsonl_blocks

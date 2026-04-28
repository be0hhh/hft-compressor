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

namespace {

bool validHeader(const format::FileHeader& fileHeader) noexcept {
    return fileHeader.magic == format::kFileMagic
        && format::isSupportedVersion(fileHeader.version)
        && fileHeader.codec == format::kCodecZstdJsonlBlocksV1
        && fileHeader.blockBytes != 0u;
}

bool validBlock(const format::BlockHeader& block,
                const format::FileHeader& fileHeader,
                std::uint64_t expectedOffset) noexcept {
    if (block.magic != format::kBlockMagic) return false;
    if (block.uncompressedBytes == 0u || block.compressedBytes == 0u) return false;
    if (block.uncompressedBytes > fileHeader.blockBytes) return false;
    if (block.firstByteOffset != expectedOffset) return false;
    return true;
}

#if HFT_COMPRESSOR_WITH_ZSTD
Status decodeBlockPayload(const format::FileHeader& fileHeader,
                          const format::BlockHeader& block,
                          std::span<const std::uint8_t> compressed,
                          std::vector<std::uint8_t>& decoded,
                          const DecodedBlockCallback& onBlock,
                          bool& shouldContinue) noexcept {
    if (fileHeader.version >= format::kVersion2) {
        const auto compressedCrc = format::crc32c(compressed);
        if (compressedCrc != format::compressedCrc32c(block)) return Status::CorruptData;
    }

    decoded.resize(block.uncompressedBytes);
    const auto written = ZSTD_decompress(decoded.data(), decoded.size(), compressed.data(), compressed.size());
    if (ZSTD_isError(written) || written != block.uncompressedBytes) return Status::DecodeError;

    if (fileHeader.version >= format::kVersion2) {
        const auto uncompressedCrc = format::crc32c(decoded);
        if (uncompressedCrc != format::uncompressedCrc32c(block)) return Status::CorruptData;
    }

    shouldContinue = onBlock(decoded);
    return Status::Ok;
}
#endif

bool validHeaderCrc(const format::FileHeader& fileHeader) noexcept {
    return fileHeader.version < format::kVersion2
        || format::storedHeaderCrc32c(fileHeader) == format::headerCrc32c(fileHeader);
}

}  // namespace

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
    fileHeader.version = format::kVersion2;
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
        format::setBlockChecksums(block,
                                  format::crc32c({compressed.data(), written}),
                                  format::crc32c({input.data() + offset, plainSize}));
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
    format::setHeaderCrc32c(fileHeader, format::headerCrc32c(fileHeader));
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
    if (!validHeader(fileHeader)) return Status::CorruptData;
    if (!validHeaderCrc(fileHeader)) return Status::CorruptData;
    if (fileHeader.outputBytes != 0u && fileHeader.outputBytes != compressedFile.size()) return Status::CorruptData;

    std::size_t offset = format::kFileHeaderBytes;
    std::vector<std::uint8_t> decoded;
    std::uint64_t expectedOffset = 0;
    for (std::uint64_t blockIndex = 0; blockIndex < fileHeader.blockCount; ++blockIndex) {
        if (compressedFile.size() - offset < format::kBlockHeaderBytes) return Status::CorruptData;
        format::BlockHeader block{};
        if (!format::parseBlockHeader(compressedFile.data() + offset, compressedFile.size() - offset, block)) return Status::CorruptData;
        if (!validBlock(block, fileHeader, expectedOffset)) return Status::CorruptData;
        offset += format::kBlockHeaderBytes;
        if (compressedFile.size() - offset < block.compressedBytes) return Status::CorruptData;
        bool shouldContinue = true;
        const auto status = decodeBlockPayload(fileHeader,
                                               block,
                                               {compressedFile.data() + offset, block.compressedBytes},
                                               decoded,
                                               onBlock,
                                               shouldContinue);
        if (!isOk(status)) return status;
        offset += block.compressedBytes;
        expectedOffset += block.uncompressedBytes;
        if (!shouldContinue) return Status::Ok;
    }
    if (offset != compressedFile.size()) return Status::CorruptData;
    if (expectedOffset != fileHeader.inputBytes) return Status::CorruptData;
    return Status::Ok;
#endif
}

Status decodeFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept {
#if !HFT_COMPRESSOR_WITH_ZSTD
    (void)path;
    (void)onBlock;
    return Status::DependencyUnavailable;
#else
    if (path.empty() || !onBlock) return Status::InvalidArgument;
    std::ifstream in(path, std::ios::binary);
    if (!in) return Status::IoError;

    std::uint8_t fileHeaderBytes[format::kFileHeaderBytes]{};
    in.read(reinterpret_cast<char*>(fileHeaderBytes), static_cast<std::streamsize>(sizeof(fileHeaderBytes)));
    if (in.gcount() != static_cast<std::streamsize>(sizeof(fileHeaderBytes))) return Status::CorruptData;

    format::FileHeader fileHeader{};
    if (!format::parseFileHeader(fileHeaderBytes, sizeof(fileHeaderBytes), fileHeader)) return Status::CorruptData;
    if (!validHeader(fileHeader)) return Status::CorruptData;
    if (!validHeaderCrc(fileHeader)) return Status::CorruptData;

    std::vector<std::uint8_t> compressed;
    std::vector<std::uint8_t> decoded;
    std::uint64_t expectedOffset = 0;
    std::uint64_t totalRead = format::kFileHeaderBytes;
    for (std::uint64_t blockIndex = 0; blockIndex < fileHeader.blockCount; ++blockIndex) {
        std::uint8_t blockHeaderBytes[format::kBlockHeaderBytes]{};
        in.read(reinterpret_cast<char*>(blockHeaderBytes), static_cast<std::streamsize>(sizeof(blockHeaderBytes)));
        if (in.gcount() != static_cast<std::streamsize>(sizeof(blockHeaderBytes))) return Status::CorruptData;

        format::BlockHeader block{};
        if (!format::parseBlockHeader(blockHeaderBytes, sizeof(blockHeaderBytes), block)) return Status::CorruptData;
        if (!validBlock(block, fileHeader, expectedOffset)) return Status::CorruptData;
        totalRead += format::kBlockHeaderBytes;

        compressed.resize(block.compressedBytes);
        in.read(reinterpret_cast<char*>(compressed.data()), static_cast<std::streamsize>(compressed.size()));
        if (in.gcount() != static_cast<std::streamsize>(compressed.size())) return Status::CorruptData;
        totalRead += block.compressedBytes;

        bool shouldContinue = true;
        const auto status = decodeBlockPayload(fileHeader, block, compressed, decoded, onBlock, shouldContinue);
        if (!isOk(status)) return status;
        expectedOffset += block.uncompressedBytes;
        if (!shouldContinue) return Status::Ok;
    }

    if (fileHeader.outputBytes != 0u && totalRead != fileHeader.outputBytes) return Status::CorruptData;
    if (expectedOffset != fileHeader.inputBytes) return Status::CorruptData;

    char extra = 0;
    if (in.read(&extra, 1)) return Status::CorruptData;
    return Status::Ok;
#endif
}

}  // namespace hft_compressor::codecs::zstd_jsonl_blocks

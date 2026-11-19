#include "StandardJsonlBlocks.hpp"

#include <algorithm>
#include <fstream>
#include <string>
#include <system_error>
#include <utility>
#include <vector>

#include "hft_compressor/CorpusLab.hpp"
#include "../../Common/CompressionInternals.hpp"
#include "../../Common/Timing.hpp"
#include "../../Container/Hfc/Format.hpp"

namespace hft_compressor::codecs::standard_jsonl_blocks {
namespace {

bool codecAvailable(const CodecSpec& spec) noexcept {
    return spec.compressPayload != nullptr && spec.decompressPayload != nullptr;
}

bool validHeader(const format::FileHeader& fileHeader, const CodecSpec& spec) noexcept {
    return fileHeader.magic == format::kFileMagic
        && fileHeader.codec == spec.codec
        && fileHeader.blockBytes != 0u
        && format::streamFromWire(fileHeader.stream) != StreamType::Unknown;
}

bool validBlock(const format::BlockHeader& block,
                const format::FileHeader& fileHeader,
                std::uint64_t expectedOffset) noexcept {
    return block.magic == format::kBlockMagic
        && block.uncompressedBytes != 0u
        && block.compressedBytes != 0u
        && block.uncompressedBytes <= fileHeader.blockBytes
        && block.firstByteOffset == expectedOffset;
}

bool validHeaderCrc(const format::FileHeader& fileHeader) noexcept {
    return format::storedHeaderCrc32c(fileHeader) == format::headerCrc32c(fileHeader);
}

ReplayArtifactInfo failArtifact(const std::filesystem::path& path, Status status, std::string error) {
    ReplayArtifactInfo info{};
    info.status = status;
    info.path = path;
    info.error = std::move(error);
    return info;
}

Status decodeBlockPayload(const format::FileHeader& fileHeader,
                          const format::BlockHeader& block,
                          std::span<const std::uint8_t> compressed,
                          std::vector<std::uint8_t>& decoded,
                          const DecodedBlockCallback& onBlock,
                          bool& shouldContinue,
                          const CodecSpec& spec) noexcept {
    if (format::crc32c(compressed) != format::compressedCrc32c(block)) return Status::CorruptData;

    std::string error;
    const auto status = spec.decompressPayload(compressed, block.uncompressedBytes, decoded, error);
    (void)error;
    if (!isOk(status)) return status;
    if (decoded.size() != block.uncompressedBytes) return Status::DecodeError;
    if (format::crc32c(decoded) != format::uncompressedCrc32c(block)) return Status::CorruptData;
    if (block.uncompressedBytes > fileHeader.blockBytes) return Status::CorruptData;

    shouldContinue = onBlock(decoded);
    return Status::Ok;
}

}  // namespace

CompressionResult compress(const CompressionRequest& request,
                           const PipelineDescriptor& pipeline,
                           const CodecSpec& spec) noexcept {
    if (!codecAvailable(spec)) {
        return internal::fail(Status::DependencyUnavailable, request, &pipeline, std::string{spec.dependencyError});
    }
    if (request.inputPath.empty()) {
        return internal::fail(Status::InvalidArgument, request, &pipeline, "input path is empty");
    }
    const StreamType streamType = inferStreamTypeFromPath(request.inputPath);
    if (streamType == StreamType::Unknown) {
        return internal::fail(Status::UnsupportedStream, request, &pipeline, "expected canonical binary stream slice (.bin/.hbs)");
    }
    std::vector<std::uint8_t> input;
    if (!internal::readFileBytes(request.inputPath, input)) {
        return internal::fail(Status::IoError, request, &pipeline, "failed to read input file");
    }
    BinarySliceView slice{};
    const auto sliceStatus = parseBinarySlice(input, slice);
    if (!isOk(sliceStatus) || slice.layout.streamType != streamType) {
        return internal::fail(
            isOk(sliceStatus) ? Status::UnsupportedStream : sliceStatus,
            request,
            &pipeline,
            "input is not a valid canonical binary slice for stream");
    }

    const auto blockBytes = std::max<std::uint32_t>(request.blockBytes, 4096u);
    const auto outputPath = internal::outputPathFor(request, pipeline, streamType);
    std::error_code ec;
    std::filesystem::create_directories(outputPath.parent_path(), ec);
    if (ec) {
        return internal::fail(Status::IoError, request, &pipeline, "failed to create output directory");
    }

    CompressionResult result{};
    internal::applyPipeline(result, &pipeline);
    result.streamType = streamType;
    result.inputPath = request.inputPath;
    result.outputPath = outputPath;
    result.metricsPath = outputPath.parent_path() / (outputPath.stem().string() + ".metrics.json");
    result.inputBytes = static_cast<std::uint64_t>(input.size());
    result.lineCount = slice.recordCount;

    std::ofstream out(outputPath, std::ios::binary | std::ios::trunc);
    if (!out) {
        return internal::fail(Status::IoError, request, &pipeline, "failed to open output file");
    }

    format::FileHeader fileHeader{};
    fileHeader.codec = spec.codec;
    fileHeader.stream = format::streamToWire(streamType);
    fileHeader.blockBytes = blockBytes;
    fileHeader.inputBytes = result.inputBytes;
    const auto placeholderHeader = format::serializeFileHeader(fileHeader);
    out.write(reinterpret_cast<const char*>(placeholderHeader.data()),
              static_cast<std::streamsize>(placeholderHeader.size()));

    const auto encodeStartNs = timing::nowNs();
    const auto encodeStartCycles = timing::readCycles();
    std::vector<std::uint8_t> compressed;
    for (std::size_t offset = 0u; offset < input.size(); offset += blockBytes) {
        const auto remaining = input.size() - offset;
        const auto plainSize = std::min<std::size_t>(remaining, blockBytes);
        std::string encodeError;
        const auto encodeStatus = spec.compressPayload(
            {input.data() + offset, plainSize}, request.zstdLevel, compressed, encodeError);
        if (!isOk(encodeStatus)) {
            return internal::fail(
                encodeStatus,
                request,
                &pipeline,
                encodeError.empty() ? "failed to compress block" : encodeError);
        }

        format::BlockHeader block{};
        block.uncompressedBytes = static_cast<std::uint32_t>(plainSize);
        block.compressedBytes = static_cast<std::uint32_t>(compressed.size());
        block.lineCount = (offset == 0u) ? slice.recordCount : 0u;
        block.firstByteOffset = static_cast<std::uint64_t>(offset);
        format::setBlockChecksums(block,
                                  format::crc32c(compressed),
                                  format::crc32c({input.data() + offset, plainSize}));
        const auto blockHeader = format::serializeBlockHeader(block);
        out.write(reinterpret_cast<const char*>(blockHeader.data()),
                  static_cast<std::streamsize>(blockHeader.size()));
        out.write(reinterpret_cast<const char*>(compressed.data()),
                  static_cast<std::streamsize>(compressed.size()));
        result.outputBytes += format::kBlockHeaderBytes + static_cast<std::uint64_t>(compressed.size());
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
    out.write(reinterpret_cast<const char*>(finalHeader.data()),
              static_cast<std::streamsize>(finalHeader.size()));
    out.close();
    if (!out) {
        return internal::fail(Status::IoError, request, &pipeline, "failed to write compressed file");
    }

    std::vector<std::uint8_t> compressedFile;
    if (internal::readFileBytes(outputPath, compressedFile)) {
        std::size_t decodedOffset = 0u;
        bool decodedMatchesInput = true;
        const auto decodeStartNs = timing::nowNs();
        const auto decodeStartCycles = timing::readCycles();
        const auto decodeStatus = decode(compressedFile, [&](std::span<const std::uint8_t> block) {
            if (decodedOffset + block.size() > input.size()) {
                decodedMatchesInput = false;
                return false;
            }
            decodedMatchesInput = std::equal(
                block.begin(),
                block.end(),
                input.begin() + static_cast<std::ptrdiff_t>(decodedOffset));
            decodedOffset += block.size();
            return decodedMatchesInput;
        }, spec);
        result.decodeCycles = timing::readCycles() - decodeStartCycles;
        result.decodeNs = timing::nowNs() - decodeStartNs;
        result.roundtripOk = isOk(decodeStatus) && decodedMatchesInput && decodedOffset == input.size();
    }
    result.status = result.roundtripOk ? Status::Ok : Status::DecodeError;
    if (!result.roundtripOk) result.error = "roundtrip check failed";

    (void)internal::writeTextFile(result.metricsPath, toMetricsJson(result));
    return result;
}

ReplayArtifactInfo inspectArtifact(const std::filesystem::path& path,
                                   const PipelineDescriptor& pipeline,
                                   const CodecSpec& spec) noexcept {
    if (!codecAvailable(spec)) return failArtifact(path, Status::DependencyUnavailable, std::string{spec.dependencyError});
    if (path.empty()) return failArtifact(path, Status::InvalidArgument, "artifact path is empty");
    std::ifstream in(path, std::ios::binary);
    if (!in) return failArtifact(path, Status::IoError, "failed to open artifact");

    std::uint8_t fileHeaderBytes[format::kFileHeaderBytes]{};
    in.read(reinterpret_cast<char*>(fileHeaderBytes), static_cast<std::streamsize>(sizeof(fileHeaderBytes)));
    if (in.gcount() != static_cast<std::streamsize>(sizeof(fileHeaderBytes))) {
        return failArtifact(path, Status::CorruptData, "truncated artifact header");
    }

    format::FileHeader header{};
    if (!format::parseFileHeader(fileHeaderBytes, sizeof(fileHeaderBytes), header) || !validHeader(header, spec)) {
        return failArtifact(path, Status::CorruptData, "invalid artifact header");
    }
    if (!validHeaderCrc(header)) return failArtifact(path, Status::CorruptData, "artifact header crc mismatch");

    std::uint64_t fileOffset = format::kFileHeaderBytes;
    std::uint64_t expectedPlainOffset = 0u;
    std::vector<std::uint8_t> compressed;
    for (std::uint64_t blockIndex = 0u; blockIndex < header.blockCount; ++blockIndex) {
        std::uint8_t blockHeaderBytes[format::kBlockHeaderBytes]{};
        in.read(reinterpret_cast<char*>(blockHeaderBytes), static_cast<std::streamsize>(sizeof(blockHeaderBytes)));
        if (in.gcount() != static_cast<std::streamsize>(sizeof(blockHeaderBytes))) {
            return failArtifact(path, Status::CorruptData, "truncated artifact block header");
        }
        format::BlockHeader block{};
        if (!format::parseBlockHeader(blockHeaderBytes, sizeof(blockHeaderBytes), block)
            || !validBlock(block, header, expectedPlainOffset)) {
            return failArtifact(path, Status::CorruptData, "invalid artifact block header");
        }
        compressed.resize(block.compressedBytes);
        in.read(reinterpret_cast<char*>(compressed.data()), static_cast<std::streamsize>(compressed.size()));
        if (in.gcount() != static_cast<std::streamsize>(compressed.size())) {
            return failArtifact(path, Status::CorruptData, "truncated artifact payload");
        }
        if (format::crc32c(compressed) != format::compressedCrc32c(block)) {
            return failArtifact(path, Status::CorruptData, "artifact compressed crc mismatch");
        }
        fileOffset += format::kBlockHeaderBytes + block.compressedBytes;
        expectedPlainOffset += block.uncompressedBytes;
    }

    if (expectedPlainOffset != header.inputBytes) {
        return failArtifact(path, Status::CorruptData, "artifact decoded byte count mismatch");
    }
    if (header.outputBytes != 0u && fileOffset != header.outputBytes) {
        return failArtifact(path, Status::CorruptData, "artifact output byte count mismatch");
    }
    char extra = 0;
    if (in.read(&extra, 1)) return failArtifact(path, Status::CorruptData, "trailing bytes after artifact blocks");

    ReplayArtifactInfo info{};
    info.status = Status::Ok;
    info.found = true;
    info.path = path;
    info.formatId = std::string{spec.formatId};
    info.pipelineId = std::string{pipeline.id};
    info.transform = std::string{pipeline.transform};
    info.entropy = std::string{pipeline.entropy};
    info.streamType = format::streamFromWire(header.stream);
    info.inputBytes = header.inputBytes;
    info.outputBytes = header.outputBytes;
    info.lineCount = header.lineCount;
    info.blockCount = header.blockCount;
    return info;
}

Status decode(std::span<const std::uint8_t> compressedFile,
              const DecodedBlockCallback& onBlock,
              const CodecSpec& spec) noexcept {
    if (!codecAvailable(spec)) return Status::DependencyUnavailable;
    if (compressedFile.size() < format::kFileHeaderBytes || !onBlock) return Status::InvalidArgument;

    format::FileHeader fileHeader{};
    if (!format::parseFileHeader(compressedFile.data(), compressedFile.size(), fileHeader)
        || !validHeader(fileHeader, spec)
        || !validHeaderCrc(fileHeader)) {
        return Status::CorruptData;
    }
    if (fileHeader.outputBytes != 0u && fileHeader.outputBytes != compressedFile.size()) {
        return Status::CorruptData;
    }

    std::size_t offset = format::kFileHeaderBytes;
    std::vector<std::uint8_t> decoded;
    std::uint64_t expectedOffset = 0u;
    for (std::uint64_t blockIndex = 0u; blockIndex < fileHeader.blockCount; ++blockIndex) {
        if (compressedFile.size() - offset < format::kBlockHeaderBytes) return Status::CorruptData;
        format::BlockHeader block{};
        if (!format::parseBlockHeader(compressedFile.data() + offset, compressedFile.size() - offset, block)
            || !validBlock(block, fileHeader, expectedOffset)) {
            return Status::CorruptData;
        }
        offset += format::kBlockHeaderBytes;
        if (compressedFile.size() - offset < block.compressedBytes) return Status::CorruptData;
        bool shouldContinue = true;
        const auto status = decodeBlockPayload(
            fileHeader,
            block,
            {compressedFile.data() + offset, block.compressedBytes},
            decoded,
            onBlock,
            shouldContinue,
            spec);
        if (!isOk(status)) return status;
        offset += block.compressedBytes;
        expectedOffset += block.uncompressedBytes;
        if (!shouldContinue) return Status::CallbackStopped;
    }

    if (offset != compressedFile.size() || expectedOffset != fileHeader.inputBytes) return Status::CorruptData;
    return Status::Ok;
}

Status decodeFile(const std::filesystem::path& path,
                  const DecodedBlockCallback& onBlock,
                  const CodecSpec& spec) noexcept {
    if (!codecAvailable(spec)) return Status::DependencyUnavailable;
    if (path.empty() || !onBlock) return Status::InvalidArgument;
    std::ifstream in(path, std::ios::binary);
    if (!in) return Status::IoError;

    std::uint8_t fileHeaderBytes[format::kFileHeaderBytes]{};
    in.read(reinterpret_cast<char*>(fileHeaderBytes), static_cast<std::streamsize>(sizeof(fileHeaderBytes)));
    if (in.gcount() != static_cast<std::streamsize>(sizeof(fileHeaderBytes))) return Status::CorruptData;

    format::FileHeader fileHeader{};
    if (!format::parseFileHeader(fileHeaderBytes, sizeof(fileHeaderBytes), fileHeader)
        || !validHeader(fileHeader, spec)
        || !validHeaderCrc(fileHeader)) {
        return Status::CorruptData;
    }

    std::uint64_t bytesRead = format::kFileHeaderBytes;
    std::uint64_t expectedOffset = 0u;
    std::vector<std::uint8_t> compressed;
    std::vector<std::uint8_t> decoded;
    for (std::uint64_t blockIndex = 0u; blockIndex < fileHeader.blockCount; ++blockIndex) {
        std::uint8_t blockHeaderBytes[format::kBlockHeaderBytes]{};
        in.read(reinterpret_cast<char*>(blockHeaderBytes), static_cast<std::streamsize>(sizeof(blockHeaderBytes)));
        if (in.gcount() != static_cast<std::streamsize>(sizeof(blockHeaderBytes))) return Status::CorruptData;

        format::BlockHeader block{};
        if (!format::parseBlockHeader(blockHeaderBytes, sizeof(blockHeaderBytes), block)
            || !validBlock(block, fileHeader, expectedOffset)) {
            return Status::CorruptData;
        }
        compressed.resize(block.compressedBytes);
        in.read(reinterpret_cast<char*>(compressed.data()), static_cast<std::streamsize>(compressed.size()));
        if (in.gcount() != static_cast<std::streamsize>(compressed.size())) return Status::CorruptData;

        bool shouldContinue = true;
        const auto status = decodeBlockPayload(fileHeader, block, compressed, decoded, onBlock, shouldContinue, spec);
        if (!isOk(status)) return status;
        bytesRead += format::kBlockHeaderBytes + block.compressedBytes;
        expectedOffset += block.uncompressedBytes;
        if (!shouldContinue) return Status::CallbackStopped;
    }

    if (expectedOffset != fileHeader.inputBytes) return Status::CorruptData;
    if (fileHeader.outputBytes != 0u && bytesRead != fileHeader.outputBytes) return Status::CorruptData;
    char extra = 0;
    if (in.read(&extra, 1)) return Status::CorruptData;
    return Status::Ok;
}

}  // namespace hft_compressor::codecs::standard_jsonl_blocks

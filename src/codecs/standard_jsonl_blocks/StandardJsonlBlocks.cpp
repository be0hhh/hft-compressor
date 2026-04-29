#include "codecs/standard_jsonl_blocks/StandardJsonlBlocks.hpp"

#include <algorithm>
#include <charconv>
#include <fstream>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "common/CompressionInternals.hpp"
#include "common/timing.hpp"
#include "container/hfc/format.hpp"
#include "hft_compressor/metrics.hpp"

namespace hft_compressor::codecs::standard_jsonl_blocks {
namespace {

bool codecAvailable(const CodecSpec& spec) noexcept {
    return spec.compressPayload != nullptr && spec.decompressPayload != nullptr;
}

bool validHeader(const format::FileHeader& fileHeader, const CodecSpec& spec) noexcept {
    return fileHeader.magic == format::kFileMagic
        && fileHeader.version == format::kVersion2
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

struct JsonCursor {
    std::string_view text{};
    std::size_t pos{0};

    void skipSpaces() noexcept {
        while (pos < text.size()) {
            const char c = text[pos];
            if (c != ' ' && c != '\t' && c != '\r') break;
            ++pos;
        }
    }

    bool consume(char c) noexcept {
        skipSpaces();
        if (pos >= text.size() || text[pos] != c) return false;
        ++pos;
        return true;
    }

    bool peek(char c) noexcept {
        skipSpaces();
        return pos < text.size() && text[pos] == c;
    }

    bool parseInt64(std::int64_t& out) noexcept {
        skipSpaces();
        if (pos >= text.size()) return false;
        const char* begin = text.data() + pos;
        const char* end = text.data() + text.size();
        const auto [ptr, ec] = std::from_chars(begin, end, out);
        if (ec != std::errc{} || ptr == begin) return false;
        pos = static_cast<std::size_t>(ptr - text.data());
        return true;
    }

    bool finish() noexcept {
        skipSpaces();
        return pos == text.size();
    }
};

bool validSide(std::int64_t side) noexcept {
    return side == 0 || side == 1;
}

bool validTradeLine(std::string_view line) noexcept {
    JsonCursor p{line};
    std::int64_t value = 0;
    std::int64_t side = 0;
    return p.consume('[')
        && p.parseInt64(value) && p.consume(',')
        && p.parseInt64(value) && p.consume(',')
        && p.parseInt64(side) && validSide(side) && p.consume(',')
        && p.parseInt64(value)
        && p.consume(']') && p.finish();
}

bool validBookTickerLine(std::string_view line) noexcept {
    JsonCursor p{line};
    std::int64_t value = 0;
    return p.consume('[')
        && p.parseInt64(value) && p.consume(',')
        && p.parseInt64(value) && p.consume(',')
        && p.parseInt64(value) && p.consume(',')
        && p.parseInt64(value) && p.consume(',')
        && p.parseInt64(value)
        && p.consume(']') && p.finish();
}

bool validDepthLevel(JsonCursor& p) noexcept {
    std::int64_t value = 0;
    std::int64_t side = 0;
    return p.consume('[')
        && p.parseInt64(value) && p.consume(',')
        && p.parseInt64(value) && p.consume(',')
        && p.parseInt64(side) && validSide(side)
        && p.consume(']');
}

bool validDepthLine(std::string_view line) noexcept {
    JsonCursor p{line};
    std::int64_t tsNs = 0;
    if (!p.consume('[') || !p.peek('[')) return false;
    while (p.peek('[')) {
        if (!validDepthLevel(p) || !p.consume(',')) return false;
    }
    return p.parseInt64(tsNs) && p.consume(']') && p.finish();
}

bool validateStrictJsonl(std::span<const std::uint8_t> input, StreamType streamType) noexcept {
    std::size_t lineStart = 0;
    while (lineStart < input.size()) {
        std::size_t lineEnd = lineStart;
        while (lineEnd < input.size() && input[lineEnd] != static_cast<std::uint8_t>('\n')) ++lineEnd;
        std::string_view line{reinterpret_cast<const char*>(input.data() + lineStart), lineEnd - lineStart};
        if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
        if (line.empty()) return false;
        const bool ok = streamType == StreamType::Trades ? validTradeLine(line)
            : streamType == StreamType::BookTicker ? validBookTickerLine(line)
            : streamType == StreamType::Depth ? validDepthLine(line)
            : false;
        if (!ok) return false;
        lineStart = lineEnd + (lineEnd < input.size() ? 1u : 0u);
    }
    return true;
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
        auto result = internal::fail(Status::DependencyUnavailable, request, &pipeline, std::string{spec.dependencyError});
        metrics::recordRun(result);
        return result;
    }
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
    if (!validateStrictJsonl(input, streamType)) {
        auto result = internal::fail(Status::CorruptData, request, &pipeline, "input is not strict canonical jsonl for stream");
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
    fileHeader.codec = spec.codec;
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
        std::string encodeError;
        const auto encodeStatus = spec.compressPayload({input.data() + offset, plainSize}, request.zstdLevel, compressed, encodeError);
        if (!isOk(encodeStatus)) {
            auto failed = internal::fail(encodeStatus, request, &pipeline, encodeError.empty() ? "failed to compress block" : encodeError);
            metrics::recordRun(failed);
            return failed;
        }

        format::BlockHeader block{};
        block.uncompressedBytes = static_cast<std::uint32_t>(plainSize);
        block.compressedBytes = static_cast<std::uint32_t>(compressed.size());
        block.lineCount = static_cast<std::uint32_t>(internal::countLines({input.data() + offset, plainSize}, isLast));
        block.firstByteOffset = static_cast<std::uint64_t>(offset);
        format::setBlockChecksums(block,
                                  format::crc32c(compressed),
                                  format::crc32c({input.data() + offset, plainSize}));
        const auto blockHeader = format::serializeBlockHeader(block);
        out.write(reinterpret_cast<const char*>(blockHeader.data()), static_cast<std::streamsize>(blockHeader.size()));
        out.write(reinterpret_cast<const char*>(compressed.data()), static_cast<std::streamsize>(compressed.size()));
        result.outputBytes += format::kBlockHeaderBytes + static_cast<std::uint64_t>(compressed.size());
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
        }, spec);
        result.decodeCycles = timing::readCycles() - decodeStartCycles;
        result.decodeNs = timing::nowNs() - decodeStartNs;
        result.roundtripOk = isOk(decodeStatus) && decodedMatchesInput && decodedOffset == input.size();
    }
    result.status = result.roundtripOk ? Status::Ok : Status::DecodeError;
    if (!result.roundtripOk) result.error = "roundtrip check failed";

    (void)internal::writeTextFile(result.metricsPath, toMetricsJson(result));
    metrics::recordRun(result);
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
    if (in.gcount() != static_cast<std::streamsize>(sizeof(fileHeaderBytes))) return failArtifact(path, Status::CorruptData, "truncated artifact header");

    format::FileHeader header{};
    if (!format::parseFileHeader(fileHeaderBytes, sizeof(fileHeaderBytes), header) || !validHeader(header, spec)) {
        return failArtifact(path, Status::CorruptData, "invalid artifact header");
    }
    if (!validHeaderCrc(header)) return failArtifact(path, Status::CorruptData, "artifact header crc mismatch");

    std::uint64_t fileOffset = format::kFileHeaderBytes;
    std::uint64_t expectedPlainOffset = 0;
    std::vector<std::uint8_t> compressed;
    for (std::uint64_t blockIndex = 0; blockIndex < header.blockCount; ++blockIndex) {
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

    if (expectedPlainOffset != header.inputBytes) return failArtifact(path, Status::CorruptData, "decoded byte count mismatch");
    if (header.outputBytes != 0u && fileOffset != header.outputBytes) return failArtifact(path, Status::CorruptData, "output byte count mismatch");
    char extra = 0;
    if (in.read(&extra, 1)) return failArtifact(path, Status::CorruptData, "trailing bytes after blocks");

    ReplayArtifactInfo info{};
    info.status = Status::Ok;
    info.found = true;
    info.path = path;
    info.formatId = std::string{spec.formatId};
    info.pipelineId = std::string{pipeline.id};
    info.transform = std::string{pipeline.transform};
    info.entropy = std::string{pipeline.entropy};
    info.streamType = format::streamFromWire(header.stream);
    info.version = header.version;
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
    if (!format::parseFileHeader(compressedFile.data(), compressedFile.size(), fileHeader)) return Status::CorruptData;
    if (!validHeader(fileHeader, spec)) return Status::CorruptData;
    if (!validHeaderCrc(fileHeader)) return Status::CorruptData;
    if (fileHeader.outputBytes != 0u && fileHeader.outputBytes != compressedFile.size()) return Status::CorruptData;

    std::size_t offset = format::kFileHeaderBytes;
    std::vector<std::uint8_t> decoded;
    std::uint64_t expectedOffset = 0;
    for (std::uint64_t blockIndex = 0; blockIndex < fileHeader.blockCount; ++blockIndex) {
        if (compressedFile.size() - offset < format::kBlockHeaderBytes) return Status::CorruptData;
        format::BlockHeader block{};
        if (!format::parseBlockHeader(compressedFile.data() + offset, compressedFile.size() - offset, block)
            || !validBlock(block, fileHeader, expectedOffset)) {
            return Status::CorruptData;
        }
        offset += format::kBlockHeaderBytes;
        if (compressedFile.size() - offset < block.compressedBytes) return Status::CorruptData;
        bool shouldContinue = true;
        const auto status = decodeBlockPayload(fileHeader,
                                               block,
                                               {compressedFile.data() + offset, block.compressedBytes},
                                               decoded,
                                               onBlock,
                                               shouldContinue,
                                               spec);
        if (!isOk(status)) return status;
        offset += block.compressedBytes;
        expectedOffset += block.uncompressedBytes;
        if (!shouldContinue) return Status::Ok;
    }
    if (offset != compressedFile.size()) return Status::CorruptData;
    if (expectedOffset != fileHeader.inputBytes) return Status::CorruptData;
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
    if (!format::parseFileHeader(fileHeaderBytes, sizeof(fileHeaderBytes), fileHeader)) return Status::CorruptData;
    if (!validHeader(fileHeader, spec)) return Status::CorruptData;
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
        if (!format::parseBlockHeader(blockHeaderBytes, sizeof(blockHeaderBytes), block)
            || !validBlock(block, fileHeader, expectedOffset)) {
            return Status::CorruptData;
        }
        totalRead += format::kBlockHeaderBytes;

        compressed.resize(block.compressedBytes);
        in.read(reinterpret_cast<char*>(compressed.data()), static_cast<std::streamsize>(compressed.size()));
        if (in.gcount() != static_cast<std::streamsize>(compressed.size())) return Status::CorruptData;
        totalRead += block.compressedBytes;

        bool shouldContinue = true;
        const auto status = decodeBlockPayload(fileHeader, block, compressed, decoded, onBlock, shouldContinue, spec);
        if (!isOk(status)) return status;
        expectedOffset += block.uncompressedBytes;
        if (!shouldContinue) return Status::Ok;
    }

    if (fileHeader.outputBytes != 0u && totalRead != fileHeader.outputBytes) return Status::CorruptData;
    if (expectedOffset != fileHeader.inputBytes) return Status::CorruptData;
    char extra = 0;
    if (in.read(&extra, 1)) return Status::CorruptData;
    return Status::Ok;
}

}  // namespace hft_compressor::codecs::standard_jsonl_blocks



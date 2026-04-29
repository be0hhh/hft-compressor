#include "codecs/raw_jsonl_blocks/RawJsonlBlocks.hpp"

#include <algorithm>
#include <charconv>
#include <fstream>
#include <string_view>
#include <system_error>
#include <vector>

#include "common/CompressionInternals.hpp"
#include "common/timing.hpp"
#include "container/hfc/format.hpp"
#include "hft_compressor/metrics.hpp"

namespace hft_compressor::codecs::raw_jsonl_blocks {
namespace {

constexpr std::uint32_t kFileMagic = 0x31524648u;
constexpr std::uint32_t kBlockMagic = 0x30524648u;
constexpr std::uint16_t kVersion = 1u;
constexpr std::size_t kFileHeaderBytes = 64u;
constexpr std::size_t kBlockHeaderBytes = 32u;

struct FileHeader {
    std::uint32_t magic{kFileMagic};
    std::uint16_t version{kVersion};
    std::uint16_t stream{0};
    std::uint32_t blockBytes{0};
    std::uint64_t inputBytes{0};
    std::uint64_t outputBytes{0};
    std::uint64_t lineCount{0};
    std::uint64_t blockCount{0};
    std::uint32_t headerCrc32c{0};
};

struct BlockHeader {
    std::uint32_t magic{kBlockMagic};
    std::uint32_t uncompressedBytes{0};
    std::uint32_t lineCount{0};
    std::uint64_t firstByteOffset{0};
    std::uint32_t crc32c{0};
};

template <typename T>
void writeLe(std::vector<std::uint8_t>& out, T value) {
    for (std::size_t i = 0; i < sizeof(T); ++i) {
        out.push_back(static_cast<std::uint8_t>((static_cast<std::uint64_t>(value) >> (i * 8u)) & 0xffu));
    }
}

template <typename T>
bool readLe(const std::uint8_t*& p, const std::uint8_t* end, T& out) noexcept {
    if (static_cast<std::size_t>(end - p) < sizeof(T)) return false;
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < sizeof(T); ++i) value |= static_cast<std::uint64_t>(p[i]) << (i * 8u);
    p += sizeof(T);
    out = static_cast<T>(value);
    return true;
}

std::vector<std::uint8_t> serializeFileHeader(FileHeader header, bool includeCrc) {
    if (!includeCrc) header.headerCrc32c = 0u;
    std::vector<std::uint8_t> out;
    out.reserve(kFileHeaderBytes);
    writeLe(out, header.magic);
    writeLe(out, header.version);
    writeLe(out, header.stream);
    writeLe(out, header.blockBytes);
    writeLe(out, header.inputBytes);
    writeLe(out, header.outputBytes);
    writeLe(out, header.lineCount);
    writeLe(out, header.blockCount);
    writeLe(out, header.headerCrc32c);
    out.resize(kFileHeaderBytes, 0u);
    return out;
}

std::vector<std::uint8_t> serializeBlockHeader(const BlockHeader& header) {
    std::vector<std::uint8_t> out;
    out.reserve(kBlockHeaderBytes);
    writeLe(out, header.magic);
    writeLe(out, header.uncompressedBytes);
    writeLe(out, header.lineCount);
    writeLe<std::uint32_t>(out, 0u);
    writeLe(out, header.firstByteOffset);
    writeLe(out, header.crc32c);
    writeLe<std::uint32_t>(out, 0u);
    out.resize(kBlockHeaderBytes, 0u);
    return out;
}

bool parseFileHeader(const std::uint8_t* data, std::size_t size, FileHeader& out) noexcept {
    if (data == nullptr || size < kFileHeaderBytes) return false;
    const auto* p = data;
    const auto* end = data + kFileHeaderBytes;
    return readLe(p, end, out.magic)
        && readLe(p, end, out.version)
        && readLe(p, end, out.stream)
        && readLe(p, end, out.blockBytes)
        && readLe(p, end, out.inputBytes)
        && readLe(p, end, out.outputBytes)
        && readLe(p, end, out.lineCount)
        && readLe(p, end, out.blockCount)
        && readLe(p, end, out.headerCrc32c);
}

bool parseBlockHeader(const std::uint8_t* data, std::size_t size, BlockHeader& out) noexcept {
    if (data == nullptr || size < kBlockHeaderBytes) return false;
    const auto* p = data;
    const auto* end = data + kBlockHeaderBytes;
    std::uint32_t reserved = 0;
    std::uint32_t reserved2 = 0;
    return readLe(p, end, out.magic)
        && readLe(p, end, out.uncompressedBytes)
        && readLe(p, end, out.lineCount)
        && readLe(p, end, reserved)
        && readLe(p, end, out.firstByteOffset)
        && readLe(p, end, out.crc32c)
        && readLe(p, end, reserved2)
        && reserved == 0u
        && reserved2 == 0u;
}

std::uint32_t headerCrc32c(const FileHeader& header) {
    return format::crc32c(serializeFileHeader(header, false));
}

bool validHeader(const FileHeader& header) noexcept {
    return header.magic == kFileMagic
        && header.version == kVersion
        && header.blockBytes != 0u
        && format::streamFromWire(header.stream) != StreamType::Unknown;
}

bool validBlock(const BlockHeader& block, const FileHeader& header, std::uint64_t expectedOffset) noexcept {
    return block.magic == kBlockMagic
        && block.uncompressedBytes != 0u
        && block.uncompressedBytes <= header.blockBytes
        && block.firstByteOffset == expectedOffset;
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

Status readHeader(std::ifstream& in, FileHeader& header) noexcept {
    std::uint8_t bytes[kFileHeaderBytes]{};
    in.read(reinterpret_cast<char*>(bytes), static_cast<std::streamsize>(sizeof(bytes)));
    if (in.gcount() != static_cast<std::streamsize>(sizeof(bytes))) return Status::CorruptData;
    if (!parseFileHeader(bytes, sizeof(bytes), header) || !validHeader(header)) return Status::CorruptData;
    if (header.headerCrc32c != headerCrc32c(header)) return Status::CorruptData;
    return Status::Ok;
}

}  // namespace

CompressionResult compress(const CompressionRequest& request, const PipelineDescriptor& pipeline) noexcept {
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

    FileHeader fileHeader{};
    fileHeader.stream = format::streamToWire(streamType);
    fileHeader.blockBytes = blockBytes;
    fileHeader.inputBytes = result.inputBytes;
    const auto placeholderHeader = serializeFileHeader(fileHeader, true);
    out.write(reinterpret_cast<const char*>(placeholderHeader.data()), static_cast<std::streamsize>(placeholderHeader.size()));

    const auto encodeStartNs = timing::nowNs();
    const auto encodeStartCycles = timing::readCycles();
    for (std::size_t offset = 0; offset < input.size(); offset += blockBytes) {
        const auto plainSize = std::min<std::size_t>(input.size() - offset, blockBytes);
        const bool isLast = offset + plainSize >= input.size();
        BlockHeader block{};
        block.uncompressedBytes = static_cast<std::uint32_t>(plainSize);
        block.lineCount = static_cast<std::uint32_t>(internal::countLines({input.data() + offset, plainSize}, isLast));
        block.firstByteOffset = static_cast<std::uint64_t>(offset);
        block.crc32c = format::crc32c({input.data() + offset, plainSize});
        const auto blockHeader = serializeBlockHeader(block);
        out.write(reinterpret_cast<const char*>(blockHeader.data()), static_cast<std::streamsize>(blockHeader.size()));
        out.write(reinterpret_cast<const char*>(input.data() + offset), static_cast<std::streamsize>(plainSize));
        result.outputBytes += kBlockHeaderBytes + static_cast<std::uint64_t>(plainSize);
        result.lineCount += block.lineCount;
        ++result.blockCount;
    }
    result.outputBytes += kFileHeaderBytes;
    result.encodeCycles = timing::readCycles() - encodeStartCycles;
    result.encodeNs = timing::nowNs() - encodeStartNs;

    fileHeader.outputBytes = result.outputBytes;
    fileHeader.lineCount = result.lineCount;
    fileHeader.blockCount = result.blockCount;
    fileHeader.headerCrc32c = headerCrc32c(fileHeader);
    const auto finalHeader = serializeFileHeader(fileHeader, true);
    out.seekp(0, std::ios::beg);
    out.write(reinterpret_cast<const char*>(finalHeader.data()), static_cast<std::streamsize>(finalHeader.size()));
    out.close();
    if (!out) {
        auto failed = internal::fail(Status::IoError, request, &pipeline, "failed to write raw artifact");
        metrics::recordRun(failed);
        return failed;
    }

    std::size_t decodedOffset = 0;
    bool decodedMatchesInput = true;
    const auto decodeStartNs = timing::nowNs();
    const auto decodeStartCycles = timing::readCycles();
    const auto decodeStatus = decodeFile(outputPath, [&](std::span<const std::uint8_t> block) {
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
    result.status = result.roundtripOk ? Status::Ok : Status::DecodeError;
    if (!result.roundtripOk) result.error = "roundtrip check failed";

    (void)internal::writeTextFile(result.metricsPath, toMetricsJson(result));
    metrics::recordRun(result);
    return result;
}

ReplayArtifactInfo inspectArtifact(const std::filesystem::path& path, const PipelineDescriptor& pipeline) noexcept {
    if (path.empty()) return failArtifact(path, Status::InvalidArgument, "raw artifact path is empty");
    std::ifstream in(path, std::ios::binary);
    if (!in) return failArtifact(path, Status::IoError, "failed to open raw artifact");
    FileHeader header{};
    const auto headerStatus = readHeader(in, header);
    if (!isOk(headerStatus)) return failArtifact(path, headerStatus, "invalid raw artifact header");

    std::uint64_t expectedOffset = 0;
    std::uint64_t fileOffset = kFileHeaderBytes;
    for (std::uint64_t blockIndex = 0; blockIndex < header.blockCount; ++blockIndex) {
        std::uint8_t blockBytes[kBlockHeaderBytes]{};
        in.read(reinterpret_cast<char*>(blockBytes), static_cast<std::streamsize>(sizeof(blockBytes)));
        if (in.gcount() != static_cast<std::streamsize>(sizeof(blockBytes))) {
            return failArtifact(path, Status::CorruptData, "truncated raw block header");
        }
        BlockHeader block{};
        if (!parseBlockHeader(blockBytes, sizeof(blockBytes), block) || !validBlock(block, header, expectedOffset)) {
            return failArtifact(path, Status::CorruptData, "invalid raw block header");
        }
        fileOffset += kBlockHeaderBytes + block.uncompressedBytes;
        expectedOffset += block.uncompressedBytes;
        in.seekg(static_cast<std::streamoff>(block.uncompressedBytes), std::ios::cur);
        if (!in) return failArtifact(path, Status::CorruptData, "truncated raw payload");
    }
    if (expectedOffset != header.inputBytes) return failArtifact(path, Status::CorruptData, "raw decoded byte count mismatch");
    if (header.outputBytes != 0u && fileOffset != header.outputBytes) return failArtifact(path, Status::CorruptData, "raw output byte count mismatch");
    char extra = 0;
    if (in.read(&extra, 1)) return failArtifact(path, Status::CorruptData, "trailing bytes after raw blocks");

    ReplayArtifactInfo info{};
    info.status = Status::Ok;
    info.found = true;
    info.path = path;
    info.formatId = "hfr.raw_jsonl_blocks_v1";
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

Status decode(std::span<const std::uint8_t> file, const DecodedBlockCallback& onBlock) noexcept {
    if (file.size() < kFileHeaderBytes || !onBlock) return Status::InvalidArgument;
    FileHeader header{};
    if (!parseFileHeader(file.data(), file.size(), header) || !validHeader(header)) return Status::CorruptData;
    if (header.headerCrc32c != headerCrc32c(header)) return Status::CorruptData;
    if (header.outputBytes != 0u && header.outputBytes != file.size()) return Status::CorruptData;

    std::size_t offset = kFileHeaderBytes;
    std::uint64_t expectedOffset = 0;
    for (std::uint64_t blockIndex = 0; blockIndex < header.blockCount; ++blockIndex) {
        if (file.size() - offset < kBlockHeaderBytes) return Status::CorruptData;
        BlockHeader block{};
        if (!parseBlockHeader(file.data() + offset, file.size() - offset, block) || !validBlock(block, header, expectedOffset)) {
            return Status::CorruptData;
        }
        offset += kBlockHeaderBytes;
        if (file.size() - offset < block.uncompressedBytes) return Status::CorruptData;
        std::span<const std::uint8_t> payload{file.data() + offset, block.uncompressedBytes};
        if (format::crc32c(payload) != block.crc32c) return Status::CorruptData;
        if (!onBlock(payload)) return Status::Ok;
        offset += block.uncompressedBytes;
        expectedOffset += block.uncompressedBytes;
    }
    if (offset != file.size()) return Status::CorruptData;
    if (expectedOffset != header.inputBytes) return Status::CorruptData;
    return Status::Ok;
}

Status decodeFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept {
    if (path.empty() || !onBlock) return Status::InvalidArgument;
    std::ifstream in(path, std::ios::binary);
    if (!in) return Status::IoError;

    FileHeader header{};
    const auto headerStatus = readHeader(in, header);
    if (!isOk(headerStatus)) return headerStatus;

    std::vector<std::uint8_t> payload;
    std::uint64_t expectedOffset = 0;
    std::uint64_t totalRead = kFileHeaderBytes;
    for (std::uint64_t blockIndex = 0; blockIndex < header.blockCount; ++blockIndex) {
        std::uint8_t blockBytes[kBlockHeaderBytes]{};
        in.read(reinterpret_cast<char*>(blockBytes), static_cast<std::streamsize>(sizeof(blockBytes)));
        if (in.gcount() != static_cast<std::streamsize>(sizeof(blockBytes))) return Status::CorruptData;

        BlockHeader block{};
        if (!parseBlockHeader(blockBytes, sizeof(blockBytes), block) || !validBlock(block, header, expectedOffset)) {
            return Status::CorruptData;
        }
        totalRead += kBlockHeaderBytes;
        payload.resize(block.uncompressedBytes);
        in.read(reinterpret_cast<char*>(payload.data()), static_cast<std::streamsize>(payload.size()));
        if (in.gcount() != static_cast<std::streamsize>(payload.size())) return Status::CorruptData;
        totalRead += block.uncompressedBytes;
        if (format::crc32c(payload) != block.crc32c) return Status::CorruptData;
        expectedOffset += block.uncompressedBytes;
        if (!onBlock(payload)) return Status::Ok;
    }

    if (header.outputBytes != 0u && totalRead != header.outputBytes) return Status::CorruptData;
    if (expectedOffset != header.inputBytes) return Status::CorruptData;
    char extra = 0;
    if (in.read(&extra, 1)) return Status::CorruptData;
    return Status::Ok;
}

}  // namespace hft_compressor::codecs::raw_jsonl_blocks

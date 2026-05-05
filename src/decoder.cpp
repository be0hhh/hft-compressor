#include "hft_compressor/compressor.hpp"

#include <algorithm>
#include <charconv>
#include <fstream>
#include <sstream>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "common/CompressionInternals.hpp"
#include "common/timing.hpp"
#include "container/hfc/format.hpp"
#include "codecs/bookticker_delta_mask/BookTickerDeltaMask.hpp"
#include "codecs/depth_ladder_offset/DepthLadderOffset.hpp"
#include "codecs/depth_ladder_offset/DepthLadderOffsetV2.hpp"
#include "codecs/entropy_hftmac/EntropyHftMac.hpp"
#include "codecs/trades_grouped_delta_qtydict/TradesGroupedDeltaQtyDict.hpp"
#include "hft_compressor/replay_decode.hpp"
#include "pipelines/PipelineBackend.hpp"

namespace hft_compressor {
namespace {

constexpr std::string_view kCurrentReplayPipelineId{"std.zstd_jsonl_blocks_v1"};
constexpr std::uint64_t kFnvOffset = 14695981039346656037ull;
constexpr std::uint64_t kFnvPrime = 1099511628211ull;

bool wantsByteVerify(const DecodeVerifyRequest& request) noexcept {
    return request.verifyBytes
        && (request.verifyMode == VerifyMode::ByteExact || request.verifyMode == VerifyMode::Both);
}

bool wantsRecordVerify(const DecodeVerifyRequest& request) noexcept {
    return request.verifyMode == VerifyMode::RecordExact || request.verifyMode == VerifyMode::Both;
}

void hashBytes(std::uint64_t& hash, std::span<const std::uint8_t> bytes) noexcept {
    for (const auto byte : bytes) {
        hash ^= byte;
        hash *= kFnvPrime;
    }
}

void hashInt64(std::uint64_t& hash, std::int64_t value) noexcept {
    for (std::size_t i = 0; i < sizeof(value); ++i) {
        const auto byte = static_cast<std::uint8_t>((static_cast<std::uint64_t>(value) >> (i * 8u)) & 0xffu);
        hashBytes(hash, {&byte, 1u});
    }
}

void hashText(std::uint64_t& hash, std::string_view text) noexcept {
    hashBytes(hash, {reinterpret_cast<const std::uint8_t*>(text.data()), text.size()});
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

struct VerifyRecord {
    StreamType streamType{StreamType::Unknown};
    std::int64_t tsNs{0};
    std::int64_t values[8]{};
    std::uint32_t valueCount{0};
    std::uint64_t depthHash{kFnvOffset};
};

bool validSide(std::int64_t side) noexcept {
    return side == 0 || side == 1;
}

bool parseVerifyRecord(std::string_view line, StreamType streamType, VerifyRecord& out) noexcept {
    out = VerifyRecord{};
    out.streamType = streamType;
    JsonCursor p{line};
    if (streamType == StreamType::Trades) {
        return p.consume('[')
            && p.parseInt64(out.values[0]) && p.consume(',')
            && p.parseInt64(out.values[1]) && p.consume(',')
            && p.parseInt64(out.values[2]) && validSide(out.values[2]) && p.consume(',')
            && p.parseInt64(out.tsNs)
            && (out.valueCount = 3u, true)
            && p.consume(']') && p.finish();
    }
    if (streamType == StreamType::BookTicker) {
        return p.consume('[')
            && p.parseInt64(out.values[0]) && p.consume(',')
            && p.parseInt64(out.values[1]) && p.consume(',')
            && p.parseInt64(out.values[2]) && p.consume(',')
            && p.parseInt64(out.values[3]) && p.consume(',')
            && p.parseInt64(out.tsNs)
            && (out.valueCount = 4u, true)
            && p.consume(']') && p.finish();
    }
    if (streamType == StreamType::Depth) {
        if (!p.consume('[') || !p.peek('[')) return false;
        while (p.peek('[')) {
            std::int64_t price = 0;
            std::int64_t qty = 0;
            std::int64_t side = 0;
            if (!p.consume('[') || !p.parseInt64(price) || !p.consume(',')
                || !p.parseInt64(qty) || !p.consume(',')
                || !p.parseInt64(side) || !validSide(side) || !p.consume(']')
                || !p.consume(',')) {
                return false;
            }
            hashInt64(out.depthHash, price);
            hashInt64(out.depthHash, qty);
            hashInt64(out.depthHash, side);
            ++out.valueCount;
        }
        return p.parseInt64(out.tsNs) && p.consume(']') && p.finish();
    }
    return false;
}

void hashRecord(std::uint64_t& hash, const VerifyRecord& record) noexcept {
    hashText(hash, streamTypeToString(record.streamType));
    hashInt64(hash, record.tsNs);
    hashInt64(hash, static_cast<std::int64_t>(record.valueCount));
    if (record.streamType == StreamType::Depth) hashInt64(hash, static_cast<std::int64_t>(record.depthHash));
    for (std::uint32_t i = 0; i < record.valueCount && i < 8u; ++i) hashInt64(hash, record.values[i]);
}

std::string firstDifferingField(const VerifyRecord& expected, const VerifyRecord& actual) {
    if (expected.streamType != actual.streamType) return "stream";
    if (expected.tsNs != actual.tsNs) return "ts_ns";
    if (expected.valueCount != actual.valueCount) return "field_count";
    if (expected.streamType == StreamType::Depth && expected.depthHash != actual.depthHash) return "levels";
    const std::string_view namesTrades[] = {"price_e8", "qty_e8", "side"};
    const std::string_view namesBookTicker[] = {"bid_price_e8", "bid_qty_e8", "ask_price_e8", "ask_qty_e8"};
    for (std::uint32_t i = 0; i < expected.valueCount && i < 8u; ++i) {
        if (expected.values[i] == actual.values[i]) continue;
        if (expected.streamType == StreamType::Trades && i < 3u) return std::string{namesTrades[i]};
        if (expected.streamType == StreamType::BookTicker && i < 4u) return std::string{namesBookTicker[i]};
        return expected.streamType == StreamType::Depth ? "levels" : "field";
    }
    return {};
}

void recordFirstRecordMismatch(DecodeVerifyResult& result,
                               std::uint64_t lineNumber,
                               std::string field,
                               std::string_view stream) {
    if (result.firstMismatchLine != 0u) return;
    result.firstMismatchLine = lineNumber;
    if (result.firstMismatchField.empty() || result.firstMismatchField == "byte" || result.firstMismatchField == "length") {
        result.firstMismatchField = std::move(field);
    }
    result.firstMismatchStream = std::string{stream};
}

void updateReplaySpan(DecodeVerifyResult& result, std::int64_t tsNs, bool& sawTs, std::int64_t& firstTs, std::int64_t& lastTs) noexcept {
    if (!sawTs) {
        sawTs = true;
        firstTs = tsNs;
        lastTs = tsNs;
    } else {
        firstTs = std::min(firstTs, tsNs);
        lastTs = std::max(lastTs, tsNs);
    }
    result.replayTimeSpanNs = lastTs > firstTs ? static_cast<std::uint64_t>(lastTs - firstTs) : 0u;
}

std::string previewBytes(std::span<const std::uint8_t> data, std::size_t offset) {
    constexpr std::size_t kPreviewBytes = 32;
    if (offset >= data.size()) return {};
    const auto len = std::min<std::size_t>(kPreviewBytes, data.size() - offset);
    std::string out;
    out.reserve(len);
    for (std::size_t i = 0; i < len; ++i) {
        const auto c = static_cast<char>(data[offset + i]);
        out.push_back((c >= 32 && c <= 126 && c != '"' && c != '\\') ? c : '.');
    }
    return out;
}

DecodeVerifyResult failVerify(Status status, const DecodeVerifyRequest& request, std::string error) {
    DecodeVerifyResult result{};
    result.status = status;
    result.error = std::move(error);
    result.pipelineId = request.pipelineId;
    result.compressedPath = request.compressedPath;
    result.canonicalPath = request.canonicalPath;
    result.metricsPath = request.compressedPath.empty()
        ? std::filesystem::path{}
        : request.compressedPath.parent_path() / (request.compressedPath.stem().string() + ".verify.json");
    if (const auto* pipeline = findPipeline(request.pipelineId); pipeline != nullptr) {
        result.profile = std::string{pipeline->profile};
    }
    return result;
}

HfcFileInfo failOpen(const std::filesystem::path& path, Status status, std::string error) {
    HfcFileInfo info{};
    info.status = status;
    info.error = std::move(error);
    info.path = path;
    return info;
}

ReplayArtifactInfo missingArtifact() {
    ReplayArtifactInfo info{};
    info.status = Status::Ok;
    info.found = false;
    return info;
}

ReplayArtifactInfo failArtifact(Status status, std::string error) {
    ReplayArtifactInfo info{};
    info.status = status;
    info.found = false;
    info.error = std::move(error);
    return info;
}

std::string sessionIdForReplayRequest(const ReplayArtifactRequest& request) {
    if (!request.sessionId.empty()) return request.sessionId;
    if (!request.sessionDir.empty()) return request.sessionDir.filename().string();
    return {};
}

void addUniquePath(std::vector<std::filesystem::path>& paths, const std::filesystem::path& path) {
    if (path.empty()) return;
    if (std::find(paths.begin(), paths.end(), path) == paths.end()) paths.push_back(path);
}

void addRootCandidates(std::vector<std::filesystem::path>& paths,
                       const std::filesystem::path& root,
                       std::string_view outputSlug,
                       std::string_view extension,
                       const std::string& sessionId,
                       std::string_view channel) {
    if (root.empty() || sessionId.empty() || channel.empty()) return;
    const std::string fileName = std::string{channel} + (extension.empty() ? ".hfc" : std::string{extension});
    addUniquePath(paths, root / std::string{outputSlug} / "sessions" / sessionId / fileName);
    addUniquePath(paths, root / "sessions" / sessionId / fileName);
    addUniquePath(paths, root / sessionId / fileName);
    addUniquePath(paths, root / fileName);
}

std::vector<std::filesystem::path> replayArtifactCandidates(const ReplayArtifactRequest& request,
                                                            const PipelineDescriptor& pipeline) {
    std::vector<std::filesystem::path> paths;
    const auto sessionId = sessionIdForReplayRequest(request);
    const auto channel = streamTypeChannelName(request.streamType);
    addRootCandidates(paths, request.compressedRoot, pipeline.outputSlug, pipeline.fileExtension, sessionId, channel);
    if (!request.sessionDir.empty() && !channel.empty()) {
        const std::string extension = pipeline.fileExtension.empty() ? ".hfc" : std::string{pipeline.fileExtension};
        addUniquePath(paths, request.sessionDir / (std::string{channel} + extension));
    }

    for (std::filesystem::path cursor = request.sessionDir; !cursor.empty();) {
        addRootCandidates(paths, cursor / "compressedData", pipeline.outputSlug, pipeline.fileExtension, sessionId, channel);
        addRootCandidates(paths, cursor / "hft-compressor" / "compressedData", pipeline.outputSlug, pipeline.fileExtension, sessionId, channel);
        addRootCandidates(paths, cursor / "apps" / "hft-compressor" / "compressedData", pipeline.outputSlug, pipeline.fileExtension, sessionId, channel);
        const auto parent = cursor.parent_path();
        if (parent == cursor) break;
        cursor = parent;
    }
    return paths;
}

}  // namespace

std::string toVerifyMetricsJson(const DecodeVerifyResult& result) {
    const char q = char(34);
    std::ostringstream out;
    out << '{' << '\n';
    out << "  " << q << "status" << q << ": " << q << statusToString(result.status) << q << ',' << '\n';
    out << "  " << q << "pipeline_id" << q << ": " << q << result.pipelineId << q << ',' << '\n';
    out << "  " << q << "profile" << q << ": " << q << result.profile << q << ',' << '\n';
    out << "  " << q << "stream" << q << ": " << q << streamTypeToString(result.streamType) << q << ',' << '\n';
    out << "  " << q << "compressed_bytes" << q << ": " << result.compressedBytes << ',' << '\n';
    out << "  " << q << "decoded_bytes" << q << ": " << result.decodedBytes << ',' << '\n';
    out << "  " << q << "canonical_bytes" << q << ": " << result.canonicalBytes << ',' << '\n';
    out << "  " << q << "compared_bytes" << q << ": " << result.comparedBytes << ',' << '\n';
    out << "  " << q << "mismatch_bytes" << q << ": " << result.mismatchBytes << ',' << '\n';
    out << "  " << q << "mismatch_percent" << q << ": " << result.mismatchPercent << ',' << '\n';
    out << "  " << q << "byte_exact" << q << ": " << (result.byteExact ? "true" : "false") << ',' << '\n';
    out << "  " << q << "record_exact" << q << ": " << (result.recordExact ? "true" : "false") << ',' << '\n';
    out << "  " << q << "canonical_record_count" << q << ": " << result.canonicalRecordCount << ',' << '\n';
    out << "  " << q << "decoded_record_count" << q << ": " << result.decodedRecordCount << ',' << '\n';
    out << "  " << q << "canonical_byte_hash" << q << ": " << result.canonicalByteHash << ',' << '\n';
    out << "  " << q << "decoded_byte_hash" << q << ": " << result.decodedByteHash << ',' << '\n';
    out << "  " << q << "canonical_record_hash" << q << ": " << result.canonicalRecordHash << ',' << '\n';
    out << "  " << q << "decoded_record_hash" << q << ": " << result.decodedRecordHash << ',' << '\n';
    out << "  " << q << "replay_time_span_ns" << q << ": " << result.replayTimeSpanNs << ',' << '\n';
    out << "  " << q << "estimated_replay_multiplier" << q << ": " << result.estimatedReplayMultiplier << ',' << '\n';
    out << "  " << q << "line_count" << q << ": " << result.lineCount << ',' << '\n';
    out << "  " << q << "block_count" << q << ": " << result.blockCount << ',' << '\n';
    out << "  " << q << "decode_ns" << q << ": " << result.decodeNs << ',' << '\n';
    out << "  " << q << "decode_cycles" << q << ": " << result.decodeCycles << ',' << '\n';
    out << "  " << q << "decode_mb_per_sec" << q << ": " << decodeMbPerSec(result) << ',' << '\n';
    out << "  " << q << "verified" << q << ": " << (result.verified ? "true" : "false") << ',' << '\n';
    out << "  " << q << "first_mismatch_offset" << q << ": " << result.firstMismatchOffset << ',' << '\n';
    out << "  " << q << "first_mismatch_line" << q << ": " << result.firstMismatchLine << ',' << '\n';
    out << "  " << q << "first_mismatch_field" << q << ": " << q << result.firstMismatchField << q << ',' << '\n';
    out << "  " << q << "first_mismatch_stream" << q << ": " << q << result.firstMismatchStream << q << ',' << '\n';
    out << "  " << q << "first_mismatch_preview_canonical" << q << ": " << q << result.firstMismatchPreviewCanonical << q << ',' << '\n';
    out << "  " << q << "first_mismatch_preview_decoded" << q << ": " << q << result.firstMismatchPreviewDecoded << q << '\n';
    out << '}' << '\n';
    return out.str();
}

HfcFileInfo openHfcFile(const std::filesystem::path& path) noexcept {
    if (path.empty()) return failOpen(path, Status::InvalidArgument, "hfc path is empty");
    std::ifstream in(path, std::ios::binary);
    if (!in) return failOpen(path, Status::IoError, "failed to open hfc file");

    std::uint8_t fileHeaderBytes[format::kFileHeaderBytes]{};
    in.read(reinterpret_cast<char*>(fileHeaderBytes), static_cast<std::streamsize>(sizeof(fileHeaderBytes)));
    if (in.gcount() != static_cast<std::streamsize>(sizeof(fileHeaderBytes))) {
        return failOpen(path, Status::CorruptData, "truncated hfc header");
    }

    format::FileHeader header{};
    if (!format::parseFileHeader(fileHeaderBytes, sizeof(fileHeaderBytes), header)
        || header.magic != format::kFileMagic
        || !format::isSupportedVersion(header.version)
        || header.codec != format::kCodecZstdJsonlBlocksV1
        || header.blockBytes == 0u) {
        return failOpen(path, Status::CorruptData, "invalid hfc header");
    }
    if (header.version >= format::kVersion2
        && format::storedHeaderCrc32c(header) != format::headerCrc32c(header)) {
        return failOpen(path, Status::CorruptData, "hfc header crc mismatch");
    }

    HfcFileInfo info{};
    info.status = Status::Ok;
    info.path = path;
    info.streamType = format::streamFromWire(header.stream);
    info.version = header.version;
    info.codec = header.codec;
    info.blockBytes = header.blockBytes;
    info.inputBytes = header.inputBytes;
    info.outputBytes = header.outputBytes;
    info.lineCount = header.lineCount;
    info.blockCount = header.blockCount;
    info.blocks.reserve(static_cast<std::size_t>(std::min<std::uint64_t>(header.blockCount, 1024u * 1024u)));

    std::uint64_t fileOffset = format::kFileHeaderBytes;
    std::uint64_t expectedPlainOffset = 0;
    std::vector<std::uint8_t> compressed;
    for (std::uint64_t blockIndex = 0; blockIndex < header.blockCount; ++blockIndex) {
        std::uint8_t blockHeaderBytes[format::kBlockHeaderBytes]{};
        in.read(reinterpret_cast<char*>(blockHeaderBytes), static_cast<std::streamsize>(sizeof(blockHeaderBytes)));
        if (in.gcount() != static_cast<std::streamsize>(sizeof(blockHeaderBytes))) {
            return failOpen(path, Status::CorruptData, "truncated hfc block header");
        }

        format::BlockHeader block{};
        if (!format::parseBlockHeader(blockHeaderBytes, sizeof(blockHeaderBytes), block)
            || block.magic != format::kBlockMagic
            || block.uncompressedBytes == 0u
            || block.compressedBytes == 0u
            || block.uncompressedBytes > header.blockBytes
            || block.firstByteOffset != expectedPlainOffset) {
            return failOpen(path, Status::CorruptData, "invalid hfc block header");
        }

        compressed.resize(block.compressedBytes);
        in.read(reinterpret_cast<char*>(compressed.data()), static_cast<std::streamsize>(compressed.size()));
        if (in.gcount() != static_cast<std::streamsize>(compressed.size())) {
            return failOpen(path, Status::CorruptData, "truncated hfc payload");
        }
        if (header.version >= format::kVersion2 && format::crc32c(compressed) != format::compressedCrc32c(block)) {
            return failOpen(path, Status::CorruptData, "hfc compressed crc mismatch");
        }

        info.blocks.push_back(HfcBlockInfo{
            fileOffset,
            block.uncompressedBytes,
            block.compressedBytes,
            block.lineCount,
            block.firstByteOffset,
            header.version >= format::kVersion2 ? format::compressedCrc32c(block) : 0u,
            header.version >= format::kVersion2 ? format::uncompressedCrc32c(block) : 0u,
        });

        fileOffset += format::kBlockHeaderBytes + block.compressedBytes;
        expectedPlainOffset += block.uncompressedBytes;
    }

    if (expectedPlainOffset != header.inputBytes) {
        return failOpen(path, Status::CorruptData, "hfc decoded byte count mismatch");
    }
    if (header.outputBytes != 0u && fileOffset != header.outputBytes) {
        return failOpen(path, Status::CorruptData, "hfc output byte count mismatch");
    }
    char extra = 0;
    if (in.read(&extra, 1)) return failOpen(path, Status::CorruptData, "trailing bytes after hfc blocks");
    return info;
}

ReplayArtifactInfo discoverReplayArtifact(const ReplayArtifactRequest& request) noexcept {
    if (request.streamType == StreamType::Unknown) {
        return failArtifact(Status::InvalidArgument, "replay artifact stream is unknown");
    }

    const std::string pipelineId = request.preferredPipelineId.empty()
        ? std::string{kCurrentReplayPipelineId}
        : request.preferredPipelineId;
    const auto* pipeline = findPipeline(pipelineId);
    if (pipeline == nullptr) return failArtifact(Status::UnsupportedPipeline, "unknown replay artifact pipeline");
    const auto* backend = pipelines::findBackend(pipeline->id);
    if (backend == nullptr || backend->inspectArtifact == nullptr || backend->decodeJsonl == nullptr) {
        return failArtifact(Status::NotImplemented, "replay artifact pipeline is not implemented by the public decoder API");
    }

    const auto paths = replayArtifactCandidates(request, *pipeline);
    for (const auto& path : paths) {
        std::error_code ec;
        if (!std::filesystem::is_regular_file(path, ec) || ec) continue;
        auto artifact = backend->inspectArtifact(path, *pipeline);
        if (!isOk(artifact.status)) {
            return failArtifact(artifact.status, artifact.error.empty() ? "failed to open replay artifact" : artifact.error);
        }
        if (artifact.streamType != request.streamType) {
            return failArtifact(Status::CorruptData, "replay artifact stream does not match requested channel");
        }
        return artifact;
    }

    return missingArtifact();
}

Status decodeReplayArtifactJsonl(const ReplayArtifactInfo& artifact,
                                 const DecodedBlockCallback& onBlock) noexcept {
    if (!artifact.found || artifact.path.empty() || !onBlock) return Status::InvalidArgument;
    const auto* backend = pipelines::findBackend(artifact.pipelineId);
    if (backend == nullptr || backend->decodeJsonl == nullptr || backend->formatId != artifact.formatId) {
        return Status::NotImplemented;
    }
    return backend->decodeJsonl(artifact.path, onBlock);
}

Status decodeReplayJsonl(const ReplayArtifactRequest& request,
                         const DecodedBlockCallback& onBlock) noexcept {
    if (!onBlock) return Status::InvalidArgument;
    const auto artifact = discoverReplayArtifact(request);
    if (!isOk(artifact.status)) return artifact.status;
    if (!artifact.found) return Status::IoError;
    return decodeReplayArtifactJsonl(artifact, onBlock);
}

Status inspectCompressedArtifact(const std::filesystem::path& path,
                                 std::string_view pipelineId,
                                 std::string_view view,
                                 const DecodedBlockCallback& onBlock) noexcept {
    if (path.empty() || pipelineId.empty() || view.empty() || !onBlock) return Status::InvalidArgument;
    if (pipelineId.find("_ac16_") != std::string_view::npos
        || pipelineId.find("_ac32_") != std::string_view::npos
        || pipelineId.find("_range_byte_") != std::string_view::npos
        || pipelineId.find("_rans_byte_") != std::string_view::npos) {
        if (view == "canonical-json" || view == "canonical-jsonl") return codecs::entropy_hftmac::decodeFile(path, onBlock);
        if (view == "encoded-json") return codecs::entropy_hftmac::inspectEncodedJsonFile(path, onBlock);
        if (view == "encoded-binary") return codecs::entropy_hftmac::inspectEncodedBinaryFile(path, onBlock);
        if (view == "stats") return codecs::entropy_hftmac::inspectStatsJsonFile(path, onBlock);
    }
    if (pipelineId == "hftmac.trades_grouped_delta_qtydict_math_v3" || pipelineId == "hftmac.trades_grouped_delta_qtydict_v1") {
        if (view == "canonical-json" || view == "canonical-jsonl") {
            return codecs::trades_grouped_delta_qtydict::decodeFile(path, onBlock);
        }
        if (view == "encoded-json") {
            return codecs::trades_grouped_delta_qtydict::inspectEncodedJsonFile(path, onBlock);
        }
        if (view == "encoded-binary") {
            return codecs::trades_grouped_delta_qtydict::inspectEncodedBinaryFile(path, onBlock);
        }
        if (view == "stats") {
            return codecs::trades_grouped_delta_qtydict::inspectStatsJsonFile(path, onBlock);
        }
    }
    if (pipelineId == "hftmac.bookticker_delta_mask_v1" || pipelineId == "hftmac.bookticker_delta_mask_v2") {
        if (view == "canonical-json" || view == "canonical-jsonl") return codecs::bookticker_delta_mask::decodeFile(path, onBlock);
        if (view == "encoded-json") return codecs::bookticker_delta_mask::inspectEncodedJsonFile(path, onBlock);
        if (view == "encoded-binary") return codecs::bookticker_delta_mask::inspectEncodedBinaryFile(path, onBlock);
        if (view == "stats") return codecs::bookticker_delta_mask::inspectStatsJsonFile(path, onBlock);
    }
    if (pipelineId == "hftmac.depth_ladder_offset_v1") {
        if (view == "canonical-json" || view == "canonical-jsonl") return codecs::depth_ladder_offset::decodeFile(path, onBlock);
        if (view == "encoded-json") return codecs::depth_ladder_offset::inspectEncodedJsonFile(path, onBlock);
        if (view == "encoded-binary") return codecs::depth_ladder_offset::inspectEncodedBinaryFile(path, onBlock);
        if (view == "stats") return codecs::depth_ladder_offset::inspectStatsJsonFile(path, onBlock);
    }
    if (pipelineId == "hftmac.depth_ladder_offset_v2" || pipelineId == "hftmac.depth_ladder_offset_v3") {
        if (view == "canonical-json" || view == "canonical-jsonl") return codecs::depth_ladder_offset_v2::decodeFile(path, onBlock);
        if (view == "encoded-json") return codecs::depth_ladder_offset_v2::inspectEncodedJsonFile(path, onBlock);
        if (view == "encoded-binary") return codecs::depth_ladder_offset_v2::inspectEncodedBinaryFile(path, onBlock);
        if (view == "stats") return codecs::depth_ladder_offset_v2::inspectStatsJsonFile(path, onBlock);
    }
    return Status::NotImplemented;
}

Status decodeReplayRecords(const ReplayArtifactRequest& request,
                           const DecodedRecordCallback& onRecord) noexcept {
    if (!onRecord) return Status::InvalidArgument;
    ReplayDecodeRequest decodeRequest{};
    decodeRequest.artifact = request;
    return decodeReplayRecordBatches(decodeRequest, [&](const ReplayRecordBatchV1& batch) noexcept -> bool {
        for (const auto& row : batch.trades) {
            ReplayRecord record{};
            record.kind = ReplayRecordKind::Trade;
            record.trade.tsNs = row.tsNs;
            record.trade.priceE8 = row.priceE8;
            record.trade.qtyE8 = row.qtyE8;
            record.trade.side = row.side;
            if (!onRecord(record)) return false;
        }
        for (const auto& row : batch.bookTickers) {
            ReplayRecord record{};
            record.kind = ReplayRecordKind::BookTicker;
            record.bookTicker.tsNs = row.tsNs;
            record.bookTicker.bidPriceE8 = row.bidPriceE8;
            record.bookTicker.bidQtyE8 = row.bidQtyE8;
            record.bookTicker.askPriceE8 = row.askPriceE8;
            record.bookTicker.askQtyE8 = row.askQtyE8;
            if (!onRecord(record)) return false;
        }
        for (const auto& row : batch.depths) {
            ReplayRecord record{};
            record.kind = ReplayRecordKind::Depth;
            record.depth.tsNs = row.tsNs;
            record.depth.levels.reserve(row.levelCount);
            for (std::uint32_t i = 0; i < row.levelCount; ++i) {
                const auto& level = batch.depthLevels[row.firstLevelIndex + i];
                record.depth.levels.push_back(ReplayDepthLevel{level.priceE8, level.qtyE8, level.side});
            }
            if (!onRecord(record)) return false;
        }
        return true;
    });
}

DecodeVerifyResult decodeAndVerify(const DecodeVerifyRequest& request) noexcept {
    if (request.compressedPath.empty()) {
        return failVerify(Status::InvalidArgument, request, "compressed path is empty");
    }
    const bool verifyByteExact = wantsByteVerify(request);
    const bool verifyRecordExact = wantsRecordVerify(request);
    const bool needsCanonical = verifyByteExact || verifyRecordExact;
    if (needsCanonical && request.canonicalPath.empty()) {
        return failVerify(Status::InvalidArgument, request, "canonical path is empty");
    }
    std::error_code ec;
    if (!std::filesystem::exists(request.compressedPath, ec)) {
        return failVerify(Status::IoError, request, "compressed file does not exist");
    }
    ec.clear();
    if (needsCanonical && !std::filesystem::exists(request.canonicalPath, ec)) {
        return failVerify(Status::IoError, request, "canonical file does not exist");
    }

    DecodeVerifyResult result{};
    result.pipelineId = request.pipelineId;
    if (const auto* pipeline = findPipeline(request.pipelineId); pipeline != nullptr) {
        result.profile = std::string{pipeline->profile};
    }
    result.compressedPath = request.compressedPath;
    result.canonicalPath = request.canonicalPath;
    result.metricsPath = request.compressedPath.parent_path() / (request.compressedPath.stem().string() + ".verify.json");
    const auto* pipeline = findPipeline(request.pipelineId);
    if (pipeline == nullptr) {
        result.status = Status::UnsupportedPipeline;
        result.error = "unknown pipeline id";
        return result;
    }
    const auto* backend = pipelines::findBackend(pipeline->id);
    if (backend == nullptr || backend->inspectArtifact == nullptr || backend->decodeJsonl == nullptr) {
        result.status = Status::NotImplemented;
        result.error = "pipeline has no decoder implementation";
        return result;
    }
    const auto artifact = backend->inspectArtifact(request.compressedPath, *pipeline);
    if (!isOk(artifact.status)) {
        result.status = artifact.status;
        result.error = artifact.error.empty() ? "failed to parse compressed artifact" : artifact.error;
        return result;
    }
    result.pipelineId = artifact.pipelineId;
    result.compressedBytes = artifact.outputBytes;
    result.streamType = artifact.streamType;
    result.lineCount = artifact.lineCount;
    result.blockCount = artifact.blockCount;

    std::ifstream canonicalBytes;
    std::ifstream canonicalRecords;
    std::vector<std::uint8_t> canonicalBlock;
    bool byteMatches = true;
    bool recordMatches = true;
    bool firstMismatchRecorded = false;
    if (needsCanonical) {
        ec.clear();
        result.canonicalBytes = static_cast<std::uint64_t>(std::filesystem::file_size(request.canonicalPath, ec));
        if (ec) {
            result.status = Status::IoError;
            result.error = "failed to stat canonical file";
            return result;
        }
        if (verifyByteExact) canonicalBytes.open(request.canonicalPath, std::ios::binary);
        if (verifyByteExact && !canonicalBytes) {
            result.status = Status::IoError;
            result.error = "failed to open canonical file for byte verification";
            return result;
        }
        if (verifyRecordExact) canonicalRecords.open(request.canonicalPath, std::ios::binary);
        if (verifyRecordExact && !canonicalRecords) {
            result.status = Status::IoError;
            result.error = "failed to open canonical file for record verification";
            return result;
        }
    }

    result.canonicalByteHash = kFnvOffset;
    result.decodedByteHash = kFnvOffset;
    result.canonicalRecordHash = kFnvOffset;
    result.decodedRecordHash = kFnvOffset;

    std::string decodedCarry;
    std::string canonicalLine;
    std::uint64_t decodedLineNumber = 0;
    bool sawReplayTs = false;
    std::int64_t firstReplayTs = 0;
    std::int64_t lastReplayTs = 0;

    auto noteByteMismatch = [&](std::uint64_t offset,
                                std::span<const std::uint8_t> canonicalPreview,
                                std::span<const std::uint8_t> decodedPreview,
                                std::size_t previewOffset) {
        byteMatches = false;
        if (firstMismatchRecorded) return;
        firstMismatchRecorded = true;
        result.firstMismatchOffset = offset;
        if (result.firstMismatchLine == 0u) {
            result.firstMismatchField = "byte";
            result.firstMismatchStream = std::string{streamTypeToString(result.streamType)};
        }
        result.firstMismatchPreviewCanonical = previewBytes(canonicalPreview, previewOffset);
        result.firstMismatchPreviewDecoded = previewBytes(decodedPreview, previewOffset);
    };

    auto compareRecordLine = [&](std::string_view decodedLine) {
        if (!verifyRecordExact || decodedLine.empty()) return;
        ++decodedLineNumber;
        VerifyRecord decodedRecord{};
        VerifyRecord canonicalRecord{};
        const bool decodedOk = parseVerifyRecord(decodedLine, result.streamType, decodedRecord);
        bool canonicalAvailable = static_cast<bool>(std::getline(canonicalRecords, canonicalLine));
        if (canonicalAvailable && !canonicalLine.empty() && canonicalLine.back() == '\r') canonicalLine.pop_back();
        const bool canonicalOk = canonicalAvailable && parseVerifyRecord(canonicalLine, result.streamType, canonicalRecord);

        if (canonicalOk) {
            ++result.canonicalRecordCount;
            hashRecord(result.canonicalRecordHash, canonicalRecord);
        }
        if (decodedOk) {
            ++result.decodedRecordCount;
            hashRecord(result.decodedRecordHash, decodedRecord);
            updateReplaySpan(result, decodedRecord.tsNs, sawReplayTs, firstReplayTs, lastReplayTs);
        }

        if (!canonicalAvailable) {
            recordMatches = false;
            recordFirstRecordMismatch(result, decodedLineNumber, "extra_decoded_record", streamTypeToString(result.streamType));
        } else if (!canonicalOk) {
            recordMatches = false;
            recordFirstRecordMismatch(result, decodedLineNumber, "canonical_parse", streamTypeToString(result.streamType));
        } else if (!decodedOk) {
            recordMatches = false;
            recordFirstRecordMismatch(result, decodedLineNumber, "decoded_parse", streamTypeToString(result.streamType));
        } else {
            const auto field = firstDifferingField(canonicalRecord, decodedRecord);
            if (!field.empty()) {
                recordMatches = false;
                recordFirstRecordMismatch(result, decodedLineNumber, field, streamTypeToString(result.streamType));
            }
        }
    };

    auto processDecodedRecords = [&](std::span<const std::uint8_t> block) {
        if (!verifyRecordExact) return;
        decodedCarry.append(reinterpret_cast<const char*>(block.data()), block.size());
        std::size_t start = 0;
        for (;;) {
            const std::size_t newline = decodedCarry.find('\n', start);
            if (newline == std::string::npos) break;
            std::string_view line{decodedCarry.data() + start, newline - start};
            if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
            compareRecordLine(line);
            start = newline + 1u;
        }
        if (start != 0u) decodedCarry.erase(0, start);
    };

    const auto decodeStartNs = timing::nowNs();
    const auto decodeStartCycles = timing::readCycles();
    const auto decodeStatus = backend->decodeJsonl(request.compressedPath, [&](std::span<const std::uint8_t> block) {
        const auto blockStart = result.decodedBytes;
        result.decodedBytes += static_cast<std::uint64_t>(block.size());
        hashBytes(result.decodedByteHash, block);
        processDecodedRecords(block);
        if (!verifyByteExact) return true;

        canonicalBlock.resize(block.size());
        canonicalBytes.read(reinterpret_cast<char*>(canonicalBlock.data()), static_cast<std::streamsize>(canonicalBlock.size()));
        const auto readBytes = static_cast<std::size_t>(canonicalBytes.gcount());
        hashBytes(result.canonicalByteHash, {canonicalBlock.data(), readBytes});
        const auto compared = std::min<std::size_t>(readBytes, block.size());
        result.comparedBytes += static_cast<std::uint64_t>(compared);
        for (std::size_t i = 0; i < compared; ++i) {
            if (block[i] == canonicalBlock[i]) continue;
            ++result.mismatchBytes;
            if (!firstMismatchRecorded) {
                noteByteMismatch(blockStart + static_cast<std::uint64_t>(i), canonicalBlock, block, i);
            }
        }
        if (readBytes != block.size()) {
            if (!firstMismatchRecorded) {
                noteByteMismatch(blockStart + static_cast<std::uint64_t>(compared),
                                 {canonicalBlock.data(), readBytes},
                                 block,
                                 compared < readBytes ? compared : 0u);
            }
            byteMatches = false;
        }
        if (result.mismatchBytes > 0u) byteMatches = false;
        return true;
    });
    if (verifyByteExact && result.decodedBytes < result.canonicalBytes) {
        std::vector<std::uint8_t> tail(64u * 1024u);
        while (canonicalBytes) {
            canonicalBytes.read(reinterpret_cast<char*>(tail.data()), static_cast<std::streamsize>(tail.size()));
            const auto readBytes = static_cast<std::size_t>(canonicalBytes.gcount());
            if (readBytes == 0u) break;
            hashBytes(result.canonicalByteHash, {tail.data(), readBytes});
        }
    }

    if (verifyRecordExact) {
        if (!decodedCarry.empty()) {
            if (!decodedCarry.empty() && decodedCarry.back() == '\r') decodedCarry.pop_back();
            compareRecordLine(decodedCarry);
        }
        while (std::getline(canonicalRecords, canonicalLine)) {
            if (!canonicalLine.empty() && canonicalLine.back() == '\r') canonicalLine.pop_back();
            if (canonicalLine.empty()) continue;
            VerifyRecord canonicalRecord{};
            if (parseVerifyRecord(canonicalLine, result.streamType, canonicalRecord)) {
                ++result.canonicalRecordCount;
                hashRecord(result.canonicalRecordHash, canonicalRecord);
            }
            recordMatches = false;
            recordFirstRecordMismatch(result,
                                      result.decodedRecordCount + 1u,
                                      "missing_decoded_record",
                                      streamTypeToString(result.streamType));
        }
    }

    if (verifyByteExact && result.decodedBytes != result.canonicalBytes) {
        const auto tailMismatch = result.decodedBytes > result.canonicalBytes
            ? result.decodedBytes - result.canonicalBytes
            : result.canonicalBytes - result.decodedBytes;
        result.mismatchBytes += tailMismatch;
        if (!firstMismatchRecorded) {
            firstMismatchRecorded = true;
            result.firstMismatchOffset = std::min(result.decodedBytes, result.canonicalBytes);
            if (result.firstMismatchLine == 0u) {
                result.firstMismatchField = "length";
                result.firstMismatchStream = std::string{streamTypeToString(result.streamType)};
            }
        }
        byteMatches = false;
    }
    result.decodeCycles = timing::readCycles() - decodeStartCycles;
    result.decodeNs = timing::nowNs() - decodeStartNs;
    const auto denominator = std::max(result.decodedBytes, result.canonicalBytes);
    result.mismatchPercent = denominator == 0u ? 0.0 : (static_cast<double>(result.mismatchBytes) * 100.0) / static_cast<double>(denominator);
    result.byteExact = verifyByteExact && byteMatches && result.decodedBytes == result.canonicalBytes
        && result.decodedByteHash == result.canonicalByteHash;
    result.recordExact = verifyRecordExact && recordMatches
        && result.decodedRecordCount == result.canonicalRecordCount
        && result.decodedRecordHash == result.canonicalRecordHash;
    result.estimatedReplayMultiplier = result.decodeNs == 0u
        ? 0.0
        : static_cast<double>(result.replayTimeSpanNs) / static_cast<double>(result.decodeNs);

    if (!isOk(decodeStatus)) {
        result.status = decodeStatus;
        result.error = "decode failed";
    } else if ((verifyByteExact && !result.byteExact) || (verifyRecordExact && !result.recordExact)) {
        result.status = Status::VerificationFailed;
        result.error = (verifyRecordExact && !result.recordExact)
            ? "decoded records differ from canonical jsonl"
            : "decoded bytes differ from canonical jsonl";
    } else {
        result.status = Status::Ok;
        result.verified = verifyByteExact || verifyRecordExact;
    }

    (void)internal::writeTextFile(result.metricsPath, toVerifyMetricsJson(result));
    return result;
}

}  // namespace hft_compressor






#include "hft_compressor/compressor.hpp"

#include <algorithm>
#include <fstream>
#include <sstream>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "common/CompressionInternals.hpp"
#include "common/timing.hpp"
#include "container/hfc/format.hpp"

namespace hft_compressor {
namespace {

constexpr std::string_view kCurrentReplayFormatId{"hfc.zstd_jsonl_blocks_v1"};
constexpr std::string_view kCurrentReplayPipelineId{"std.zstd_jsonl_blocks_v1"};

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
                       const std::string& sessionId,
                       std::string_view channel) {
    if (root.empty() || sessionId.empty() || channel.empty()) return;
    const std::string fileName = std::string{channel} + ".hfc";
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
    addRootCandidates(paths, request.compressedRoot, pipeline.outputSlug, sessionId, channel);
    if (!request.sessionDir.empty() && !channel.empty()) {
        addUniquePath(paths, request.sessionDir / (std::string{channel} + ".hfc"));
    }

    for (std::filesystem::path cursor = request.sessionDir; !cursor.empty();) {
        addRootCandidates(paths, cursor / "compressedData", pipeline.outputSlug, sessionId, channel);
        addRootCandidates(paths, cursor / "hft-compressor" / "compressedData", pipeline.outputSlug, sessionId, channel);
        addRootCandidates(paths, cursor / "apps" / "hft-compressor" / "compressedData", pipeline.outputSlug, sessionId, channel);
        const auto parent = cursor.parent_path();
        if (parent == cursor) break;
        cursor = parent;
    }
    return paths;
}

ReplayArtifactInfo replayArtifactFromHfc(const std::filesystem::path& path,
                                         const HfcFileInfo& hfcInfo,
                                         const PipelineDescriptor& pipeline) {
    ReplayArtifactInfo info{};
    info.status = Status::Ok;
    info.found = true;
    info.path = path;
    info.formatId = std::string{kCurrentReplayFormatId};
    info.pipelineId = std::string{pipeline.id};
    info.transform = std::string{pipeline.transform};
    info.entropy = std::string{pipeline.entropy};
    info.streamType = hfcInfo.streamType;
    info.version = hfcInfo.version;
    info.inputBytes = hfcInfo.inputBytes;
    info.outputBytes = hfcInfo.outputBytes;
    info.lineCount = hfcInfo.lineCount;
    info.blockCount = hfcInfo.blockCount;
    return info;
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
    out << "  " << q << "line_count" << q << ": " << result.lineCount << ',' << '\n';
    out << "  " << q << "block_count" << q << ": " << result.blockCount << ',' << '\n';
    out << "  " << q << "decode_ns" << q << ": " << result.decodeNs << ',' << '\n';
    out << "  " << q << "decode_cycles" << q << ": " << result.decodeCycles << ',' << '\n';
    out << "  " << q << "decode_mb_per_sec" << q << ": " << decodeMbPerSec(result) << ',' << '\n';
    out << "  " << q << "verified" << q << ": " << (result.verified ? "true" : "false") << ',' << '\n';
    out << "  " << q << "first_mismatch_offset" << q << ": " << result.firstMismatchOffset << ',' << '\n';
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
        in.seekg(static_cast<std::streamoff>(block.compressedBytes), std::ios::cur);
        if (!in) return failOpen(path, Status::CorruptData, "truncated hfc payload");
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
    if (pipeline->id != kCurrentReplayPipelineId) {
        return failArtifact(Status::NotImplemented, "replay artifact pipeline is not implemented by the public decoder API");
    }

    const auto paths = replayArtifactCandidates(request, *pipeline);
    for (const auto& path : paths) {
        std::error_code ec;
        if (!std::filesystem::is_regular_file(path, ec) || ec) continue;
        const auto hfcInfo = openHfcFile(path);
        if (!isOk(hfcInfo.status)) {
            return failArtifact(hfcInfo.status, hfcInfo.error.empty() ? "failed to open replay artifact" : hfcInfo.error);
        }
        if (hfcInfo.streamType != request.streamType) {
            return failArtifact(Status::CorruptData, "replay artifact stream does not match requested channel");
        }
        return replayArtifactFromHfc(path, hfcInfo, *pipeline);
    }

    return missingArtifact();
}

Status decodeReplayArtifactJsonl(const ReplayArtifactInfo& artifact,
                                 const DecodedBlockCallback& onBlock) noexcept {
    if (!artifact.found || artifact.path.empty() || !onBlock) return Status::InvalidArgument;
    if (std::string_view{artifact.pipelineId} != kCurrentReplayPipelineId
        || std::string_view{artifact.formatId} != kCurrentReplayFormatId) {
        return Status::NotImplemented;
    }
    return decodeHfcFile(artifact.path, onBlock);
}

Status decodeReplayJsonl(const ReplayArtifactRequest& request,
                         const DecodedBlockCallback& onBlock) noexcept {
    if (!onBlock) return Status::InvalidArgument;
    const auto artifact = discoverReplayArtifact(request);
    if (!isOk(artifact.status)) return artifact.status;
    if (!artifact.found) return Status::IoError;
    return decodeReplayArtifactJsonl(artifact, onBlock);
}

Status decodeReplayRecords(const ReplayArtifactRequest& request,
                           const DecodedRecordCallback& onRecord) noexcept {
    if (!onRecord) return Status::InvalidArgument;
    const auto artifact = discoverReplayArtifact(request);
    if (!isOk(artifact.status)) return artifact.status;
    if (!artifact.found) return Status::IoError;
    return Status::NotImplemented;
}

DecodeVerifyResult decodeAndVerify(const DecodeVerifyRequest& request) noexcept {
    if (request.compressedPath.empty()) {
        return failVerify(Status::InvalidArgument, request, "compressed path is empty");
    }
    if (request.verifyBytes && request.canonicalPath.empty()) {
        return failVerify(Status::InvalidArgument, request, "canonical path is empty");
    }
    std::error_code ec;
    if (!std::filesystem::exists(request.compressedPath, ec)) {
        return failVerify(Status::IoError, request, "compressed file does not exist");
    }
    ec.clear();
    if (request.verifyBytes && !std::filesystem::exists(request.canonicalPath, ec)) {
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
    const auto hfcInfo = openHfcFile(request.compressedPath);
    if (!isOk(hfcInfo.status)) {
        result.status = hfcInfo.status;
        result.error = hfcInfo.error.empty() ? "failed to parse hfc header" : hfcInfo.error;
        return result;
    }
    result.compressedBytes = hfcInfo.outputBytes;
    result.streamType = hfcInfo.streamType;
    result.lineCount = hfcInfo.lineCount;
    result.blockCount = hfcInfo.blockCount;

    std::ifstream canonical;
    std::vector<std::uint8_t> canonicalBlock;
    bool matches = true;
    bool firstMismatchRecorded = false;
    if (request.verifyBytes) {
        ec.clear();
        result.canonicalBytes = static_cast<std::uint64_t>(std::filesystem::file_size(request.canonicalPath, ec));
        if (ec) {
            result.status = Status::IoError;
            result.error = "failed to stat canonical file";
            return result;
        }
        canonical.open(request.canonicalPath, std::ios::binary);
        if (!canonical) {
            result.status = Status::IoError;
            result.error = "failed to open canonical file";
            return result;
        }
    }

    const auto decodeStartNs = timing::nowNs();
    const auto decodeStartCycles = timing::readCycles();
    const auto decodeStatus = decodeHfcFile(request.compressedPath, [&](std::span<const std::uint8_t> block) {
        const auto blockStart = result.decodedBytes;
        result.decodedBytes += static_cast<std::uint64_t>(block.size());
        if (!request.verifyBytes) return true;

        canonicalBlock.resize(block.size());
        canonical.read(reinterpret_cast<char*>(canonicalBlock.data()), static_cast<std::streamsize>(canonicalBlock.size()));
        const auto readBytes = static_cast<std::size_t>(canonical.gcount());
        const auto compared = std::min<std::size_t>(readBytes, block.size());
        result.comparedBytes += static_cast<std::uint64_t>(compared);
        for (std::size_t i = 0; i < compared; ++i) {
            if (block[i] == canonicalBlock[i]) continue;
            ++result.mismatchBytes;
            if (!firstMismatchRecorded) {
                firstMismatchRecorded = true;
                matches = false;
                result.firstMismatchOffset = blockStart + static_cast<std::uint64_t>(i);
                result.firstMismatchPreviewCanonical = previewBytes(canonicalBlock, i);
                result.firstMismatchPreviewDecoded = previewBytes(block, i);
            }
        }
        if (readBytes != block.size()) {
            result.mismatchBytes += static_cast<std::uint64_t>(block.size() > readBytes ? block.size() - readBytes : readBytes - block.size());
            matches = false;
            if (!firstMismatchRecorded) {
                firstMismatchRecorded = true;
                result.firstMismatchOffset = blockStart + static_cast<std::uint64_t>(compared);
                result.firstMismatchPreviewCanonical = previewBytes({canonicalBlock.data(), readBytes}, compared < readBytes ? compared : 0u);
                result.firstMismatchPreviewDecoded = previewBytes(block, compared < block.size() ? compared : 0u);
            }
        }
        if (result.mismatchBytes > 0u) matches = false;
        return true;
    });
    result.decodeCycles = timing::readCycles() - decodeStartCycles;
    result.decodeNs = timing::nowNs() - decodeStartNs;

    if (request.verifyBytes && result.decodedBytes != result.canonicalBytes) {
        const auto tailMismatch = result.decodedBytes > result.canonicalBytes
            ? result.decodedBytes - result.canonicalBytes
            : result.canonicalBytes - result.decodedBytes;
        result.mismatchBytes += tailMismatch;
        if (!firstMismatchRecorded) result.firstMismatchOffset = std::min(result.decodedBytes, result.canonicalBytes);
        matches = false;
    }
    const auto denominator = std::max(result.decodedBytes, result.canonicalBytes);
    result.mismatchPercent = denominator == 0u ? 0.0 : (static_cast<double>(result.mismatchBytes) * 100.0) / static_cast<double>(denominator);

    if (!isOk(decodeStatus)) {
        result.status = decodeStatus;
        result.error = "decode failed";
    } else if (request.verifyBytes && !matches) {
        result.status = Status::VerificationFailed;
        result.error = "decoded bytes differ from canonical jsonl";
    } else {
        result.status = Status::Ok;
        result.verified = request.verifyBytes;
    }

    (void)internal::writeTextFile(result.metricsPath, toVerifyMetricsJson(result));
    return result;
}

}  // namespace hft_compressor

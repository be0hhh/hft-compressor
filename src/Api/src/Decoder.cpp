#include "hft_compressor/Compressor.hpp"

#include <algorithm>
#include <charconv>
#include <fstream>
#include <sstream>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "../../Runtime/src/Common/CompressionInternals.hpp"
#include "../../Runtime/src/Common/Timing.hpp"
#include "../../Runtime/src/Container/Hfc/Format.hpp"
#include "../../Runtime/src/Codecs/BooktickerDeltaMask/BookTickerDeltaMask.hpp"
#include "../../Runtime/src/Codecs/DepthLadderOffset/DepthLadderOffset.hpp"
#include "../../Runtime/src/Codecs/EntropyHftmac/EntropyHftMac.hpp"
#include "../../Runtime/src/Codecs/TradesGroupedDeltaQtydict/TradesGroupedDeltaQtyDict.hpp"
#include "hft_compressor/ReplayDecode.hpp"
#include "../../Runtime/src/Pipelines/PipelineBackend.hpp"

namespace hft_compressor {
namespace {


constexpr std::string_view kCurrentReplayPipelineId{"std.zstd_jsonl_blocks_v1"};

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

HfcFileInfo openHfcFile(const std::filesystem::path& path) noexcept {
    try {
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
        || header.codec != format::kCodecZstdJsonlBlocks
        || header.blockBytes == 0u) {
        return failOpen(path, Status::CorruptData, "invalid hfc header");
    }
    if (format::storedHeaderCrc32c(header) != format::headerCrc32c(header)) {
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
        if (format::crc32c(compressed) != format::compressedCrc32c(block)) {
            return failOpen(path, Status::CorruptData, "hfc compressed crc mismatch");
        }

        info.blocks.push_back(HfcBlockInfo{
            fileOffset,
            block.uncompressedBytes,
            block.compressedBytes,
            block.lineCount,
            block.firstByteOffset,
            format::compressedCrc32c(block),
            format::uncompressedCrc32c(block),
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
    } catch (...) { HfcFileInfo failed{}; failed.status = Status::DecodeError; return failed; }
}

ReplayArtifactInfo discoverReplayArtifact(const ReplayArtifactRequest& request) noexcept {
    try {
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
    } catch (...) { ReplayArtifactInfo failed{}; failed.status = Status::DecodeError; return failed; }
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
    if (pipelineId == "hftmac.trades_grouped_delta_qtydict_math_v3") {
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
    if (pipelineId == "hftmac.bookticker_delta_mask_v2") {
        if (view == "canonical-json" || view == "canonical-jsonl") return codecs::bookticker_delta_mask::decodeFile(path, onBlock);
        if (view == "encoded-json") return codecs::bookticker_delta_mask::inspectEncodedJsonFile(path, onBlock);
        if (view == "encoded-binary") return codecs::bookticker_delta_mask::inspectEncodedBinaryFile(path, onBlock);
        if (view == "stats") return codecs::bookticker_delta_mask::inspectStatsJsonFile(path, onBlock);
    }
    if (pipelineId == "hftmac.depth_ladder_offset_v3") {
        if (view == "canonical-json" || view == "canonical-jsonl") return codecs::depth_ladder_offset::decodeFile(path, onBlock);
        if (view == "encoded-json") return codecs::depth_ladder_offset::inspectEncodedJsonFile(path, onBlock);
        if (view == "encoded-binary") return codecs::depth_ladder_offset::inspectEncodedBinaryFile(path, onBlock);
        if (view == "stats") return codecs::depth_ladder_offset::inspectStatsJsonFile(path, onBlock);
    }
    return Status::NotImplemented;
}

Status decodeReplayRecords(const ReplayArtifactRequest& request,
                           const DecodedRecordCallback& onRecord) noexcept {
    try {
    if (!onRecord) return Status::InvalidArgument;
    ReplayDecodeRequest decodeRequest{};
    decodeRequest.artifact = request;
    return decodeReplayRecordBatches(decodeRequest, [&](const ReplayRecordBatch& batch) -> bool {
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
    } catch (...) { return Status::DecodeError; }
}


}  // namespace hft_compressor

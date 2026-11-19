#include "hft_compressor/ReplayDecode.hpp"
#include "hft_compressor/CorpusLab.hpp"

#include <algorithm>
#include <bit>
#include <vector>

#include "../../Runtime/src/Codecs/ByteBlock/BinaryTransform.hpp"

namespace hft_compressor {
namespace {

Status flushBatch(ReplayRecordBatch& batch, const ReplayRecordBatchCallback& onBatch) {
    if (batch.recordCount() == 0u) return Status::Ok;
    if (!onBatch(batch)) return Status::CallbackStopped;
    batch.clearRows();
    return Status::Ok;
}

bool appendBinaryRecordToBatch(const ByteBlockLayout& layout,
                               std::span<const std::uint8_t> rec,
                               ReplayRecordBatch& batch) {
    const std::int64_t tsNs = (layout.timestampOffset + 8u <= layout.recordBytes)
        ? std::bit_cast<std::int64_t>(codecs::byte_block::word(rec, layout.timestampOffset, 8u))
        : 0;
    if (layout.streamType == StreamType::Trades) {
        const auto price = std::bit_cast<std::int64_t>(codecs::byte_block::word(rec, layout.payloadOffset, 8u));
        const auto qty = std::bit_cast<std::int64_t>(codecs::byte_block::word(rec, layout.payloadOffset + 8u, 8u));
        const auto side = static_cast<std::int64_t>(rec[layout.payloadOffset + 16u]);
        batch.trades.push_back(ReplayBatchTradeRecord{tsNs, price, qty, side});
        return true;
    }
    if (layout.streamType == StreamType::BookTicker) {
        const auto bid = std::bit_cast<std::int64_t>(codecs::byte_block::word(rec, layout.payloadOffset, 8u));
        const auto bidQty = std::bit_cast<std::int64_t>(codecs::byte_block::word(rec, layout.payloadOffset + 8u, 8u));
        const auto ask = std::bit_cast<std::int64_t>(codecs::byte_block::word(rec, layout.payloadOffset + 16u, 8u));
        const auto askQty = std::bit_cast<std::int64_t>(codecs::byte_block::word(rec, layout.payloadOffset + 24u, 8u));
        batch.bookTickers.push_back(ReplayBatchBookTickerRecord{tsNs, bid, bidQty, ask, askQty});
        return true;
    }
    if (layout.streamType == StreamType::Depth) {
        const auto levelCount = codecs::byte_block::word(rec, layout.depthLevelCountOffset, 2u);
        if (levelCount > layout.depthLevelCapacity) return false;
        ReplayBatchDepthRecord row{};
        row.tsNs = tsNs;
        row.firstLevelIndex = static_cast<std::uint32_t>(batch.depthLevels.size());
        row.levelCount = static_cast<std::uint32_t>(levelCount);
        for (std::size_t i = 0u; i < levelCount; ++i) {
            const std::size_t off = layout.depthLevelsOffset + i * layout.depthLevelStride;
            const auto price = std::bit_cast<std::int64_t>(codecs::byte_block::word(rec, off, 8u));
            const auto qty = std::bit_cast<std::int64_t>(codecs::byte_block::word(rec, off + 8u, 8u));
            const auto side = static_cast<std::int64_t>(rec[off + 16u]);
            batch.depthLevels.push_back(ReplayDepthLevel{price, qty, side});
        }
        batch.depths.push_back(row);
        return true;
    }
    return false;
}

}  // namespace

void ReplayRecordBatch::clearRows() noexcept {
    firstLineNumber = 0u;
    lineCount = 0u;
    decodedBytes = 0u;
    trades.clear();
    bookTickers.clear();
    depths.clear();
    depthLevels.clear();
}

std::size_t ReplayRecordBatch::recordCount() const noexcept {
    return trades.size() + bookTickers.size() + depths.size();
}

Status decodeReplayArtifactRecordBatches(const ReplayArtifactInfo& artifact,
                                         std::size_t maxRecordsPerBatch,
                                         const ReplayRecordBatchCallback& onBatch) noexcept {
    try {
        if (!artifact.found || artifact.streamType == StreamType::Unknown || !onBatch) {
            return Status::InvalidArgument;
        }
        maxRecordsPerBatch = std::max<std::size_t>(maxRecordsPerBatch, 1u);

        std::vector<std::uint8_t> sliceBytes;
        const auto decodeStatus = decodeReplayArtifactJsonl(artifact, [&](std::span<const std::uint8_t> block) -> bool {
            sliceBytes.insert(sliceBytes.end(), block.begin(), block.end());
            return true;
        });
        if (!isOk(decodeStatus)) return decodeStatus;

        BinarySliceView slice{};
        const auto parseSt = parseBinarySlice(sliceBytes, slice);
        if (!isOk(parseSt)) return parseSt;
        if (slice.layout.streamType != artifact.streamType) return Status::CorruptData;

        ReplayRecordBatch batch{};
        batch.streamType = artifact.streamType;
        const std::size_t stride = slice.layout.recordBytes;

        for (std::uint32_t i = 0u; i < slice.recordCount; ++i) {
            if (batch.recordCount() == 0u) batch.firstLineNumber = i + 1u;
            const auto rec = slice.records.subspan(static_cast<std::size_t>(i) * stride, stride);
            if (!appendBinaryRecordToBatch(slice.layout, rec, batch)) return Status::CorruptData;
            ++batch.lineCount;
            batch.decodedBytes += stride;
            if (batch.recordCount() >= maxRecordsPerBatch) {
                const auto st = flushBatch(batch, onBatch);
                if (!isOk(st)) return st;
            }
        }
        return flushBatch(batch, onBatch);
    } catch (...) {
        return Status::DecodeError;
    }
}

Status decodeReplayRecordBatches(const ReplayDecodeRequest& request,
                                 const ReplayRecordBatchCallback& onBatch) noexcept {
    try {
        if (!onBatch) return Status::InvalidArgument;
        const auto artifact = discoverReplayArtifact(request.artifact);
        if (!isOk(artifact.status)) return artifact.status;
        if (!artifact.found) return Status::IoError;
        return decodeReplayArtifactRecordBatches(artifact, request.maxRecordsPerBatch, onBatch);
    } catch (...) {
        return Status::DecodeError;
    }
}

}  // namespace hft_compressor

#include "hft_compressor/replay_decode.hpp"

#include <algorithm>
#include <charconv>
#include <string>
#include <string_view>
#include <system_error>

namespace hft_compressor {
namespace {

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

bool parseTradeLine(std::string_view line, ReplayTradeRecordV1& out) noexcept {
    JsonCursor p{line};
    return p.consume('[')
        && p.parseInt64(out.priceE8) && p.consume(',')
        && p.parseInt64(out.qtyE8) && p.consume(',')
        && p.parseInt64(out.side) && validSide(out.side) && p.consume(',')
        && p.parseInt64(out.tsNs)
        && p.consume(']') && p.finish();
}

bool parseBookTickerLine(std::string_view line, ReplayBookTickerRecordV1& out) noexcept {
    JsonCursor p{line};
    return p.consume('[')
        && p.parseInt64(out.bidPriceE8) && p.consume(',')
        && p.parseInt64(out.bidQtyE8) && p.consume(',')
        && p.parseInt64(out.askPriceE8) && p.consume(',')
        && p.parseInt64(out.askQtyE8) && p.consume(',')
        && p.parseInt64(out.tsNs)
        && p.consume(']') && p.finish();
}

bool parseDepthLevel(JsonCursor& p, ReplayDepthLevelV1& out) noexcept {
    return p.consume('[')
        && p.parseInt64(out.priceE8) && p.consume(',')
        && p.parseInt64(out.qtyE8) && p.consume(',')
        && p.parseInt64(out.side) && validSide(out.side)
        && p.consume(']');
}

bool parseDepthLine(std::string_view line, ReplayRecordBatchV1& batch) noexcept {
    JsonCursor p{line};
    if (!p.consume('[') || !p.peek('[')) return false;
    ReplayDepthRecordV1 row{};
    row.firstLevelIndex = static_cast<std::uint32_t>(batch.depthLevels.size());
    while (p.peek('[')) {
        ReplayDepthLevelV1 level{};
        if (!parseDepthLevel(p, level) || !p.consume(',')) return false;
        batch.depthLevels.push_back(level);
        ++row.levelCount;
    }
    if (!p.parseInt64(row.tsNs) || !p.consume(']') || !p.finish()) return false;
    batch.depths.push_back(row);
    return true;
}

bool parseLine(StreamType streamType, std::string_view line, ReplayRecordBatchV1& batch) noexcept {
    if (streamType == StreamType::Trades) {
        ReplayTradeRecordV1 row{};
        if (!parseTradeLine(line, row)) return false;
        batch.trades.push_back(row);
        return true;
    }
    if (streamType == StreamType::BookTicker) {
        ReplayBookTickerRecordV1 row{};
        if (!parseBookTickerLine(line, row)) return false;
        batch.bookTickers.push_back(row);
        return true;
    }
    if (streamType == StreamType::Depth) return parseDepthLine(line, batch);
    return false;
}

Status flushBatch(ReplayRecordBatchV1& batch, const ReplayRecordBatchCallback& onBatch) noexcept {
    if (batch.recordCount() == 0u) return Status::Ok;
    if (!onBatch(batch)) return Status::CallbackStopped;
    batch.clearRows();
    return Status::Ok;
}

Status processJsonlBlock(StreamType streamType,
                         std::span<const std::uint8_t> block,
                         std::string& carry,
                         std::uint64_t& lineNumber,
                         ReplayRecordBatchV1& batch,
                         std::size_t maxRecordsPerBatch,
                         const ReplayRecordBatchCallback& onBatch) noexcept {
    carry.append(reinterpret_cast<const char*>(block.data()), block.size());
    std::size_t start = 0;
    for (;;) {
        const std::size_t newline = carry.find('\n', start);
        if (newline == std::string::npos) break;
        std::string_view line{carry.data() + start, newline - start};
        ++lineNumber;
        if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
        if (!line.empty()) {
            if (batch.recordCount() == 0u) batch.firstLineNumber = lineNumber;
            if (!parseLine(streamType, line, batch)) return Status::CorruptData;
            ++batch.lineCount;
            batch.decodedBytes += static_cast<std::uint64_t>(newline - start + 1u);
            if (batch.recordCount() >= maxRecordsPerBatch) {
                const auto status = flushBatch(batch, onBatch);
                if (!isOk(status)) return status;
            }
        }
        start = newline + 1u;
    }
    if (start != 0u) carry.erase(0, start);
    return Status::Ok;
}

}  // namespace

void ReplayRecordBatchV1::clearRows() noexcept {
    firstLineNumber = 0;
    lineCount = 0;
    decodedBytes = 0;
    trades.clear();
    bookTickers.clear();
    depths.clear();
    depthLevels.clear();
}

std::size_t ReplayRecordBatchV1::recordCount() const noexcept {
    return trades.size() + bookTickers.size() + depths.size();
}

Status decodeReplayArtifactRecordBatches(const ReplayArtifactInfo& artifact,
                                         std::size_t maxRecordsPerBatch,
                                         const ReplayRecordBatchCallback& onBatch) noexcept {
    if (!artifact.found || artifact.streamType == StreamType::Unknown || !onBatch) return Status::InvalidArgument;
    maxRecordsPerBatch = std::max<std::size_t>(maxRecordsPerBatch, 1u);
    ReplayRecordBatchV1 batch{};
    batch.streamType = artifact.streamType;
    std::string carry;
    std::uint64_t lineNumber = 0;
    Status streamStatus = Status::Ok;
    const auto decodeStatus = decodeReplayArtifactJsonl(artifact, [&](std::span<const std::uint8_t> block) noexcept -> bool {
        streamStatus = processJsonlBlock(artifact.streamType,
                                         block,
                                         carry,
                                         lineNumber,
                                         batch,
                                         maxRecordsPerBatch,
                                         onBatch);
        return isOk(streamStatus);
    });
    if (!isOk(streamStatus)) return streamStatus;
    if (!isOk(decodeStatus)) return decodeStatus;
    if (!carry.empty()) {
        ++lineNumber;
        if (!carry.empty() && carry.back() == '\r') carry.pop_back();
        if (!carry.empty()) {
            if (batch.recordCount() == 0u) batch.firstLineNumber = lineNumber;
            if (!parseLine(artifact.streamType, carry, batch)) return Status::CorruptData;
            ++batch.lineCount;
            batch.decodedBytes += static_cast<std::uint64_t>(carry.size());
        }
    }
    return flushBatch(batch, onBatch);
}

Status decodeReplayRecordBatches(const ReplayDecodeRequest& request,
                                 const ReplayRecordBatchCallback& onBatch) noexcept {
    if (!onBatch) return Status::InvalidArgument;
    const auto artifact = discoverReplayArtifact(request.artifact);
    if (!isOk(artifact.status)) return artifact.status;
    if (!artifact.found) return Status::IoError;
    return decodeReplayArtifactRecordBatches(artifact, request.maxRecordsPerBatch, onBatch);
}

}  // namespace hft_compressor

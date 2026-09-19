#pragma once

#include <cstddef>
#include <cstdint>
#include <functional>
#include <vector>

#include "hft_compressor/Api.hpp"
#include "hft_compressor/Compressor.hpp"
#include "hft_compressor/Status.hpp"
#include "hft_compressor/StreamType.hpp"

namespace hft_compressor {

enum class ReplayDecodeOutput : std::uint8_t {
    JsonlChunks = 1u,
    RecordBatches = 2u,
};

struct ReplayDecodeRequest {
    ReplayArtifactRequest artifact{};
    std::size_t maxRecordsPerBatch{4096u};
};

struct ReplayBatchTradeRecord {
    std::int64_t tsNs{0};
    std::int64_t priceE8{0};
    std::int64_t qtyE8{0};
    std::int64_t side{0};
};

struct ReplayBatchBookTickerRecord {
    std::int64_t tsNs{0};
    std::int64_t bidPriceE8{0};
    std::int64_t bidQtyE8{0};
    std::int64_t askPriceE8{0};
    std::int64_t askQtyE8{0};
};

struct ReplayBatchDepthRecord {
    std::int64_t tsNs{0};
    std::uint32_t firstLevelIndex{0};
    std::uint32_t levelCount{0};
};

struct ReplayRecordBatch {
    StreamType streamType{StreamType::Unknown};
    std::uint64_t firstLineNumber{0};
    std::uint64_t lineCount{0};
    std::uint64_t decodedBytes{0};
    std::vector<ReplayBatchTradeRecord> trades{};
    std::vector<ReplayBatchBookTickerRecord> bookTickers{};
    std::vector<ReplayBatchDepthRecord> depths{};
    std::vector<ReplayDepthLevel> depthLevels{};

    void clearRows() noexcept;
    std::size_t recordCount() const noexcept;
};

using ReplayRecordBatchCallback = std::function<bool(const ReplayRecordBatch& batch)>;

HFT_COMPRESSOR_API Status decodeReplayRecordBatches(const ReplayDecodeRequest& request,
                                                    const ReplayRecordBatchCallback& onBatch) noexcept;
HFT_COMPRESSOR_API Status decodeReplayArtifactRecordBatches(const ReplayArtifactInfo& artifact,
                                                            std::size_t maxRecordsPerBatch,
                                                            const ReplayRecordBatchCallback& onBatch) noexcept;

}  // namespace hft_compressor

#include "hft_compressor/c_api.h"

#include <new>
#include <vector>

#include "hft_compressor/replay_decode.hpp"

struct hftc_decoder {
    hft_compressor::ReplayDecodeRequest request{};
};

namespace {

hftc_status toCStatus(hft_compressor::Status status) noexcept {
    switch (status) {
        case hft_compressor::Status::Ok: return HFTC_STATUS_OK;
        case hft_compressor::Status::InvalidArgument: return HFTC_STATUS_INVALID_ARGUMENT;
        case hft_compressor::Status::IoError: return HFTC_STATUS_IO_ERROR;
        case hft_compressor::Status::UnsupportedStream: return HFTC_STATUS_UNSUPPORTED_STREAM;
        case hft_compressor::Status::UnsupportedPipeline: return HFTC_STATUS_UNSUPPORTED_PIPELINE;
        case hft_compressor::Status::DependencyUnavailable: return HFTC_STATUS_DEPENDENCY_UNAVAILABLE;
        case hft_compressor::Status::NotImplemented: return HFTC_STATUS_NOT_IMPLEMENTED;
        case hft_compressor::Status::CorruptData: return HFTC_STATUS_CORRUPT_DATA;
        case hft_compressor::Status::DecodeError: return HFTC_STATUS_DECODE_ERROR;
        case hft_compressor::Status::VerificationFailed: return HFTC_STATUS_VERIFICATION_FAILED;
        case hft_compressor::Status::CallbackStopped: return HFTC_STATUS_CALLBACK_STOPPED;
    }
    return HFTC_STATUS_CORRUPT_DATA;
}

hft_compressor::StreamType toStreamType(uint32_t value) noexcept {
    switch (value) {
        case HFTC_STREAM_TRADES: return hft_compressor::StreamType::Trades;
        case HFTC_STREAM_BOOKTICKER: return hft_compressor::StreamType::BookTicker;
        case HFTC_STREAM_DEPTH: return hft_compressor::StreamType::Depth;
        default: return hft_compressor::StreamType::Unknown;
    }
}

hft_compressor::ArtifactPreference toPreference(uint32_t value) noexcept {
    switch (value) {
        case HFTC_ARTIFACT_REPLAY: return hft_compressor::ArtifactPreference::Replay;
        case HFTC_ARTIFACT_ARCHIVE: return hft_compressor::ArtifactPreference::Archive;
        case HFTC_ARTIFACT_LIVE: return hft_compressor::ArtifactPreference::Live;
        default: return hft_compressor::ArtifactPreference::CurrentBaseline;
    }
}

uint32_t toCStreamType(hft_compressor::StreamType value) noexcept {
    switch (value) {
        case hft_compressor::StreamType::Trades: return HFTC_STREAM_TRADES;
        case hft_compressor::StreamType::BookTicker: return HFTC_STREAM_BOOKTICKER;
        case hft_compressor::StreamType::Depth: return HFTC_STREAM_DEPTH;
        case hft_compressor::StreamType::Unknown: return HFTC_STREAM_UNKNOWN;
    }
    return HFTC_STREAM_UNKNOWN;
}

}  // namespace

extern "C" hftc_status hftc_decoder_open(const hftc_replay_decode_request* request,
                                          hftc_decoder** out_decoder) {
    if (request == nullptr || out_decoder == nullptr) return HFTC_STATUS_INVALID_ARGUMENT;
    *out_decoder = nullptr;
    auto* decoder = new (std::nothrow) hftc_decoder{};
    if (decoder == nullptr) return HFTC_STATUS_IO_ERROR;
    decoder->request.artifact.compressedRoot = request->compressed_root == nullptr ? "" : request->compressed_root;
    decoder->request.artifact.sessionDir = request->session_dir == nullptr ? "" : request->session_dir;
    decoder->request.artifact.sessionId = request->session_id == nullptr ? "" : request->session_id;
    decoder->request.artifact.preferredPipelineId = request->preferred_pipeline_id == nullptr ? "" : request->preferred_pipeline_id;
    decoder->request.artifact.streamType = toStreamType(request->stream_type);
    decoder->request.artifact.preference = toPreference(request->artifact_preference);
    decoder->request.maxRecordsPerBatch = request->max_records_per_batch == 0u ? 4096u : request->max_records_per_batch;
    if (decoder->request.artifact.streamType == hft_compressor::StreamType::Unknown) {
        delete decoder;
        return HFTC_STATUS_INVALID_ARGUMENT;
    }
    *out_decoder = decoder;
    return HFTC_STATUS_OK;
}

extern "C" hftc_status hftc_decoder_decode_all(hftc_decoder* decoder,
                                                hftc_record_batch_callback callback,
                                                void* user_data) {
    if (decoder == nullptr || callback == nullptr) return HFTC_STATUS_INVALID_ARGUMENT;
    try {
        const auto status = hft_compressor::decodeReplayRecordBatches(
            decoder->request,
            [&](const hft_compressor::ReplayRecordBatchV1& batch) -> bool {
            std::vector<hftc_trade_record_v1> trades;
            trades.reserve(batch.trades.size());
            for (const auto& row : batch.trades) {
                trades.push_back(hftc_trade_record_v1{row.tsNs, row.priceE8, row.qtyE8, row.side});
            }
            std::vector<hftc_bookticker_record_v1> booktickers;
            booktickers.reserve(batch.bookTickers.size());
            for (const auto& row : batch.bookTickers) {
                booktickers.push_back(hftc_bookticker_record_v1{
                    row.tsNs,
                    row.bidPriceE8,
                    row.bidQtyE8,
                    row.askPriceE8,
                    row.askQtyE8,
                });
            }
            std::vector<hftc_depth_record_v1> depths;
            depths.reserve(batch.depths.size());
            for (const auto& row : batch.depths) {
                depths.push_back(hftc_depth_record_v1{row.tsNs, row.firstLevelIndex, row.levelCount});
            }
            std::vector<hftc_depth_level_v1> depthLevels;
            depthLevels.reserve(batch.depthLevels.size());
            for (const auto& row : batch.depthLevels) {
                depthLevels.push_back(hftc_depth_level_v1{row.priceE8, row.qtyE8, row.side});
            }
            hftc_record_batch_v1 out{};
            out.stream_type = toCStreamType(batch.streamType);
            out.first_line_number = batch.firstLineNumber;
            out.line_count = batch.lineCount;
            out.decoded_bytes = batch.decodedBytes;
            out.trades = trades.data();
            out.trade_count = trades.size();
            out.booktickers = booktickers.data();
            out.bookticker_count = booktickers.size();
            out.depths = depths.data();
            out.depth_count = depths.size();
            out.depth_levels = depthLevels.data();
            out.depth_level_count = depthLevels.size();
                return callback(&out, user_data) != 0;
            });
        return toCStatus(status);
    } catch (...) {
        return HFTC_STATUS_IO_ERROR;
    }
}

extern "C" void hftc_decoder_close(hftc_decoder* decoder) {
    delete decoder;
}

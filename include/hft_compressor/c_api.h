#pragma once

#include <stddef.h>
#include <stdint.h>

#include "hft_compressor/api.hpp"

#ifdef __cplusplus
extern "C" {
#endif

typedef enum hftc_status {
    HFTC_STATUS_OK = 0,
    HFTC_STATUS_INVALID_ARGUMENT = 1,
    HFTC_STATUS_IO_ERROR = 2,
    HFTC_STATUS_UNSUPPORTED_STREAM = 3,
    HFTC_STATUS_UNSUPPORTED_PIPELINE = 4,
    HFTC_STATUS_DEPENDENCY_UNAVAILABLE = 5,
    HFTC_STATUS_NOT_IMPLEMENTED = 6,
    HFTC_STATUS_CORRUPT_DATA = 7,
    HFTC_STATUS_DECODE_ERROR = 8,
    HFTC_STATUS_VERIFICATION_FAILED = 9,
    HFTC_STATUS_CALLBACK_STOPPED = 10,
} hftc_status;

typedef enum hftc_stream_type {
    HFTC_STREAM_UNKNOWN = 0,
    HFTC_STREAM_TRADES = 1,
    HFTC_STREAM_BOOKTICKER = 2,
    HFTC_STREAM_DEPTH = 3,
} hftc_stream_type;

typedef enum hftc_artifact_preference {
    HFTC_ARTIFACT_CURRENT_BASELINE = 1,
    HFTC_ARTIFACT_REPLAY = 2,
    HFTC_ARTIFACT_ARCHIVE = 3,
    HFTC_ARTIFACT_LIVE = 4,
} hftc_artifact_preference;

typedef struct hftc_replay_decode_request {
    const char* compressed_root;
    const char* session_dir;
    const char* session_id;
    const char* preferred_pipeline_id;
    uint32_t stream_type;
    uint32_t artifact_preference;
    size_t max_records_per_batch;
} hftc_replay_decode_request;

typedef struct hftc_trade_record_v1 {
    int64_t ts_ns;
    int64_t price_e8;
    int64_t qty_e8;
    int64_t side;
} hftc_trade_record_v1;

typedef struct hftc_bookticker_record_v1 {
    int64_t ts_ns;
    int64_t bid_price_e8;
    int64_t bid_qty_e8;
    int64_t ask_price_e8;
    int64_t ask_qty_e8;
} hftc_bookticker_record_v1;

typedef struct hftc_depth_level_v1 {
    int64_t price_e8;
    int64_t qty_e8;
    int64_t side;
} hftc_depth_level_v1;

typedef struct hftc_depth_record_v1 {
    int64_t ts_ns;
    uint32_t first_level_index;
    uint32_t level_count;
} hftc_depth_record_v1;

typedef struct hftc_record_batch_v1 {
    uint32_t stream_type;
    uint64_t first_line_number;
    uint64_t line_count;
    uint64_t decoded_bytes;
    const hftc_trade_record_v1* trades;
    size_t trade_count;
    const hftc_bookticker_record_v1* booktickers;
    size_t bookticker_count;
    const hftc_depth_record_v1* depths;
    size_t depth_count;
    const hftc_depth_level_v1* depth_levels;
    size_t depth_level_count;
} hftc_record_batch_v1;

typedef int (*hftc_record_batch_callback)(const hftc_record_batch_v1* batch, void* user_data);

typedef struct hftc_decoder hftc_decoder;

HFT_COMPRESSOR_API hftc_status hftc_decoder_open(const hftc_replay_decode_request* request,
                                                 hftc_decoder** out_decoder);
HFT_COMPRESSOR_API hftc_status hftc_decoder_decode_all(hftc_decoder* decoder,
                                                       hftc_record_batch_callback callback,
                                                       void* user_data);
HFT_COMPRESSOR_API void hftc_decoder_close(hftc_decoder* decoder);

#ifdef __cplusplus
}
#endif

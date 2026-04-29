#pragma once

#include <cstdint>
#include <filesystem>
#include <string>

#include "hft_compressor/api.hpp"
#include "hft_compressor/status.hpp"
#include "hft_compressor/stream_type.hpp"

namespace hft_compressor {

struct CompressionResult {
    Status status{Status::Ok};
    std::string error{};
    std::string pipelineId{};
    std::string representation{};
    std::string transform{};
    std::string entropy{};
    std::string profile{};
    std::string implementationKind{};
    StreamType streamType{StreamType::Unknown};
    std::filesystem::path inputPath{};
    std::filesystem::path outputPath{};
    std::filesystem::path metricsPath{};
    std::uint64_t inputBytes{0};
    std::uint64_t outputBytes{0};
    std::uint64_t lineCount{0};
    std::uint64_t blockCount{0};
    std::uint64_t encodeNs{0};
    std::uint64_t decodeNs{0};
    std::uint64_t encodeCycles{0};
    std::uint64_t decodeCycles{0};
    bool roundtripOk{false};
};

struct DecodeVerifyResult {
    Status status{Status::Ok};
    std::string error{};
    std::string pipelineId{};
    std::string profile{};
    StreamType streamType{StreamType::Unknown};
    std::filesystem::path compressedPath{};
    std::filesystem::path canonicalPath{};
    std::filesystem::path metricsPath{};
    std::uint64_t compressedBytes{0};
    std::uint64_t decodedBytes{0};
    std::uint64_t canonicalBytes{0};
    std::uint64_t comparedBytes{0};
    std::uint64_t mismatchBytes{0};
    double mismatchPercent{0.0};
    bool byteExact{false};
    bool recordExact{false};
    std::uint64_t canonicalRecordCount{0};
    std::uint64_t decodedRecordCount{0};
    std::uint64_t firstMismatchLine{0};
    std::string firstMismatchField{};
    std::string firstMismatchStream{};
    std::uint64_t canonicalByteHash{0};
    std::uint64_t decodedByteHash{0};
    std::uint64_t canonicalRecordHash{0};
    std::uint64_t decodedRecordHash{0};
    std::uint64_t replayTimeSpanNs{0};
    double estimatedReplayMultiplier{0.0};
    std::uint64_t lineCount{0};
    std::uint64_t blockCount{0};
    std::uint64_t decodeNs{0};
    std::uint64_t decodeCycles{0};
    bool verified{false};
    std::uint64_t firstMismatchOffset{0};
    std::string firstMismatchPreviewCanonical{};
    std::string firstMismatchPreviewDecoded{};
};

HFT_COMPRESSOR_API double ratio(const CompressionResult& result) noexcept;
HFT_COMPRESSOR_API double encodeMbPerSec(const CompressionResult& result) noexcept;
HFT_COMPRESSOR_API double decodeMbPerSec(const CompressionResult& result) noexcept;
HFT_COMPRESSOR_API double decodeMbPerSec(const DecodeVerifyResult& result) noexcept;
HFT_COMPRESSOR_API std::string toMetricsJson(const CompressionResult& result);
HFT_COMPRESSOR_API std::string toVerifyMetricsJson(const DecodeVerifyResult& result);

}  // namespace hft_compressor

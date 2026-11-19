#include "hft_compressor/Compressor.hpp"
#include "hft_compressor/CorpusLab.hpp"
#include "hft_compressor/ReplayDecode.hpp"

#include <algorithm>
#include <bit>
#include <fstream>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <utility>
#include <vector>

#include "../../Runtime/src/Codecs/ByteBlock/BinaryTransform.hpp"
#include "../../Runtime/src/Common/CompressionInternals.hpp"
#include "../../Runtime/src/Common/Timing.hpp"
#include "../../Runtime/src/Container/Hfc/Format.hpp"
#include "../../Runtime/src/Pipelines/PipelineBackend.hpp"

namespace hft_compressor {
namespace {

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

struct VerifyRecord {
    StreamType streamType{StreamType::Unknown};
    std::int64_t tsNs{0};
    std::int64_t values[8]{};
    std::uint32_t valueCount{0};
    std::uint64_t depthHash{kFnvOffset};
};

VerifyRecord extractBinaryRecord(const ByteBlockLayout& layout, std::span<const std::uint8_t> rec) noexcept {
    VerifyRecord out{};
    out.streamType = layout.streamType;
    if (layout.timestampOffset + 8u <= layout.recordBytes) {
        out.tsNs = std::bit_cast<std::int64_t>(codecs::byte_block::word(rec, layout.timestampOffset, 8u));
    }
    if (layout.streamType == StreamType::Trades) {
        out.values[0] = std::bit_cast<std::int64_t>(codecs::byte_block::word(rec, layout.payloadOffset, 8u));
        out.values[1] = std::bit_cast<std::int64_t>(codecs::byte_block::word(rec, layout.payloadOffset + 8u, 8u));
        out.values[2] = static_cast<std::int64_t>(rec[layout.payloadOffset + 16u]);
        out.valueCount = 3u;
    } else if (layout.streamType == StreamType::BookTicker) {
        for (std::uint32_t i = 0u; i < 4u; ++i) {
            out.values[i] = std::bit_cast<std::int64_t>(
                codecs::byte_block::word(rec, layout.payloadOffset + 8u * i, 8u));
        }
        out.valueCount = 4u;
    } else if (layout.streamType == StreamType::Depth) {
        const auto count = codecs::byte_block::word(rec, layout.depthLevelCountOffset, 2u);
        out.valueCount = static_cast<std::uint32_t>(count);
        for (std::size_t i = 0u; i < count; ++i) {
            const std::size_t off = layout.depthLevelsOffset + i * layout.depthLevelStride;
            hashInt64(out.depthHash, std::bit_cast<std::int64_t>(codecs::byte_block::word(rec, off, 8u)));
            hashInt64(out.depthHash, std::bit_cast<std::int64_t>(codecs::byte_block::word(rec, off + 8u, 8u)));
            hashInt64(out.depthHash, static_cast<std::int64_t>(rec[off + 16u]));
        }
    } else {
        hashBytes(out.depthHash, rec);
    }
    return out;
}

void hashRecord(std::uint64_t& hash, const VerifyRecord& record) noexcept {
    hashText(hash, streamTypeToString(record.streamType));
    hashInt64(hash, record.tsNs);
    hashInt64(hash, static_cast<std::int64_t>(record.valueCount));
    if (record.streamType == StreamType::Depth ||
        record.streamType == StreamType::DepthCoverage ||
        record.streamType == StreamType::Unknown) {
        hashInt64(hash, static_cast<std::int64_t>(record.depthHash));
    }
    for (std::uint32_t i = 0u; i < record.valueCount && i < 8u; ++i) {
        hashInt64(hash, record.values[i]);
    }
}

std::string firstDifferingField(const VerifyRecord& expected, const VerifyRecord& actual) {
    if (expected.streamType != actual.streamType) return "stream";
    if (expected.tsNs != actual.tsNs) return "ts_ns";
    if (expected.valueCount != actual.valueCount) return "field_count";
    if (expected.streamType == StreamType::Depth && expected.depthHash != actual.depthHash) return "levels";
    const std::string_view namesTrades[] = {"price_e8", "qty_e8", "side"};
    const std::string_view namesBookTicker[] = {"bid_price_e8", "bid_qty_e8", "ask_price_e8", "ask_qty_e8"};
    for (std::uint32_t i = 0u; i < expected.valueCount && i < 8u; ++i) {
        if (expected.values[i] == actual.values[i]) continue;
        if (expected.streamType == StreamType::Trades && i < 3u) return std::string{namesTrades[i]};
        if (expected.streamType == StreamType::BookTicker && i < 4u) return std::string{namesBookTicker[i]};
        return expected.streamType == StreamType::Depth ? "levels" : "field";
    }
    return {};
}

void updateReplaySpan(DecodeVerifyResult& result,
                      std::int64_t tsNs,
                      bool& sawTs,
                      std::int64_t& firstTs,
                      std::int64_t& lastTs) noexcept {
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
    out << "  " << q << "first_mismatch_stream" << q << ": " << q << result.firstMismatchStream << q << '\n';
    out << '}' << '\n';
    return out.str();
}

DecodeVerifyResult decodeAndVerify(const DecodeVerifyRequest& request) noexcept {
    try {
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
        result.metricsPath = request.compressedPath.parent_path() /
            (request.compressedPath.stem().string() + ".verify.json");

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

        std::vector<std::uint8_t> canonicalBuf;
        BinarySliceView canonicalSlice{};
        if (needsCanonical) {
            if (!internal::readFileBytes(request.canonicalPath, canonicalBuf)) {
                result.status = Status::IoError;
                result.error = "failed to read canonical binary slice";
                return result;
            }
            result.canonicalBytes = canonicalBuf.size();
            const auto parseSt = parseBinarySlice(canonicalBuf, canonicalSlice);
            if (!isOk(parseSt)) {
                result.status = parseSt;
                result.error = "canonical file is not a valid binary slice";
                return result;
            }
        }

        std::vector<std::uint8_t> decodedBuf;
        if (needsCanonical) decodedBuf.reserve(canonicalBuf.size());

        const auto decodeStartNs = timing::nowNs();
        const auto decodeStartCycles = timing::readCycles();
        const auto decodeStatus = backend->decodeJsonl(request.compressedPath, [&](std::span<const std::uint8_t> block) {
            decodedBuf.insert(decodedBuf.end(), block.begin(), block.end());
            return true;
        });
        result.decodeCycles = timing::readCycles() - decodeStartCycles;
        result.decodeNs = timing::nowNs() - decodeStartNs;
        if (!isOk(decodeStatus)) {
            result.status = decodeStatus;
            result.error = "failed to decode artifact";
            return result;
        }
        result.decodedBytes = decodedBuf.size();

        BinarySliceView decodedSlice{};
        const auto decParseSt = parseBinarySlice(decodedBuf, decodedSlice);
        if (!isOk(decParseSt)) {
            result.status = decParseSt;
            result.error = "decoded output is not a valid binary slice";
            return result;
        }
        result.decodedRecordCount = decodedSlice.recordCount;

        result.decodedByteHash = kFnvOffset;
        hashBytes(result.decodedByteHash, decodedBuf);

        bool sawTs = false;
        std::int64_t firstTs = 0, lastTs = 0;
        result.decodedRecordHash = kFnvOffset;
        for (std::uint32_t i = 0u; i < decodedSlice.recordCount; ++i) {
            const auto rec = decodedSlice.records.subspan(
                static_cast<std::size_t>(i) * decodedSlice.layout.recordBytes,
                decodedSlice.layout.recordBytes);
            const auto vr = extractBinaryRecord(decodedSlice.layout, rec);
            hashRecord(result.decodedRecordHash, vr);
            updateReplaySpan(result, vr.tsNs, sawTs, firstTs, lastTs);
        }

        bool byteOk = true;
        bool recordOk = true;
        if (needsCanonical) {
            result.canonicalRecordCount = canonicalSlice.recordCount;
            result.canonicalByteHash = kFnvOffset;
            hashBytes(result.canonicalByteHash, canonicalBuf);

            result.canonicalRecordHash = kFnvOffset;
            const std::uint32_t minRecs = std::min(canonicalSlice.recordCount, decodedSlice.recordCount);
            for (std::uint32_t i = 0u; i < canonicalSlice.recordCount; ++i) {
                const auto cRec = canonicalSlice.records.subspan(
                    static_cast<std::size_t>(i) * canonicalSlice.layout.recordBytes,
                    canonicalSlice.layout.recordBytes);
                const auto cvr = extractBinaryRecord(canonicalSlice.layout, cRec);
                hashRecord(result.canonicalRecordHash, cvr);
                if (verifyRecordExact && i < minRecs) {
                    const auto dRec = decodedSlice.records.subspan(
                        static_cast<std::size_t>(i) * decodedSlice.layout.recordBytes,
                        decodedSlice.layout.recordBytes);
                    const auto dvr = extractBinaryRecord(decodedSlice.layout, dRec);
                    const auto diffField = firstDifferingField(cvr, dvr);
                    if (!diffField.empty() && result.firstMismatchLine == 0u) {
                        result.firstMismatchLine = i + 1u;
                        result.firstMismatchField = diffField;
                        result.firstMismatchStream = std::string{streamTypeToString(result.streamType)};
                        recordOk = false;
                    }
                }
            }
            if (verifyRecordExact && canonicalSlice.recordCount != decodedSlice.recordCount) {
                recordOk = false;
                if (result.firstMismatchLine == 0u) {
                    result.firstMismatchLine = minRecs + 1u;
                    result.firstMismatchField = "record_count";
                    result.firstMismatchStream = std::string{streamTypeToString(result.streamType)};
                }
            }

            if (verifyByteExact) {
                const std::size_t minBytes = std::min(canonicalBuf.size(), decodedBuf.size());
                result.comparedBytes = minBytes;
                for (std::size_t b = 0u; b < minBytes; ++b) {
                    if (canonicalBuf[b] != decodedBuf[b]) {
                        if (result.mismatchBytes == 0u) result.firstMismatchOffset = b;
                        ++result.mismatchBytes;
                    }
                }
                if (canonicalBuf.size() != decodedBuf.size()) {
                    const std::size_t extra = canonicalBuf.size() > decodedBuf.size()
                        ? canonicalBuf.size() - decodedBuf.size()
                        : decodedBuf.size() - canonicalBuf.size();
                    if (result.mismatchBytes == 0u) result.firstMismatchOffset = minBytes;
                    result.mismatchBytes += extra;
                }
                byteOk = (result.mismatchBytes == 0u);
                const auto denominator = std::max(result.canonicalBytes, result.decodedBytes);
                result.mismatchPercent = denominator == 0u
                    ? 0.0
                    : (static_cast<double>(result.mismatchBytes) * 100.0) / static_cast<double>(denominator);
            }
        }

        result.byteExact = verifyByteExact ? byteOk : false;
        result.recordExact = verifyRecordExact ? recordOk : false;
        result.estimatedReplayMultiplier = result.decodeNs == 0u
            ? 0.0
            : static_cast<double>(result.replayTimeSpanNs) / static_cast<double>(result.decodeNs);
        result.verified = (!verifyByteExact || result.byteExact) && (!verifyRecordExact || result.recordExact);
        result.status = result.verified ? Status::Ok : Status::VerificationFailed;
        if (!result.verified) result.error = "lossless binary slice verification failed";
        (void)internal::writeTextFile(result.metricsPath, toVerifyMetricsJson(result));
        return result;
    } catch (...) {
        DecodeVerifyResult failed{};
        failed.status = Status::DecodeError;
        return failed;
    }
}

}  // namespace hft_compressor

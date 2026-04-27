#include "hft_compressor/compressor.hpp"

#include <algorithm>
#include <fstream>
#include <sstream>
#include <system_error>
#include <utility>
#include <vector>

#include "common/CompressionInternals.hpp"
#include "common/timing.hpp"
#include "container/hfc/format.hpp"

namespace hft_compressor {
namespace {

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

    std::vector<std::uint8_t> compressedFile;
    if (!internal::readFileBytes(request.compressedPath, compressedFile)) {
        return failVerify(Status::IoError, request, "failed to read compressed file");
    }

    DecodeVerifyResult result{};
    result.pipelineId = request.pipelineId;
    if (const auto* pipeline = findPipeline(request.pipelineId); pipeline != nullptr) {
        result.profile = std::string{pipeline->profile};
    }
    result.compressedPath = request.compressedPath;
    result.canonicalPath = request.canonicalPath;
    result.metricsPath = request.compressedPath.parent_path() / (request.compressedPath.stem().string() + ".verify.json");
    result.compressedBytes = static_cast<std::uint64_t>(compressedFile.size());

    format::FileHeader header{};
    if (!format::parseFileHeader(compressedFile.data(), compressedFile.size(), header)) {
        result.status = Status::CorruptData;
        result.error = "failed to parse hfc header";
        return result;
    }
    result.streamType = format::streamFromWire(header.stream);
    result.lineCount = header.lineCount;
    result.blockCount = header.blockCount;

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
    const auto decodeStatus = decodeHfcBuffer(compressedFile, [&](std::span<const std::uint8_t> block) {
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

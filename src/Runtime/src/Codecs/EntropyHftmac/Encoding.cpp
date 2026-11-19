#include "EntropyHftMac.hpp"
#include "EntropyHftMacInternal.hpp"
#include "../BaseDecode.hpp"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <span>
#include <string>
#include <system_error>
#include <vector>

#include "hft_compressor/CorpusLab.hpp"
#include "../../Common/CompressionInternals.hpp"
#include "../../Common/Timing.hpp"
#include "../../Container/Hfc/Format.hpp"

namespace hft_compressor::codecs::entropy_hftmac {

using namespace detail;

CompressionResult compress(const CompressionRequest& request, const PipelineDescriptor& pipeline) noexcept try {
    if (request.inputPath.empty()) {
        return internal::fail(Status::InvalidArgument, request, &pipeline, "input path is empty");
    }
    const StreamType inferredStream = inferStreamTypeFromPath(request.inputPath);
    if (inferredStream == StreamType::Unknown || inferredStream == StreamType::DepthCoverage) {
        return internal::fail(
            Status::UnsupportedStream,
            request,
            &pipeline,
            "expected trades, bookticker, or depth binary slice (.bin/.hbs)");
    }

    const auto readStartNs = timing::nowNs();
    std::vector<std::uint8_t> original;
    if (!internal::readFileBytes(request.inputPath, original)) {
        return internal::fail(Status::IoError, request, &pipeline, "failed to read binary slice input");
    }
    const auto readElapsedNs = timing::nowNs() - readStartNs;

    BinarySliceView slice{};
    const auto sliceStatus = parseBinarySlice(original, slice);
    if (!isOk(sliceStatus) || slice.layout.streamType != inferredStream) {
        return internal::fail(
            isOk(sliceStatus) ? Status::UnsupportedStream : sliceStatus,
            request,
            &pipeline,
            "input is not a valid canonical binary slice for stream");
    }

    const auto base = baseKindFor(inferredStream);
    std::vector<std::uint8_t> baseBytes;
    std::uint64_t recordCount = 0u;
    std::uint64_t baseCoreNs = 0u;
    std::uint64_t baseCycles = 0u;
    std::string baseError;

    // Encode base transform strictly in memory (zero .base.tmp disk I/O).
    Status baseStatus = Status::UnsupportedStream;
    switch (base) {
        case BaseKind::Trades:
            baseStatus = trades_grouped_delta_qtydict::encodeSliceBytes(
                slice.layout, slice.records, baseBytes, recordCount, baseCoreNs, baseCycles, baseError);
            break;
        case BaseKind::BookTicker:
            baseStatus = bookticker_delta_mask::encodeSliceBytes(
                slice.layout, slice.records, baseBytes, recordCount, baseCoreNs, baseCycles, baseError);
            break;
        case BaseKind::Depth:
            baseStatus = depth_ladder_offset::encodeSliceBytes(
                slice.layout, slice.records, baseBytes, recordCount, baseCoreNs, baseCycles, baseError);
            break;
    }
    if (!isOk(baseStatus) || baseBytes.empty()) {
        return internal::fail(
            isOk(baseStatus) ? Status::DecodeError : baseStatus,
            request,
            &pipeline,
            baseError.empty() ? "base HFT-MAC in-memory transform failed" : baseError);
    }

    const auto outputPath = internal::outputPathFor(request, pipeline, inferredStream);
    std::error_code ec;
    std::filesystem::create_directories(outputPath.parent_path(), ec);
    if (ec) {
        return internal::fail(Status::IoError, request, &pipeline, "failed to create output directory");
    }

    CompressionResult result{};
    internal::applyPipeline(result, &pipeline);
    result.streamType = inferredStream;
    result.inputPath = request.inputPath;
    result.outputPath = outputPath;
    result.metricsPath = outputPath.parent_path() / (outputPath.stem().string() + ".metrics.json");
    result.readNs = readElapsedNs;
    result.inputBytes = original.size();
    result.lineCount = recordCount;
    result.blockCount = 1u;

    const auto entropy = entropyKindFor(pipeline.id);
    const auto encodeStartNs = timing::nowNs();
    const auto encodeStartCycles = timing::readCycles();
    auto payload = arithmeticEncode(baseBytes, entropy);
    result.encodeCycles = (timing::readCycles() - encodeStartCycles) + baseCycles;
    result.encodeCoreNs = (timing::nowNs() - encodeStartNs) + baseCoreNs;
    result.encodeNs = result.encodeCoreNs;

    Header header{};
    header.entropy = static_cast<std::uint16_t>(entropy);
    header.base = static_cast<std::uint16_t>(base);
    header.stream = format::streamToWire(inferredStream);
    header.inputBytes = result.inputBytes;
    header.baseBytes = baseBytes.size();
    header.payloadBytes = payload.size();
    header.outputBytes = kHeaderBytes + payload.size();
    header.lineCount = result.lineCount;
    header.payloadCrc32c = format::crc32c(payload);
    header.decodedCrc32c = format::crc32c(baseBytes);
    header.headerCrc32c = headerCrc32c(header);
    const auto headerBytes = serializeHeader(header, true);

    std::vector<std::uint8_t> artifactBytes;
    artifactBytes.reserve(headerBytes.size() + payload.size());
    artifactBytes.insert(artifactBytes.end(), headerBytes.begin(), headerBytes.end());
    artifactBytes.insert(artifactBytes.end(), payload.begin(), payload.end());

    const auto writeStartNs = timing::nowNs();
    std::ofstream out(outputPath, std::ios::binary | std::ios::trunc);
    if (!out) {
        return internal::fail(Status::IoError, request, &pipeline, "failed to open entropy artifact");
    }
    out.write(reinterpret_cast<const char*>(artifactBytes.data()),
              static_cast<std::streamsize>(artifactBytes.size()));
    out.close();
    result.writeNs = timing::nowNs() - writeStartNs;
    if (!out) {
        return internal::fail(Status::IoError, request, &pipeline, "failed to write entropy artifact");
    }
    result.outputBytes = header.outputBytes;

    std::size_t decodedOffset = 0u;
    bool decodedMatchesInput = true;
    const auto decodeStartNs = timing::nowNs();
    const auto decodeStartCycles = timing::readCycles();
    const auto decodeStatus = decode(artifactBytes, [&](std::span<const std::uint8_t> block) {
        if (decodedOffset + block.size() > original.size()) {
            decodedMatchesInput = false;
            return false;
        }
        decodedMatchesInput = std::equal(
            block.begin(),
            block.end(),
            original.begin() + static_cast<std::ptrdiff_t>(decodedOffset));
        decodedOffset += block.size();
        return decodedMatchesInput;
    });
    result.decodeCycles = timing::readCycles() - decodeStartCycles;
    result.decodeNs = timing::nowNs() - decodeStartNs;
    result.decodeCoreNs = result.decodeNs;
    result.roundtripOk = isOk(decodeStatus) && decodedMatchesInput && decodedOffset == original.size();
    result.status = result.roundtripOk ? Status::Ok : Status::DecodeError;
    if (!result.roundtripOk) result.error = "entropy roundtrip check failed";

    (void)internal::writeTextFile(result.metricsPath, toMetricsJson(result));
    return result;
} catch (...) {
    CompressionResult failed{};
    failed.status = Status::DecodeError;
    return failed;
}

}  // namespace hft_compressor::codecs::entropy_hftmac

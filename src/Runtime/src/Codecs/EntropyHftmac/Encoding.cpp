#include "EntropyHftMac.hpp"
#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <span>
#include <sstream>
#include <string_view>
#include <system_error>
#include <vector>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include "../BooktickerDeltaMask/BookTickerDeltaMask.hpp"
#include "../DepthLadderOffset/DepthLadderOffset.hpp"
#include "../TradesGroupedDeltaQtydict/TradesGroupedDeltaQtyDict.hpp"
#include "../../Common/CompressionInternals.hpp"
#include "../../Common/Timing.hpp"
#include "../../Container/Hfc/Format.hpp"
#include "EntropyHftMacInternal.hpp"

namespace hft_compressor::codecs::entropy_hftmac::detail {

struct BaseArtifactCleanup final {
    const CompressionResult& result;
    int artifact{-1}, metrics{-1};
    explicit BaseArtifactCleanup(const CompressionResult& value) noexcept
        : result(value),
          artifact(::open(value.outputPath.c_str(), O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC)),
          metrics(::open(value.metricsPath.c_str(), O_RDONLY | O_NONBLOCK | O_NOFOLLOW | O_CLOEXEC)) {}
    ~BaseArtifactCleanup() noexcept {
        const std::filesystem::path* paths[]{&result.outputPath, &result.metricsPath};
        const int descriptors[]{artifact, metrics};
        for (std::size_t i = 0; i < 2u; ++i) {
            if (descriptors[i] < 0) continue;
            struct stat opened{}, current{};
            if (::fstat(descriptors[i], &opened) == 0 && opened.st_uid == ::geteuid()
                && ::lstat(paths[i]->c_str(), &current) == 0 && S_ISREG(current.st_mode)
                && opened.st_dev == current.st_dev && opened.st_ino == current.st_ino)
                (void)::unlink(paths[i]->c_str());
            (void)::close(descriptors[i]);
        }
    }
};

}

namespace hft_compressor::codecs::entropy_hftmac {

using namespace detail;

CompressionResult compress(const CompressionRequest& request, const PipelineDescriptor& pipeline) noexcept try {
    const StreamType streamType = inferStreamTypeFromPath(request.inputPath);
    if (streamType == StreamType::Unknown) {
        auto result = internal::fail(Status::UnsupportedStream, request, &pipeline, "expected trades.jsonl, bookticker.jsonl, or depth.jsonl");
        return result;
    }

    const auto outputPath = internal::outputPathFor(request, pipeline, streamType);
    const auto base = baseKindFor(streamType);
    const auto* basePipeline = findPipeline(basePipelineId(base));
    if (basePipeline == nullptr) {
        auto result = internal::fail(Status::UnsupportedPipeline, request, &pipeline, "base HFT-MAC pipeline is missing");
        return result;
    }

    std::error_code ec;
    std::filesystem::create_directories(outputPath.parent_path(), ec);
    if (ec) {
        auto result = internal::fail(Status::IoError, request, &pipeline, "failed to create output directory");
        return result;
    }

    CompressionRequest baseRequest = request;
    baseRequest.pipelineId = std::string{basePipeline->id};
    baseRequest.outputPathOverride = outputPath.parent_path() / (outputPath.stem().string() + ".base.tmp");
    const auto baseResult = hft_compressor::compress(baseRequest);
    BaseArtifactCleanup cleanup(baseResult);
    if (!isOk(baseResult.status) || !baseResult.roundtripOk) {
        auto result = internal::fail(baseResult.status, request, &pipeline, baseResult.error.empty() ? "base HFT-MAC compression failed" : baseResult.error);
        return result;
    }

    std::vector<std::uint8_t> baseBytes;
    if (!internal::readFileBytes(baseResult.outputPath, baseBytes)) {
        auto result = internal::fail(Status::IoError, request, &pipeline, "failed to read base artifact");
        return result;
    }

    CompressionResult result{};
    internal::applyPipeline(result, &pipeline);
    result.streamType = streamType;
    result.inputPath = request.inputPath;
    result.outputPath = outputPath;
    result.metricsPath = outputPath.parent_path() / (outputPath.stem().string() + ".metrics.json");
    result.inputBytes = baseResult.inputBytes;
    result.lineCount = baseResult.lineCount;
    result.blockCount = 1u;

    const auto entropy = entropyKindFor(pipeline.id);
    const auto encodeStartNs = timing::nowNs();
    const auto encodeStartCycles = timing::readCycles();
    auto payload = arithmeticEncode(baseBytes, entropy);
    result.encodeCycles = timing::readCycles() - encodeStartCycles;
    result.encodeNs = timing::nowNs() - encodeStartNs + baseResult.encodeNs;
    result.encodeCoreNs = result.encodeNs;

    Header header{};
    header.entropy = static_cast<std::uint16_t>(entropy);
    header.base = static_cast<std::uint16_t>(base);
    header.stream = format::streamToWire(streamType);
    header.inputBytes = result.inputBytes;
    header.baseBytes = baseBytes.size();
    header.payloadBytes = payload.size();
    header.outputBytes = kHeaderBytes + payload.size();
    header.lineCount = result.lineCount;
    header.payloadCrc32c = format::crc32c(payload);
    header.decodedCrc32c = format::crc32c(baseBytes);
    header.headerCrc32c = headerCrc32c(header);
    const auto headerBytes = serializeHeader(header, true);

    const auto writeStartNs = timing::nowNs();
    std::ofstream out(outputPath, std::ios::binary | std::ios::trunc);
    if (!out) {
        auto failed = internal::fail(Status::IoError, request, &pipeline, "failed to open entropy artifact");
        return failed;
    }
    out.write(reinterpret_cast<const char*>(headerBytes.data()), static_cast<std::streamsize>(headerBytes.size()));
    out.write(reinterpret_cast<const char*>(payload.data()), static_cast<std::streamsize>(payload.size()));
    out.close();
    result.writeNs = timing::nowNs() - writeStartNs;
    if (!out) {
        auto failed = internal::fail(Status::IoError, request, &pipeline, "failed to write entropy artifact");
        return failed;
    }
    result.outputBytes = header.outputBytes;

    std::size_t decodedOffset = 0;
    bool decodedMatchesInput = true;
    std::vector<std::uint8_t> original;
    if (!internal::readFileBytes(request.inputPath, original)) decodedMatchesInput = false;
    const auto decodeStartNs = timing::nowNs();
    const auto decodeStartCycles = timing::readCycles();
    const auto decodeStatus = decodeFile(outputPath, [&](std::span<const std::uint8_t> block) {
        if (decodedOffset + block.size() > original.size()) {
            decodedMatchesInput = false;
            return false;
        }
        decodedMatchesInput = std::equal(block.begin(), block.end(), original.begin() + static_cast<std::ptrdiff_t>(decodedOffset));
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
} catch (...) { CompressionResult failed{}; failed.status = Status::DecodeError; return failed; }

}

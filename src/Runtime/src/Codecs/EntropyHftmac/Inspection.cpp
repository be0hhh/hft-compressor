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
#include "../BooktickerDeltaMask/BookTickerDeltaMask.hpp"
#include "../DepthLadderOffset/DepthLadderOffset.hpp"
#include "../TradesGroupedDeltaQtydict/TradesGroupedDeltaQtyDict.hpp"
#include "../../Common/CompressionInternals.hpp"
#include "../../Common/Timing.hpp"
#include "../../Container/Hfc/Format.hpp"
#include "EntropyHftMacInternal.hpp"

namespace hft_compressor::codecs::entropy_hftmac::detail {

namespace {

ReplayArtifactInfo failArtifact(const std::filesystem::path& path, Status status, std::string error) {
    ReplayArtifactInfo info{};
    info.status = status;
    info.path = path;
    info.error = std::move(error);
    return info;
}

}


}

namespace hft_compressor::codecs::entropy_hftmac {

using namespace detail;

ReplayArtifactInfo inspectArtifact(const std::filesystem::path& path, const PipelineDescriptor& pipeline) noexcept {
    std::vector<std::uint8_t> bytes;
    const auto readStatus = readFile(path, bytes);
    if (!isOk(readStatus)) return failArtifact(path, readStatus, "failed to read entropy artifact");
    Header header{};
    if (!parseHeader(bytes.data(), bytes.size(), header) || !validHeader(header)) {
        return failArtifact(path, Status::CorruptData, "invalid entropy artifact header");
    }
    if (bytes.size() != header.outputBytes) return failArtifact(path, Status::CorruptData, "entropy artifact size mismatch");

    ReplayArtifactInfo info{};
    info.status = Status::Ok;
    info.found = true;
    info.path = path;
    info.formatId = std::string{formatIdFor(static_cast<BaseKind>(header.base), static_cast<EntropyKind>(header.entropy))};
    info.pipelineId = std::string{pipeline.id};
    info.transform = std::string{pipeline.transform};
    info.entropy = std::string{pipeline.entropy};
    info.streamType = format::streamFromWire(header.stream);
    info.version = header.version;
    info.inputBytes = header.inputBytes;
    info.outputBytes = header.outputBytes;
    info.lineCount = header.lineCount;
    info.blockCount = 1u;
    return info;
}

Status inspectEncodedJsonFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept {
    if (!onBlock) return Status::InvalidArgument;
    std::vector<std::uint8_t> bytes;
    const auto readStatus = readFile(path, bytes);
    if (!isOk(readStatus)) return readStatus;
    Header header{};
    std::vector<std::uint8_t> baseBytes;
    const auto status = decodePayload(bytes, header, baseBytes);
    if (!isOk(status)) return status;
    switch (static_cast<BaseKind>(header.base)) {
        case BaseKind::Trades: return trades_grouped_delta_qtydict::decode(baseBytes, onBlock);
        case BaseKind::BookTicker: return bookticker_delta_mask::decode(baseBytes, onBlock);
        case BaseKind::Depth: return depth_ladder_offset::decode(baseBytes, onBlock);
    }
    return Status::CorruptData;
}

Status inspectEncodedBinaryFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept {
    if (!onBlock) return Status::InvalidArgument;
    std::vector<std::uint8_t> bytes;
    const auto readStatus = readFile(path, bytes);
    if (!isOk(readStatus)) return readStatus;
    Header header{};
    if (!parseHeader(bytes.data(), bytes.size(), header) || !validHeader(header)) return Status::CorruptData;
    std::ostringstream out;
    out << "entropy_hftmac bytes=" << header.outputBytes
        << " base_bytes=" << header.baseBytes
        << " payload=" << header.payloadBytes
        << " entropy=" << entropyName(static_cast<EntropyKind>(header.entropy))
        << " stream=" << streamTypeToString(format::streamFromWire(header.stream)) << "\n";
    const auto text = out.str();
    return onBlock({reinterpret_cast<const std::uint8_t*>(text.data()), text.size()}) ? Status::Ok : Status::CallbackStopped;
}

Status inspectStatsJsonFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept {
    if (!onBlock) return Status::InvalidArgument;
    std::vector<std::uint8_t> bytes;
    const auto readStatus = readFile(path, bytes);
    if (!isOk(readStatus)) return readStatus;
    Header header{};
    if (!parseHeader(bytes.data(), bytes.size(), header) || !validHeader(header)) return Status::CorruptData;
    std::ostringstream out;
    out << "{\n"
        << "  \"pipeline_family\": \"entropy_hftmac\",\n"
        << "  \"version\": " << header.version << ",\n"
        << "  \"stream\": \"" << streamTypeToString(format::streamFromWire(header.stream)) << "\",\n"
        << "  \"entropy\": \"" << entropyName(static_cast<EntropyKind>(header.entropy)) << "\",\n"
        << "  \"input_bytes\": " << header.inputBytes << ",\n"
        << "  \"base_bytes\": " << header.baseBytes << ",\n"
        << "  \"payload_bytes\": " << header.payloadBytes << ",\n"
        << "  \"output_bytes\": " << header.outputBytes << "\n"
        << "}\n";
    const auto text = out.str();
    return onBlock({reinterpret_cast<const std::uint8_t*>(text.data()), text.size()}) ? Status::Ok : Status::CallbackStopped;
}

}

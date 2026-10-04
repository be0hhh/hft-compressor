#include "TradesGroupedDeltaQtyDictInternal.hpp"
#include "../../Common/DecodeSource.hpp"

namespace hft_compressor::codecs::trades_grouped_delta_qtydict::codec_detail {


ReplayArtifactInfo failArtifact(const std::filesystem::path& path, Status status, std::string error) {
    ReplayArtifactInfo info{};
    info.status = status;
    info.path = path;
    info.error = std::move(error);
    return info;
}

Status writeStringBlock(const std::string& text, const DecodedBlockCallback& onBlock) {
    if (!onBlock) return Status::InvalidArgument;
    return onBlock(std::span<const std::uint8_t>{reinterpret_cast<const std::uint8_t*>(text.data()), text.size()})
        ? Status::Ok
        : Status::CallbackStopped;
}
}  // namespace codec_detail

namespace hft_compressor::codecs::trades_grouped_delta_qtydict {
using namespace codec_detail;


ReplayArtifactInfo inspectArtifact(const std::filesystem::path& path, const PipelineDescriptor& pipeline) noexcept try {
    if (path.empty()) return failArtifact(path, Status::InvalidArgument, "artifact path is empty");
    internal::FileDecodeSource file(path);
    if (!file.valid()) return failArtifact(path, Status::IoError, "failed to read artifact");
    FileHeader header{};
    const auto status = walkSource(file, nullptr, nullptr, nullptr, &header);
    if (!isOk(status)) return failArtifact(path, status, "invalid trade grouped artifact");
    if (!file.unchanged()) return failArtifact(path, Status::CorruptData, "artifact changed during inspection");
    ReplayArtifactInfo info{};
    info.status = Status::Ok;
    info.found = true;
    info.path = path;
    info.formatId = "hftmac.trades_grouped_delta_qtydict.math.v3";
    info.pipelineId = std::string{pipeline.id};
    info.transform = std::string{pipeline.transform};
    info.entropy = std::string{pipeline.entropy};
    info.streamType = StreamType::Trades;
    info.version = header.version;
    info.inputBytes = header.inputBytes;
    info.outputBytes = header.outputBytes;
    info.lineCount = header.recordCount;
    info.blockCount = header.chunkCount;
    return info;
} catch (...) { ReplayArtifactInfo failed{}; failed.status = Status::DecodeError; return failed; }

Status inspectEncodedJsonFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept try {
    std::vector<std::uint8_t> file;
    if (!readFile(path, file)) return Status::IoError;
    std::ostringstream out;
    const auto status = walkFile(file, nullptr, &out, nullptr);
    if (!isOk(status)) return status;
    return writeStringBlock(out.str(), onBlock);
} catch (...) { return Status::DecodeError; }

Status inspectEncodedBinaryFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept try {
    std::vector<std::uint8_t> file;
    if (!readFile(path, file)) return Status::IoError;
    std::ostringstream out;
    const auto status = walkFile(file, nullptr, nullptr, &out);
    if (!isOk(status)) return status;
    return writeStringBlock(out.str(), onBlock);
} catch (...) { return Status::DecodeError; }

Status inspectStatsJsonFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept try {
    std::vector<std::uint8_t> file;
    if (!readFile(path, file)) return Status::IoError;
    FileHeader header{};
    const auto status = walkFile(file, nullptr, nullptr, nullptr, &header);
    if (!isOk(status)) return status;

    std::uint64_t hotQtyTableBytes = 0;
    std::uint64_t timeStreamBytes = 0;
    std::uint64_t priceStreamBytes = 0;
    std::uint64_t sideStreamBytes = 0;
    std::uint64_t dpZeroStreamBytes = 0;
    std::uint64_t countOneStreamBytes = 0;
    std::uint64_t priceGroupCountOneStreamBytes = 0;
    std::uint64_t qtyCodeStreamBytes = 0;
    std::uint64_t qtyEscapeStreamBytes = 0;
    std::uint64_t dpZeroCount = 0;
    std::uint64_t countOneCount = 0;
    std::uint64_t priceGroupCountOneCount = 0;
    std::int64_t firstPriceScale = 1;
    std::int64_t firstQtyScale = 1;
    std::int64_t firstTimeScale = 1;
    std::uint32_t firstHotQtyBits = 7;

    std::size_t offset = kFileHeaderBytes;
    for (std::uint64_t chunkIndex = 0; chunkIndex < header.chunkCount; ++chunkIndex) {
        if (file.size() - offset < kChunkHeaderBytes) return Status::CorruptData;
        ChunkHeader chunk{};
        if (!parseChunkHeader(file.data() + offset, file.size() - offset, chunk) || !validChunkHeader(chunk)) return Status::CorruptData;
        offset += kChunkHeaderBytes;
        if (chunkIndex == 0u) {
            firstPriceScale = chunk.priceScale;
            firstQtyScale = chunk.qtyScale;
            firstTimeScale = chunk.timeScale;
            firstHotQtyBits = chunk.hotQtyBits;
        }
        const auto tableBytes = static_cast<std::size_t>(chunk.hotQtyTableBytes);
        const auto streamsSize = static_cast<std::size_t>(chunk.timeStreamBytes)
            + chunk.priceStreamBytes + chunk.sideStreamBytes + chunk.dpZeroStreamBytes + chunk.countOneStreamBytes
            + chunk.priceGroupCountOneStreamBytes + chunk.qtyCodeStreamBytes + chunk.qtyEscapeStreamBytes;
        if (file.size() - offset < tableBytes + streamsSize) return Status::CorruptData;
        offset += tableBytes + streamsSize;
        hotQtyTableBytes += tableBytes;
        timeStreamBytes += chunk.timeStreamBytes;
        priceStreamBytes += chunk.priceStreamBytes;
        sideStreamBytes += chunk.sideStreamBytes;
        dpZeroStreamBytes += chunk.dpZeroStreamBytes;
        countOneStreamBytes += chunk.countOneStreamBytes;
        priceGroupCountOneStreamBytes += chunk.priceGroupCountOneStreamBytes;
        qtyCodeStreamBytes += chunk.qtyCodeStreamBytes;
        qtyEscapeStreamBytes += chunk.qtyEscapeStreamBytes;
        dpZeroCount += chunk.dpZeroCount;
        countOneCount += chunk.countOneCount;
        priceGroupCountOneCount += chunk.priceGroupCountOneCount;
    }
    if (offset != file.size()) return Status::CorruptData;

    std::ostringstream out;
    out << "{\n"
        << "  \"pipeline_id\": \"hftmac.trades_grouped_delta_qtydict_math_v3\",\n"
        << "  \"version\": " << header.version << ",\n"
        << "  \"record_count\": " << header.recordCount << ",\n"
        << "  \"chunk_count\": " << header.chunkCount << ",\n"
        << "  \"input_bytes\": " << header.inputBytes << ",\n"
        << "  \"encoded_bytes\": " << header.outputBytes << ",\n"
        << "  \"bytes_per_trade\": " << (header.recordCount == 0u ? 0.0 : static_cast<double>(header.outputBytes) / static_cast<double>(header.recordCount)) << ",\n"
        << "  \"timestamp_group_count\": " << header.timeGroupCount << ",\n"
        << "  \"price_group_count\": " << header.priceGroupCount << ",\n"
        << "  \"qty_escape_count\": " << header.qtyEscapeCount << ",\n"
        << "  \"dp_zero_count\": " << dpZeroCount << ",\n"
        << "  \"count_one_count\": " << countOneCount << ",\n"
        << "  \"price_group_count_one_count\": " << priceGroupCountOneCount << ",\n"
        << "  \"first_chunk_price_scale\": " << firstPriceScale << ",\n"
        << "  \"first_chunk_qty_scale\": " << firstQtyScale << ",\n"
        << "  \"first_chunk_time_scale\": " << firstTimeScale << ",\n"
        << "  \"first_chunk_hot_qty_bits\": " << firstHotQtyBits << ",\n"
        << "  \"hot_qty_table_bytes\": " << hotQtyTableBytes << ",\n"
        << "  \"time_stream_bytes\": " << timeStreamBytes << ",\n"
        << "  \"price_stream_bytes\": " << priceStreamBytes << ",\n"
        << "  \"side_stream_bytes\": " << sideStreamBytes << ",\n"
        << "  \"dp_zero_stream_bytes\": " << dpZeroStreamBytes << ",\n"
        << "  \"count_one_stream_bytes\": " << countOneStreamBytes << ",\n"
        << "  \"price_group_count_one_stream_bytes\": " << priceGroupCountOneStreamBytes << ",\n"
        << "  \"qty_code_stream_bytes\": " << qtyCodeStreamBytes << ",\n"
        << "  \"qty_escape_stream_bytes\": " << qtyEscapeStreamBytes << "\n"
        << "}\n";
    return writeStringBlock(out.str(), onBlock);
} catch (...) { return Status::DecodeError; }
}  // namespace hft_compressor::codecs::trades_grouped_delta_qtydict

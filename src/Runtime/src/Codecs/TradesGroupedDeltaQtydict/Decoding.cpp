#include "TradesGroupedDeltaQtyDictInternal.hpp"
#include "../BaseDecode.hpp"

namespace hft_compressor::codecs::trades_grouped_delta_qtydict::codec_detail {


bool readFile(const std::filesystem::path& path, std::vector<std::uint8_t>& out) {
    return internal::readFileBytes(path, out);
}

Status decodeChunk(
                   const ChunkHeader& header,
                   std::span<const std::int64_t> hotQty,
                   internal::DecodeCursor& time,
                   internal::DecodeCursor& price,
                   internal::DecodeCursor& side,
                   internal::DecodeCursor& dpZero,
                   internal::DecodeCursor& countOne,
                   internal::DecodeCursor& priceGroupCountOne,
                   internal::DecodeCursor& qtyCode,
                   internal::DecodeCursor& qtyEscape,
                   std::string_view lineEnding,
                   internal::DecodeOutput* jsonlOut,
                   std::ostream* encodedJsonOut) {
    internal::DecodeBits sideBits{side}, dpZeroBits{dpZero}, countOneBits{countOne};
    internal::DecodeBits priceGroupCountOneBits{priceGroupCountOne}, qtyCodeBits{qtyCode};
    const auto priceScale = header.priceScale;
    const auto qtyScale = header.qtyScale;
    const auto timeScale = header.timeScale;
    std::int64_t ts = header.baseTsUnit;
    std::int64_t previousPrice = header.basePriceTick;
    std::uint32_t records = 0;
    std::uint32_t priceGroups = 0;
    std::uint32_t qtyEscapes = 0;
    if (encodedJsonOut != nullptr) {
        *encodedJsonOut << "[\n      [" << header.baseTsNs << ", " << header.basePrice << ", [";
        for (std::size_t i = 0; i < hotQty.size(); ++i) {
            if (i != 0u) *encodedJsonOut << ", ";
            *encodedJsonOut << hotQty[i];
        }
        *encodedJsonOut << "]],\n      [\n";
    }
    for (std::uint32_t tg = 0; tg < header.timeGroupCount; ++tg) {
        std::uint64_t dt = 0;
        std::uint64_t groupCount = 0;
        if (!time.varint(dt)) return Status::CorruptData;
        std::uint64_t oneGroup = 0;
        if (!priceGroupCountOneBits.bits(1u, oneGroup)) return Status::CorruptData;
        if (oneGroup != 0u) groupCount = 1u;
        else if (!time.varint(groupCount)) return Status::CorruptData;
        if (dt > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())
            || !internal::addI64(ts, static_cast<std::int64_t>(dt), ts)
            || groupCount == 0u || groupCount > header.priceGroupCount - priceGroups) return Status::CorruptData;
        if (encodedJsonOut != nullptr) {
            if (tg != 0u) *encodedJsonOut << ",\n";
            *encodedJsonOut << "        [" << dt << ", [";
        }
        for (std::uint64_t pg = 0; pg < groupCount; ++pg) {
            std::uint64_t zz = 0;
            std::uint64_t side = 0;
            std::uint64_t tradeCount = 0;
            std::int64_t dp = 0;
            if (!sideBits.bits(1u, side)) return Status::CorruptData;
            std::uint64_t dpZero = 0;
            std::uint64_t countOne = 0;
            if (!dpZeroBits.bits(1u, dpZero) || !countOneBits.bits(1u, countOne)) return Status::CorruptData;
            if (dpZero == 0u) {
                if (!price.varint(zz)) return Status::CorruptData;
                dp = internal::decodeZigzag(zz);
            }
            if (countOne != 0u) {
                tradeCount = 1u;
            } else if (!price.varint(tradeCount)) {
                return Status::CorruptData;
            } else {
                if (tradeCount > std::numeric_limits<std::uint64_t>::max() - 2u) return Status::CorruptData;
                tradeCount += 2u;
            }
            if (side > 1u || tradeCount == 0u || tradeCount > header.recordCount - records) return Status::CorruptData;
            std::int64_t currentPrice{};
            if (!internal::addI64(previousPrice, dp, currentPrice)) return Status::CorruptData;
            previousPrice = currentPrice;
            if (encodedJsonOut != nullptr) {
                if (pg != 0u) *encodedJsonOut << ", ";
                *encodedJsonOut << '[' << dp << ", " << side << ", " << tradeCount << ", [";
            }
            for (std::uint64_t i = 0; i < tradeCount; ++i) {
                std::uint64_t code = 0;
                if (!qtyCodeBits.bits(header.hotQtyBits, code)) return Status::CorruptData;
                std::int64_t qty = 0;
                if (code < hotQty.size()) {
                    qty = hotQty[static_cast<std::size_t>(code)];
                } else if (code == ((1u << header.hotQtyBits) >> 1u)) {
                    std::uint64_t rawQty = 0;
                    if (!qtyEscape.varint(rawQty)) return Status::CorruptData;
                    qty = static_cast<std::int64_t>(rawQty);
                    ++qtyEscapes;
                } else {
                    return Status::CorruptData;
                }
                if (encodedJsonOut != nullptr) {
                    if (i != 0u) *encodedJsonOut << ", ";
                    *encodedJsonOut << code;
                }
                if (jsonlOut != nullptr) {
                    std::int64_t priceValue{}, qtyValue{}, timeValue{};
                    if (!internal::multiplyI64(currentPrice, priceScale, priceValue)
                        || !internal::multiplyI64(qty, qtyScale, qtyValue)
                        || !internal::multiplyI64(ts, timeScale, timeValue)) return Status::CorruptData;
                    const auto row = "[" + std::to_string(priceValue) + "," + std::to_string(qtyValue)
                        + "," + std::to_string(side) + "," + std::to_string(timeValue) + "]";
                    if (!jsonlOut->append(row) || !jsonlOut->append(lineEnding)) return jsonlOut->status;
                }
                ++records;
            }
            if (encodedJsonOut != nullptr) {
                *encodedJsonOut << "]]";
            }
            ++priceGroups;
        }
        if (encodedJsonOut != nullptr) *encodedJsonOut << "]]";
    }
    if (encodedJsonOut != nullptr) *encodedJsonOut << "\n      ]\n    ]";
    if (time.remaining() || price.remaining() || qtyEscape.remaining()
        || !sideBits.finished() || !dpZeroBits.finished() || !countOneBits.finished()
        || !priceGroupCountOneBits.finished() || !qtyCodeBits.finished()) return Status::CorruptData;
    if (records != header.recordCount || priceGroups != header.priceGroupCount || qtyEscapes != header.qtyEscapeCount) return Status::CorruptData;
    return Status::Ok;
}

Status walkSource(const internal::DecodeSource& file,
                const DecodedBlockCallback* onJsonl,
                std::ostream* encodedJsonOut,
                std::ostream* binaryDumpOut,
                FileHeader* parsedHeader) {
    if (file.size() < kFileHeaderBytes) return Status::CorruptData;
    std::array<std::uint8_t, kFileHeaderBytes> headerBytes{};
    if (!file.read(0u, headerBytes)) return Status::CorruptData;
    FileHeader header{};
    if (!parseFileHeader(headerBytes.data(), headerBytes.size(), header) || !validHeader(header)) return Status::CorruptData;
    if (header.headerCrc32c != headerCrc32c(header)) return Status::CorruptData;
    if (header.outputBytes != file.size() || header.chunkCount > (file.size() - kFileHeaderBytes) / kChunkHeaderBytes) return Status::CorruptData;
    if (parsedHeader != nullptr) *parsedHeader = header;
    if (encodedJsonOut != nullptr) {
        *encodedJsonOut << "{\n"
                        << "  \"schema\": {\n"
                        << "    \"chunk\": \"[chunk_index, record_count, encoded]\",\n"
                        << "    \"encoded\": \"[[base_ts, base_price, hot_qty], [time_group...]]\",\n"
                        << "    \"time_group\": \"[dt, [[dp, side, count, qty_codes]...]]\"\n"
                        << "  },\n"
                        << "  \"chunks\": [\n";
    }
    if (binaryDumpOut != nullptr) {
        *binaryDumpOut << "{\"file_header_bytes\":" << kFileHeaderBytes << ",\"chunks\":[";
    }
    const DecodedBlockCallback ignore = [](auto) { return true; };
    internal::DecodeOutput output(onJsonl == nullptr ? ignore : *onJsonl);
    std::uint64_t offset = kFileHeaderBytes;
    std::uint64_t records = 0, timeGroups = 0, priceGroups = 0, qtyEscapes = 0;
    auto buffer = std::make_unique<std::array<std::uint8_t, 65536>>();
    for (std::uint64_t chunkIndex = 0; chunkIndex < header.chunkCount; ++chunkIndex) {
        if (!file.contains(offset, kChunkHeaderBytes)) return Status::CorruptData;
        std::array<std::uint8_t, kChunkHeaderBytes> chunkBytes{};
        if (!file.read(offset, chunkBytes)) return Status::CorruptData;
        ChunkHeader chunk{};
        if (!parseChunkHeader(chunkBytes.data(), chunkBytes.size(), chunk) || !validChunkHeader(chunk)) return Status::CorruptData;
        const auto chunkStart = offset;
        offset += kChunkHeaderBytes;
        if (chunk.recordCount > header.chunkRecords || records > header.recordCount
            || chunk.recordCount > header.recordCount - records || chunk.hotQtyTableBytes > chunk.hotQtyCount * 10u
            || chunk.hotQtyCount > ((1u << chunk.hotQtyBits) >> 1u)
            || chunk.hotQtyBits == 0u || chunk.priceScale <= 0 || chunk.qtyScale <= 0 || chunk.timeScale <= 0
            || chunk.timeGroupCount > chunk.priceGroupCount || chunk.priceGroupCount > chunk.recordCount) return Status::CorruptData;
        const std::uint64_t streamsSize = static_cast<std::uint64_t>(chunk.timeStreamBytes)
            + chunk.priceStreamBytes + chunk.sideStreamBytes + chunk.dpZeroStreamBytes + chunk.countOneStreamBytes
            + chunk.priceGroupCountOneStreamBytes + chunk.qtyCodeStreamBytes + chunk.qtyEscapeStreamBytes;
        const auto payloadSize = chunk.hotQtyTableBytes + streamsSize;
        if (!file.contains(offset, payloadSize)) return Status::CorruptData;
        auto payload = file.cursor(offset, payloadSize);
        if (!payload) return Status::CorruptData;
        std::uint32_t crc = 0xffffffffu;
        while (payload->remaining()) {
            auto bytes = std::span{*buffer}.first(static_cast<std::size_t>(std::min<std::uint64_t>(payload->remaining(), buffer->size())));
            if (!payload->read(bytes)) return Status::CorruptData;
            crc = format::updateCrc32c(crc, bytes);
        }
        if (~crc != chunk.payloadCrc32c) return Status::CorruptData;
        auto table = file.cursor(offset, chunk.hotQtyTableBytes);
        if (!table) return Status::CorruptData;
        std::array<std::int64_t, 128> hotQty{};
        for (std::uint32_t i = 0; i < chunk.hotQtyCount; ++i) {
            std::uint64_t qty{};
            if (!table->varint(qty)) return Status::CorruptData;
            hotQty[i] = static_cast<std::int64_t>(qty);
        }
        if (table->remaining()) return Status::CorruptData;
        offset += chunk.hotQtyTableBytes;
        auto next = [&](std::uint32_t count) {
            auto input = file.cursor(offset, count); offset += count; return input;
        };
        auto time = next(chunk.timeStreamBytes), price = next(chunk.priceStreamBytes), side = next(chunk.sideStreamBytes);
        auto dpZero = next(chunk.dpZeroStreamBytes), countOne = next(chunk.countOneStreamBytes);
        auto priceGroupCountOne = next(chunk.priceGroupCountOneStreamBytes), qtyCode = next(chunk.qtyCodeStreamBytes);
        auto qtyEscape = next(chunk.qtyEscapeStreamBytes);
        if (!time || !price || !side || !dpZero || !countOne || !priceGroupCountOne || !qtyCode || !qtyEscape) return Status::CorruptData;
        if (encodedJsonOut != nullptr) {
            if (chunkIndex != 0u) *encodedJsonOut << ",\n";
            *encodedJsonOut << "    [" << chunkIndex << ", " << chunk.recordCount << ", ";
        }
        const auto decodeStatus = decodeChunk(chunk, {hotQty.data(), chunk.hotQtyCount},
            *time, *price, *side, *dpZero, *countOne, *priceGroupCountOne, *qtyCode, *qtyEscape,
            header.lineEnding == 2u ? std::string_view{"\r\n"} : std::string_view{"\n"},
            onJsonl == nullptr ? nullptr : &output, encodedJsonOut);
        if (!isOk(decodeStatus)) return decodeStatus;
        if (encodedJsonOut != nullptr) *encodedJsonOut << "\n    ]";
        if (binaryDumpOut != nullptr) {
            if (chunkIndex != 0u) *binaryDumpOut << ',';
            *binaryDumpOut << "{\"chunk\":" << chunkIndex
                           << ",\"chunk_header_offset\":" << chunkStart
                           << ",\"time_stream_bytes\":" << chunk.timeStreamBytes
                           << ",\"price_stream_bytes\":" << chunk.priceStreamBytes
                           << ",\"side_stream_bytes\":" << chunk.sideStreamBytes
                           << ",\"dp_zero_stream_bytes\":" << chunk.dpZeroStreamBytes
                           << ",\"count_one_stream_bytes\":" << chunk.countOneStreamBytes
                           << ",\"qty_code_stream_bytes\":" << chunk.qtyCodeStreamBytes
                           << ",\"qty_escape_stream_bytes\":" << chunk.qtyEscapeStreamBytes
                           << ",\"checksum\":" << chunk.payloadCrc32c << '}';
        }
        records += chunk.recordCount;
        timeGroups += chunk.timeGroupCount; priceGroups += chunk.priceGroupCount; qtyEscapes += chunk.qtyEscapeCount;

    }
    if (offset != file.size()) return Status::CorruptData;
    if (records != header.recordCount || timeGroups != header.timeGroupCount || priceGroups != header.priceGroupCount
        || qtyEscapes != header.qtyEscapeCount || (onJsonl && output.produced != header.inputBytes)) return Status::CorruptData;
    if (onJsonl && !output.flush()) return output.status;
    if (encodedJsonOut != nullptr) *encodedJsonOut << "\n  ]\n}\n";
    if (binaryDumpOut != nullptr) *binaryDumpOut << "]}\n";
    return Status::Ok;
}
Status walkFile(std::span<const std::uint8_t> file, const DecodedBlockCallback* onJsonl,
                std::ostream* encodedJsonOut, std::ostream* binaryDumpOut, FileHeader* parsedHeader) noexcept {
    try {
        internal::SpanDecodeSource source(file);
        return walkSource(source, onJsonl, encodedJsonOut, binaryDumpOut, parsedHeader);
    } catch (...) { return Status::DecodeError; }
}
}  // namespace codec_detail

namespace hft_compressor::codecs::trades_grouped_delta_qtydict {
using namespace codec_detail;


Status decodeSource(const internal::DecodeSource& source, const DecodedBlockCallback& onBlock) {
    if (!onBlock) return Status::InvalidArgument;
    return walkSource(source, &onBlock, nullptr, nullptr, nullptr);
}

Status decode(std::span<const std::uint8_t> file, const DecodedBlockCallback& onBlock) noexcept {
    if (!onBlock) return Status::InvalidArgument;
    try {
        internal::SpanDecodeSource source(file);
        const DecodedBlockCallback validate = [](auto) { return true; };
        auto status = decodeSource(source, validate);
        if (!isOk(status)) return status;
        if (!source.unchanged()) return Status::CorruptData;
        status = decodeSource(source, onBlock);
        return !source.unchanged() ? Status::CorruptData : status;
    } catch (...) { return Status::DecodeError; }
}

Status decodeFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept {
    if (path.empty() || !onBlock) return Status::InvalidArgument;
    try {
        internal::FileDecodeSource source(path);
        if (!source.valid()) return Status::IoError;
        const DecodedBlockCallback validate = [](auto) { return true; };
        auto status = decodeSource(source, validate);
        if (!isOk(status)) return status;
        if (!source.unchanged()) return Status::CorruptData;
        status = decodeSource(source, onBlock);
        return !source.unchanged() ? Status::CorruptData : status;
    } catch (...) { return Status::DecodeError; }
}
}  // namespace hft_compressor::codecs::trades_grouped_delta_qtydict

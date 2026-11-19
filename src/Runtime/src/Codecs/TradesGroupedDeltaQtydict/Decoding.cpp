#include "TradesGroupedDeltaQtyDictInternal.hpp"
#include "../BaseDecode.hpp"
#include "../ByteBlock/BinaryTransform.hpp"

#include <bit>

namespace hft_compressor::codecs::trades_grouped_delta_qtydict::codec_detail {

bool readFile(const std::filesystem::path& path, std::vector<std::uint8_t>& out) {
    return internal::readFileBytes(path, out);
}

Status decodeChunk(const ByteBlockLayout& layout,
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
                   internal::DecodeCursor& residual,
                   std::vector<std::uint8_t>* chunkRecordsOut,
                   std::ostream* encodedJsonOut) {
    internal::DecodeBits sideBits{side}, dpZeroBits{dpZero}, countOneBits{countOne};
    internal::DecodeBits priceGroupCountOneBits{priceGroupCountOne}, qtyCodeBits{qtyCode};
    const auto priceScale = header.priceScale;
    const auto qtyScale = header.qtyScale;
    const auto timeScale = header.timeScale;
    std::int64_t ts = header.baseTsUnit;
    std::int64_t previousPrice = header.basePriceTick;
    std::uint32_t records = 0u;
    std::uint32_t priceGroups = 0u;
    std::uint32_t qtyEscapes = 0u;

    std::vector<Trade> decodedTrades;
    if (chunkRecordsOut != nullptr) decodedTrades.reserve(header.recordCount);

    if (encodedJsonOut != nullptr) {
        *encodedJsonOut << "[\n      [" << header.baseTsNs << ", " << header.basePrice << ", [";
        for (std::size_t i = 0u; i < hotQty.size(); ++i) {
            if (i != 0u) *encodedJsonOut << ", ";
            *encodedJsonOut << hotQty[i];
        }
        *encodedJsonOut << "]],\n      [\n";
    }

    for (std::uint32_t tg = 0u; tg < header.timeGroupCount; ++tg) {
        std::uint64_t rawDt = 0u;
        std::uint64_t groupCount = 0u;
        if (!time.varint(rawDt)) return Status::CorruptData;
        const std::int64_t dt = internal::decodeZigzag(rawDt);
        std::uint64_t oneGroup = 0u;
        if (!priceGroupCountOneBits.bits(1u, oneGroup)) return Status::CorruptData;
        if (oneGroup != 0u) groupCount = 1u;
        else if (!time.varint(groupCount)) return Status::CorruptData;

        ts = std::bit_cast<std::int64_t>(
            std::bit_cast<std::uint64_t>(ts) + std::bit_cast<std::uint64_t>(dt));
        if (groupCount == 0u || groupCount > header.priceGroupCount - priceGroups) return Status::CorruptData;

        if (encodedJsonOut != nullptr) {
            if (tg != 0u) *encodedJsonOut << ",\n";
            *encodedJsonOut << "        [" << dt << ", [";
        }
        for (std::uint64_t pg = 0u; pg < groupCount; ++pg) {
            std::uint64_t zz = 0u;
            std::uint8_t sideVal = 0u;
            std::uint64_t tradeCount = 0u;
            std::int64_t dp = 0;
            if (header.sideMode == 0u) {
                std::uint64_t sBit = 0u;
                if (!sideBits.bits(1u, sBit)) return Status::CorruptData;
                sideVal = static_cast<std::uint8_t>(sBit);
            } else {
                if (!side.byte(sideVal)) return Status::CorruptData;
            }
            std::uint64_t dpZero = 0u;
            std::uint64_t countOne = 0u;
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
            if (tradeCount == 0u || tradeCount > header.recordCount - records) return Status::CorruptData;
            const auto currentPrice = std::bit_cast<std::int64_t>(
                std::bit_cast<std::uint64_t>(previousPrice) + std::bit_cast<std::uint64_t>(dp));
            previousPrice = currentPrice;

            if (encodedJsonOut != nullptr) {
                if (pg != 0u) *encodedJsonOut << ", ";
                *encodedJsonOut << '[' << dp << ", " << static_cast<unsigned>(sideVal) << ", " << tradeCount << ", [";
            }
            for (std::uint64_t i = 0u; i < tradeCount; ++i) {
                std::uint64_t code = 0u;
                if (!qtyCodeBits.bits(header.hotQtyBits, code)) return Status::CorruptData;
                std::int64_t qty = 0;
                if (code < hotQty.size()) {
                    qty = hotQty[static_cast<std::size_t>(code)];
                } else if (code == ((1u << header.hotQtyBits) >> 1u)) {
                    std::uint64_t rawQty = 0u;
                    if (!qtyEscape.varint(rawQty)) return Status::CorruptData;
                    qty = internal::decodeZigzag(rawQty);
                    ++qtyEscapes;
                } else {
                    return Status::CorruptData;
                }
                if (encodedJsonOut != nullptr) {
                    if (i != 0u) *encodedJsonOut << ", ";
                    *encodedJsonOut << code;
                }
                if (chunkRecordsOut != nullptr) {
                    std::int64_t priceValue{}, qtyValue{}, timeValue{};
                    if (!internal::multiplyI64(currentPrice, priceScale, priceValue)
                        || !internal::multiplyI64(qty, qtyScale, qtyValue)
                        || !internal::multiplyI64(ts, timeScale, timeValue)) {
                        return Status::CorruptData;
                    }
                    decodedTrades.push_back(Trade{priceValue, qtyValue, sideVal, timeValue});
                }
                ++records;
            }
            if (encodedJsonOut != nullptr) *encodedJsonOut << "]]";
            ++priceGroups;
        }
        if (encodedJsonOut != nullptr) *encodedJsonOut << "]]";
    }
    if (encodedJsonOut != nullptr) *encodedJsonOut << "\n      ]\n    ]";

    const bool sideDone = (header.sideMode == 0u) ? sideBits.finished() : (side.remaining() == 0u);
    if (time.remaining() || price.remaining() || qtyEscape.remaining()
        || !sideDone || !dpZeroBits.finished() || !countOneBits.finished()
        || !priceGroupCountOneBits.finished() || !qtyCodeBits.finished()) {
        return Status::CorruptData;
    }
    if (records != header.recordCount || priceGroups != header.priceGroupCount || qtyEscapes != header.qtyEscapeCount) {
        return Status::CorruptData;
    }

    if (chunkRecordsOut != nullptr) {
        const std::size_t stride = layout.recordBytes;
        const std::size_t count = header.recordCount;
        chunkRecordsOut->assign(count * stride, 0u);
        for (std::size_t col = 0u; col < stride; ++col) {
            std::uint8_t prevByte = 0u;
            for (std::size_t row = 0u; row < count; ++row) {
                std::uint8_t d{};
                if (!residual.byte(d)) return Status::CorruptData;
                prevByte = static_cast<std::uint8_t>(prevByte + d);
                (*chunkRecordsOut)[row * stride + col] = prevByte;
            }
        }
        if (residual.remaining() != 0u) return Status::CorruptData;

        for (std::size_t row = 0u; row < count; ++row) {
            const std::size_t baseOff = row * stride;
            const std::size_t pOff = baseOff + layout.payloadOffset;
            const std::size_t tOff = baseOff + layout.timestampOffset;
            if (byte_block::word(*chunkRecordsOut, pOff, 8u) != 0u ||
                byte_block::word(*chunkRecordsOut, pOff + 8u, 8u) != 0u ||
                (*chunkRecordsOut)[pOff + 16u] != 0u ||
                byte_block::word(*chunkRecordsOut, tOff, 8u) != 0u) {
                return Status::CorruptData;
            }
            byte_block::put(*chunkRecordsOut, pOff, std::bit_cast<std::uint64_t>(decodedTrades[row].price), 8u);
            byte_block::put(*chunkRecordsOut, pOff + 8u, std::bit_cast<std::uint64_t>(decodedTrades[row].qty), 8u);
            (*chunkRecordsOut)[pOff + 16u] = static_cast<std::uint8_t>(decodedTrades[row].side & 0xff);
            byte_block::put(*chunkRecordsOut, tOff, std::bit_cast<std::uint64_t>(decodedTrades[row].tsNs), 8u);
        }
    }
    return Status::Ok;
}

Status walkSource(const internal::DecodeSource& file,
                  const DecodedBlockCallback* onSlice,
                  std::ostream* encodedJsonOut,
                  std::ostream* binaryDumpOut,
                  FileHeader* parsedHeader) {
    if (file.size() < kFileHeaderBytes) return Status::CorruptData;
    std::array<std::uint8_t, kFileHeaderBytes> headerBytes{};
    if (!file.read(0u, headerBytes)) return Status::CorruptData;
    FileHeader header{};
    if (!parseFileHeader(headerBytes.data(), headerBytes.size(), header) || !validHeader(header)) {
        return Status::CorruptData;
    }
    if (header.headerCrc32c != headerCrc32c(header)) return Status::CorruptData;
    if (header.outputBytes != file.size() ||
        header.recordCount == 0u ||
        header.recordCount > std::numeric_limits<std::uint32_t>::max() ||
        header.inputBytes != kBinarySliceHeaderBytes + header.recordCount * header.layout.recordBytes ||
        static_cast<unsigned __int128>(header.chunkCount) * kChunkHeaderBytes > file.size() - kFileHeaderBytes) {
        return Status::CorruptData;
    }
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

    std::uint64_t offset = kFileHeaderBytes;
    std::uint64_t records = 0u, timeGroups = 0u, priceGroups = 0u, qtyEscapes = 0u;
    auto buffer = std::make_unique<std::array<std::uint8_t, 65536u>>();
    std::vector<std::vector<std::uint8_t>> decodedChunks;
    std::uint32_t runningRecordsCrc = 0xffffffffu;

    for (std::uint64_t chunkIndex = 0u; chunkIndex < header.chunkCount; ++chunkIndex) {
        if (!file.contains(offset, kChunkHeaderBytes)) return Status::CorruptData;
        std::array<std::uint8_t, kChunkHeaderBytes> chunkBytes{};
        if (!file.read(offset, chunkBytes)) return Status::CorruptData;
        ChunkHeader chunk{};
        if (!parseChunkHeader(chunkBytes.data(), chunkBytes.size(), chunk) || !validChunkHeader(chunk)) {
            return Status::CorruptData;
        }
        const auto chunkStart = offset;
        offset += kChunkHeaderBytes;
        if (chunk.recordCount > header.chunkRecords || records > header.recordCount
            || chunk.recordCount > header.recordCount - records
            || chunk.hotQtyTableBytes > chunk.hotQtyCount * 10u
            || chunk.hotQtyCount > ((1u << chunk.hotQtyBits) >> 1u)
            || chunk.hotQtyBits == 0u || chunk.priceScale <= 0 || chunk.qtyScale <= 0 || chunk.timeScale <= 0
            || chunk.timeGroupCount > chunk.priceGroupCount || chunk.priceGroupCount > chunk.recordCount
            || chunk.residualStreamBytes != static_cast<std::uint64_t>(chunk.recordCount) * header.layout.recordBytes) {
            return Status::CorruptData;
        }
        const std::uint64_t streamsSize = static_cast<std::uint64_t>(chunk.timeStreamBytes)
            + chunk.priceStreamBytes + chunk.sideStreamBytes + chunk.dpZeroStreamBytes + chunk.countOneStreamBytes
            + chunk.priceGroupCountOneStreamBytes + chunk.qtyCodeStreamBytes + chunk.qtyEscapeStreamBytes
            + chunk.residualStreamBytes;
        const auto payloadSize = chunk.hotQtyTableBytes + streamsSize;
        if (!file.contains(offset, payloadSize)) return Status::CorruptData;
        auto payload = file.cursor(offset, payloadSize);
        if (!payload) return Status::CorruptData;
        std::uint32_t crc = 0xffffffffu;
        while (payload->remaining()) {
            auto bytes = std::span{*buffer}.first(
                static_cast<std::size_t>(std::min<std::uint64_t>(payload->remaining(), buffer->size())));
            if (!payload->read(bytes)) return Status::CorruptData;
            crc = format::updateCrc32c(crc, bytes);
        }
        if (~crc != chunk.payloadCrc32c) return Status::CorruptData;

        auto table = file.cursor(offset, chunk.hotQtyTableBytes);
        if (!table) return Status::CorruptData;
        std::array<std::int64_t, 128u> hotQty{};
        for (std::uint32_t i = 0u; i < chunk.hotQtyCount; ++i) {
            std::uint64_t rawQty{};
            if (!table->varint(rawQty)) return Status::CorruptData;
            hotQty[i] = internal::decodeZigzag(rawQty);
        }
        if (table->remaining()) return Status::CorruptData;
        offset += chunk.hotQtyTableBytes;

        auto next = [&](std::uint32_t count) {
            auto input = file.cursor(offset, count);
            offset += count;
            return input;
        };
        auto time = next(chunk.timeStreamBytes);
        auto price = next(chunk.priceStreamBytes);
        auto side = next(chunk.sideStreamBytes);
        auto dpZero = next(chunk.dpZeroStreamBytes);
        auto countOne = next(chunk.countOneStreamBytes);
        auto priceGroupCountOne = next(chunk.priceGroupCountOneStreamBytes);
        auto qtyCode = next(chunk.qtyCodeStreamBytes);
        auto qtyEscape = next(chunk.qtyEscapeStreamBytes);
        auto residual = next(chunk.residualStreamBytes);
        if (!time || !price || !side || !dpZero || !countOne ||
            !priceGroupCountOne || !qtyCode || !qtyEscape || !residual) {
            return Status::CorruptData;
        }
        if (encodedJsonOut != nullptr) {
            if (chunkIndex != 0u) *encodedJsonOut << ",\n";
            *encodedJsonOut << "    [" << chunkIndex << ", " << chunk.recordCount << ", ";
        }
        std::vector<std::uint8_t> chunkRecords;
        const auto decodeStatus = decodeChunk(
            header.layout,
            chunk,
            {hotQty.data(), chunk.hotQtyCount},
            *time, *price, *side, *dpZero, *countOne, *priceGroupCountOne, *qtyCode, *qtyEscape, *residual,
            onSlice == nullptr ? nullptr : &chunkRecords,
            encodedJsonOut);
        if (!isOk(decodeStatus)) return decodeStatus;
        if (onSlice != nullptr) {
            runningRecordsCrc = format::updateCrc32c(runningRecordsCrc, chunkRecords);
            decodedChunks.push_back(std::move(chunkRecords));
        }
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
                           << ",\"residual_stream_bytes\":" << chunk.residualStreamBytes
                           << ",\"checksum\":" << chunk.payloadCrc32c << '}';
        }
        records += chunk.recordCount;
        timeGroups += chunk.timeGroupCount;
        priceGroups += chunk.priceGroupCount;
        qtyEscapes += chunk.qtyEscapeCount;
    }

    if (offset != file.size()) return Status::CorruptData;
    if (records != header.recordCount || timeGroups != header.timeGroupCount ||
        priceGroups != header.priceGroupCount || qtyEscapes != header.qtyEscapeCount) {
        return Status::CorruptData;
    }

    if (onSlice != nullptr) {
        if (~runningRecordsCrc != header.recordsCrc32c || !file.unchanged()) {
            return Status::CorruptData;
        }
        // Build exact HBS\0 header without concatenating all chunks in memory.
        std::array<std::uint8_t, kBinarySliceHeaderBytes> sliceHdr{};
        byte_block::put(sliceHdr, 0u, kBinarySliceMagic, 4u);
        byte_block::put(sliceHdr, 4u, static_cast<std::uint32_t>(header.layout.streamType), 4u);
        byte_block::put(sliceHdr, 8u, header.layout.recordBytes, 4u);
        byte_block::put(sliceHdr, 12u, header.layout.payloadOffset, 4u);
        byte_block::put(sliceHdr, 16u, header.layout.timestampOffset, 4u);
        byte_block::put(sliceHdr, 20u, header.layout.depthLevelsOffset, 4u);
        byte_block::put(sliceHdr, 24u, header.layout.depthLevelStride, 4u);
        byte_block::put(sliceHdr, 28u, header.layout.depthLevelCountOffset, 4u);
        byte_block::put(sliceHdr, 32u, header.layout.depthLevelCapacity, 4u);
        byte_block::put(sliceHdr, 36u, static_cast<std::uint32_t>(header.recordCount), 4u);
        byte_block::put(sliceHdr, 40u, header.recordsCrc32c, 4u);
        byte_block::put(sliceHdr, 44u, format::crc32c(sliceHdr), 4u);

        if (!(*onSlice)(sliceHdr)) return Status::CallbackStopped;
        for (const auto& chunkVec : decodedChunks) {
            for (std::size_t pos = 0u; pos < chunkVec.size(); pos += 65536u) {
                const std::size_t take = std::min<std::size_t>(65536u, chunkVec.size() - pos);
                if (!(*onSlice)({chunkVec.data() + pos, take})) return Status::CallbackStopped;
            }
        }
    }

    if (encodedJsonOut != nullptr) *encodedJsonOut << "\n  ]\n}\n";
    if (binaryDumpOut != nullptr) *binaryDumpOut << "]}\n";
    return Status::Ok;
}

Status walkFile(std::span<const std::uint8_t> file,
                const DecodedBlockCallback* onSlice,
                std::ostream* encodedJsonOut,
                std::ostream* binaryDumpOut,
                FileHeader* parsedHeader) noexcept {
    try {
        internal::SpanDecodeSource source(file);
        return walkSource(source, onSlice, encodedJsonOut, binaryDumpOut, parsedHeader);
    } catch (...) {
        return Status::DecodeError;
    }
}

}  // namespace hft_compressor::codecs::trades_grouped_delta_qtydict::codec_detail

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
        const auto status = decodeSource(source, onBlock);
        return !source.unchanged() ? Status::CorruptData : status;
    } catch (...) {
        return Status::DecodeError;
    }
}

Status decodeFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept {
    if (path.empty() || !onBlock) return Status::InvalidArgument;
    try {
        internal::FileDecodeSource source(path);
        if (!source.valid()) return Status::IoError;
        const auto status = decodeSource(source, onBlock);
        return !source.unchanged() ? Status::CorruptData : status;
    } catch (...) {
        return Status::DecodeError;
    }
}

}  // namespace hft_compressor::codecs::trades_grouped_delta_qtydict

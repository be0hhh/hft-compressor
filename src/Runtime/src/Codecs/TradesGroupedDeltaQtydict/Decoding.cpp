#include "TradesGroupedDeltaQtyDictInternal.hpp"

namespace hft_compressor::codecs::trades_grouped_delta_qtydict::codec_detail {


bool readFile(const std::filesystem::path& path, std::vector<std::uint8_t>& out) noexcept {
    return internal::readFileBytes(path, out);
}

Status decodeChunk(
                   const ChunkHeader& header,
                   std::span<const std::int64_t> hotQty,
                   std::span<const std::uint8_t> timeStream,
                   std::span<const std::uint8_t> priceStream,
                   std::span<const std::uint8_t> sideStream,
                   std::span<const std::uint8_t> dpZeroStream,
                   std::span<const std::uint8_t> countOneStream,
                   std::span<const std::uint8_t> priceGroupCountOneStream,
                   std::span<const std::uint8_t> qtyCodeStream,
                   std::span<const std::uint8_t> qtyEscapeStream,
                   std::string_view lineEnding,
                   std::string* jsonlOut,
                   std::ostream* encodedJsonOut) noexcept {
    const auto* time = timeStream.data();
    const auto* timeEnd = timeStream.data() + timeStream.size();
    const auto* price = priceStream.data();
    const auto* priceEnd = priceStream.data() + priceStream.size();
    const auto* qtyEscape = qtyEscapeStream.data();
    const auto* qtyEscapeEnd = qtyEscapeStream.data() + qtyEscapeStream.size();
    BitReader sideBits{sideStream.data(), sideStream.size()};
    BitReader dpZeroBits{dpZeroStream.data(), dpZeroStream.size()};
    BitReader countOneBits{countOneStream.data(), countOneStream.size()};
    BitReader priceGroupCountOneBits{priceGroupCountOneStream.data(), priceGroupCountOneStream.size()};
    BitReader qtyCodeBits{qtyCodeStream.data(), qtyCodeStream.size()};

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
        if (!readVarint(time, timeEnd, dt)) return Status::CorruptData;
        std::uint64_t oneGroup = 0;
        if (!priceGroupCountOneBits.readBits(1u, oneGroup)) return Status::CorruptData;
        if (oneGroup != 0u) groupCount = 1u;
        else if (!readVarint(time, timeEnd, groupCount)) return Status::CorruptData;
        ts += static_cast<std::int64_t>(dt);
        if (encodedJsonOut != nullptr) {
            if (tg != 0u) *encodedJsonOut << ",\n";
            *encodedJsonOut << "        [" << dt << ", [";
        }
        for (std::uint64_t pg = 0; pg < groupCount; ++pg) {
            std::uint64_t zz = 0;
            std::uint64_t side = 0;
            std::uint64_t tradeCount = 0;
            std::int64_t dp = 0;
            if (!sideBits.readBits(1u, side)) return Status::CorruptData;
            std::uint64_t dpZero = 0;
            std::uint64_t countOne = 0;
            if (!dpZeroBits.readBits(1u, dpZero) || !countOneBits.readBits(1u, countOne)) return Status::CorruptData;
            if (dpZero == 0u) {
                if (!readVarint(price, priceEnd, zz)) return Status::CorruptData;
                dp = unzigzag(zz);
            }
            if (countOne != 0u) {
                tradeCount = 1u;
            } else if (!readVarint(price, priceEnd, tradeCount)) {
                return Status::CorruptData;
            } else {
                tradeCount += 2u;
            }
            if (side > 1u || tradeCount == 0u) return Status::CorruptData;
            const auto currentPrice = previousPrice + dp;
            previousPrice = currentPrice;
            if (encodedJsonOut != nullptr) {
                if (pg != 0u) *encodedJsonOut << ", ";
                *encodedJsonOut << '[' << dp << ", " << side << ", " << tradeCount << ", [";
            }
            for (std::uint64_t i = 0; i < tradeCount; ++i) {
                std::uint64_t code = 0;
                if (!qtyCodeBits.readBits(header.hotQtyBits, code)) return Status::CorruptData;
                std::int64_t qty = 0;
                if (code < hotQty.size()) {
                    qty = hotQty[static_cast<std::size_t>(code)];
                } else if (code == ((1u << header.hotQtyBits) >> 1u)) {
                    std::uint64_t rawQty = 0;
                    if (!readVarint(qtyEscape, qtyEscapeEnd, rawQty)) return Status::CorruptData;
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
                    jsonlOut->append("[");
                    jsonlOut->append(std::to_string(currentPrice * priceScale));
                    jsonlOut->append(",");
                    jsonlOut->append(std::to_string(qty * qtyScale));
                    jsonlOut->append(",");
                    jsonlOut->append(std::to_string(side));
                    jsonlOut->append(",");
                    jsonlOut->append(std::to_string(ts * timeScale));
                    jsonlOut->append("]");
                    jsonlOut->append(lineEnding);
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
    if (time != timeEnd || price != priceEnd || qtyEscape != qtyEscapeEnd) return Status::CorruptData;
    if (records != header.recordCount || priceGroups != header.priceGroupCount || qtyEscapes != header.qtyEscapeCount) return Status::CorruptData;
    return Status::Ok;
}

Status walkFile(std::span<const std::uint8_t> file,
                const DecodedBlockCallback* onJsonl,
                std::ostream* encodedJsonOut,
                std::ostream* binaryDumpOut,
                FileHeader* parsedHeader) noexcept {
    if (file.size() < kFileHeaderBytes) return Status::InvalidArgument;
    FileHeader header{};
    if (!parseFileHeader(file.data(), file.size(), header) || !validHeader(header)) return Status::CorruptData;
    if (header.headerCrc32c != headerCrc32c(header)) return Status::CorruptData;
    if (header.outputBytes != 0u && header.outputBytes != file.size()) return Status::CorruptData;
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
    std::size_t offset = kFileHeaderBytes;
    std::uint64_t records = 0;
    for (std::uint64_t chunkIndex = 0; chunkIndex < header.chunkCount; ++chunkIndex) {
        if (file.size() - offset < kChunkHeaderBytes) return Status::CorruptData;
        ChunkHeader chunk{};
        if (!parseChunkHeader(file.data() + offset, file.size() - offset, chunk) || !validChunkHeader(chunk)) return Status::CorruptData;
        const auto chunkStart = offset;
        offset += kChunkHeaderBytes;
        std::size_t payloadStart = offset;
        std::vector<std::int64_t> hotQty;
        hotQty.reserve(chunk.hotQtyCount);
        if (file.size() - offset < chunk.hotQtyTableBytes) return Status::CorruptData;
        const auto* hotQtyPtr = file.data() + offset;
        const auto* hotQtyEnd = hotQtyPtr + chunk.hotQtyTableBytes;
        for (std::uint32_t i = 0; i < chunk.hotQtyCount; ++i) {
            std::uint64_t qty = 0;
            if (!readVarint(hotQtyPtr, hotQtyEnd, qty)) return Status::CorruptData;
            hotQty.push_back(static_cast<std::int64_t>(qty));
        }
        if (hotQtyPtr != hotQtyEnd) return Status::CorruptData;
        offset += chunk.hotQtyTableBytes;
        const auto streamsSize = static_cast<std::size_t>(chunk.timeStreamBytes)
            + chunk.priceStreamBytes + chunk.sideStreamBytes + chunk.dpZeroStreamBytes + chunk.countOneStreamBytes
            + chunk.priceGroupCountOneStreamBytes + chunk.qtyCodeStreamBytes + chunk.qtyEscapeStreamBytes;
        if (file.size() - offset < streamsSize) return Status::CorruptData;
        const auto payloadSize = (offset - payloadStart) + streamsSize;
        const auto* payloadData = file.data() + payloadStart;
        std::vector<std::uint8_t> payload;
        payload.insert(payload.end(), payloadData, payloadData + payloadSize);
        if (format::crc32c(payload) != chunk.payloadCrc32c) return Status::CorruptData;

        std::size_t streamOffset = offset;
        std::span<const std::uint8_t> timeStream{file.data() + streamOffset, chunk.timeStreamBytes};
        streamOffset += chunk.timeStreamBytes;
        std::span<const std::uint8_t> priceStream{file.data() + streamOffset, chunk.priceStreamBytes};
        streamOffset += chunk.priceStreamBytes;
        std::span<const std::uint8_t> sideStream{file.data() + streamOffset, chunk.sideStreamBytes};
        streamOffset += chunk.sideStreamBytes;
        std::span<const std::uint8_t> dpZeroStream{file.data() + streamOffset, chunk.dpZeroStreamBytes};
        streamOffset += chunk.dpZeroStreamBytes;
        std::span<const std::uint8_t> countOneStream{file.data() + streamOffset, chunk.countOneStreamBytes};
        streamOffset += chunk.countOneStreamBytes;
        std::span<const std::uint8_t> priceGroupCountOneStream{file.data() + streamOffset, chunk.priceGroupCountOneStreamBytes};
        streamOffset += chunk.priceGroupCountOneStreamBytes;
        std::span<const std::uint8_t> qtyCodeStream{file.data() + streamOffset, chunk.qtyCodeStreamBytes};
        streamOffset += chunk.qtyCodeStreamBytes;
        std::span<const std::uint8_t> qtyEscapeStream{file.data() + streamOffset, chunk.qtyEscapeStreamBytes};

        if (encodedJsonOut != nullptr) {
            if (chunkIndex != 0u) *encodedJsonOut << ",\n";
            *encodedJsonOut << "    [" << chunkIndex << ", " << chunk.recordCount << ", ";
        }
        std::string jsonl;
        jsonl.reserve(static_cast<std::size_t>(chunk.recordCount) * 32u);
        const auto decodeStatus = decodeChunk(
                                              chunk,
                                              {hotQty.data(), hotQty.size()},
                                              timeStream,
                                              priceStream,
                                              sideStream,
                                              dpZeroStream,
                                              countOneStream,
                                              priceGroupCountOneStream,
                                              qtyCodeStream,
                                              qtyEscapeStream,
                                              header.lineEnding == 2u ? std::string_view{"\r\n"} : std::string_view{"\n"},
                                              onJsonl == nullptr ? nullptr : &jsonl,
                                              encodedJsonOut);
        if (!isOk(decodeStatus)) return decodeStatus;
        if (encodedJsonOut != nullptr) *encodedJsonOut << "\n    ]";
        if (onJsonl != nullptr && !jsonl.empty()) {
            if (!(*onJsonl)(std::span<const std::uint8_t>{reinterpret_cast<const std::uint8_t*>(jsonl.data()), jsonl.size()})) return Status::Ok;
        }
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
        offset += streamsSize;
    }
    if (offset != file.size()) return Status::CorruptData;
    if (records != header.recordCount) return Status::CorruptData;
    if (encodedJsonOut != nullptr) *encodedJsonOut << "\n  ]\n}\n";
    if (binaryDumpOut != nullptr) *binaryDumpOut << "]}\n";
    return Status::Ok;
}
}  // namespace codec_detail

namespace hft_compressor::codecs::trades_grouped_delta_qtydict {
using namespace codec_detail;


Status decode(std::span<const std::uint8_t> file, const DecodedBlockCallback& onBlock) noexcept {
    if (!onBlock) return Status::InvalidArgument;
    return walkFile(file, &onBlock, nullptr, nullptr);
}

Status decodeFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept {
    if (path.empty() || !onBlock) return Status::InvalidArgument;
    std::vector<std::uint8_t> file;
    if (!readFile(path, file)) return Status::IoError;
    return decode(file, onBlock);
}
}  // namespace hft_compressor::codecs::trades_grouped_delta_qtydict

#include "TradesGroupedDeltaQtyDictInternal.hpp"
#include "../BaseDecode.hpp"
#include "../ByteBlock/BinaryTransform.hpp"

#include <bit>
#include <utility>

namespace hft_compressor::codecs::trades_grouped_delta_qtydict::codec_detail {

namespace {
std::size_t varintByteLength(std::uint64_t value) noexcept {
    std::size_t n = 1u;
    while (value >= 0x80u) {
        ++n;
        value >>= 7u;
    }
    return n;
}
} // namespace

// Flat sorted vector frequency ranking (zero std::unordered_map, zero runtime /).
void buildHotQty(const std::vector<Trade>& trades,
                 std::size_t begin,
                 std::size_t end,
                 EncodedChunk& chunk,
                 bool useCompactQuantityTable) {
    const auto qtyDiv = math::ExactScaleDivisor::prepare(chunk.qtyScale);
    std::vector<std::int64_t> scaledQtys;
    scaledQtys.reserve(end - begin);
    for (std::size_t i = begin; i < end; ++i) {
        scaledQtys.push_back(useCompactQuantityTable
            ? qtyDiv.divideSignedExact(trades[i].qty)
            : trades[i].qty);
    }
    std::sort(scaledQtys.begin(), scaledQtys.end());

    std::vector<std::pair<std::int64_t, std::uint32_t>> values;
    for (std::size_t i = 0u; i < scaledQtys.size();) {
        std::size_t j = i + 1u;
        while (j < scaledQtys.size() && scaledQtys[j] == scaledQtys[i]) ++j;
        values.push_back({scaledQtys[i], static_cast<std::uint32_t>(j - i)});
        i = j;
    }
    std::sort(values.begin(), values.end(), [](const auto& a, const auto& b) {
        if (a.second != b.second) return a.second > b.second;
        return a.first < b.first;
    });

    if (!useCompactQuantityTable) {
        chunk.hotQtyCapacity = kHotQtyCount;
        chunk.hotQtyBits = 7u;
        chunk.hotQtyCount = static_cast<std::uint32_t>(std::min<std::size_t>(values.size(), kHotQtyCount));
        chunk.hotQty.reserve(chunk.hotQtyCount);
        for (std::uint32_t i = 0u; i < chunk.hotQtyCount; ++i) chunk.hotQty.push_back(values[i].first);
        return;
    }

    constexpr std::array<std::uint32_t, 4u> capacities{16u, 32u, 64u, 128u};
    std::uint64_t bestCost = UINT64_MAX;
    std::uint32_t bestCapacity = 16u;
    for (const auto capacity : capacities) {
        const auto hotCount = std::min<std::size_t>(values.size(), capacity);
        std::uint64_t hotHits = 0u;
        for (std::size_t i = 0u; i < hotCount; ++i) hotHits += values[i].second;
        const auto escapes = static_cast<std::uint64_t>(end - begin) - hotHits;
        const auto bits = bitsForHotCapacity(capacity);
        std::uint64_t escapeBytes = 0u;
        for (std::size_t i = hotCount; i < values.size(); ++i) {
            escapeBytes += static_cast<std::uint64_t>(varintByteLength(zigzag(values[i].first))) * values[i].second;
        }
        const auto tableBytesEstimate = hotCount * 2u;
        const auto codeBytes = ((static_cast<std::uint64_t>(end - begin) * bits) + 7u) >> 3u;
        const auto cost = codeBytes + escapeBytes + tableBytesEstimate + escapes;
        if (cost < bestCost) {
            bestCost = cost;
            bestCapacity = capacity;
        }
    }

    chunk.hotQtyCapacity = bestCapacity;
    chunk.hotQtyBits = bitsForHotCapacity(bestCapacity);
    chunk.hotQtyCount = static_cast<std::uint32_t>(std::min<std::size_t>(values.size(), bestCapacity));
    chunk.hotQty.reserve(chunk.hotQtyCount);
    for (std::uint32_t i = 0u; i < chunk.hotQtyCount; ++i) chunk.hotQty.push_back(values[i].first);
}

std::uint64_t qtyCode(const EncodedChunk& chunk, std::int64_t qty) noexcept {
    for (std::uint32_t i = 0u; i < chunk.hotQtyCount; ++i) {
        if (chunk.hotQty[i] == qty) return i;
    }
    return chunk.hotQtyCapacity;
}

EncodedChunk encodeChunk(const ByteBlockLayout& layout,
                         std::span<const std::uint8_t> scratchRecords,
                         const std::vector<Trade>& trades,
                         std::size_t begin,
                         std::size_t end) {
    EncodedChunk chunk{};
    chunk.recordCount = static_cast<std::uint32_t>(end - begin);
    chunk.firstTsNs = trades[begin].tsNs;
    chunk.lastTsNs = trades[end - 1u].tsNs;
    chunk.baseTsNs = trades[begin].tsNs;
    chunk.basePrice = trades[begin].price;

    std::int64_t priceScale = 0;
    std::int64_t qtyScale = 0;
    std::int64_t timeScale = chunk.baseTsNs;
    bool binarySideOnly = true;
    bool priceDiffOk = true;
    for (std::size_t i = begin; i < end; ++i) {
        priceScale = gcdPositive(priceScale, trades[i].price);
        qtyScale = gcdPositive(qtyScale, trades[i].qty);
        std::int64_t tsDiff{};
        if (internal::subtractI64(trades[i].tsNs, chunk.baseTsNs, tsDiff)) {
            timeScale = gcdPositive(timeScale, tsDiff);
        } else {
            timeScale = 1;
        }
        if (i > begin) {
            std::int64_t pDiff{};
            if (!internal::subtractI64(trades[i].price, trades[i - 1u].price, pDiff)) {
                priceDiffOk = false;
            }
        }
        if (trades[i].side < 0 || trades[i].side > 1) {
            binarySideOnly = false;
        }
    }

    chunk.priceScale = priceDiffOk ? safeScale(priceScale) : std::int64_t{1};
    chunk.qtyScale = safeScale(qtyScale);
    chunk.timeScale = safeScale(timeScale);
    chunk.sideMode = binarySideOnly ? 0u : 1u;

    const auto priceDiv = math::ExactScaleDivisor::prepare(chunk.priceScale);
    const auto qtyDiv = math::ExactScaleDivisor::prepare(chunk.qtyScale);
    const auto timeDiv = math::ExactScaleDivisor::prepare(chunk.timeScale);

    chunk.baseTsUnit = timeDiv.divideSignedExact(chunk.baseTsNs);
    chunk.basePriceTick = priceDiv.divideSignedExact(chunk.basePrice);

    buildHotQty(trades, begin, end, chunk, true);
    for (const auto qty : chunk.hotQty) {
        writeVarint(chunk.hotQtyTableStream, zigzag(qty));
    }

    std::int64_t previousGroupTs = chunk.baseTsUnit;
    std::int64_t previousPrice = chunk.basePriceTick;
    BitWriter sideBits;
    BitWriter dpZeroBits;
    BitWriter countOneBits;
    BitWriter priceGroupCountOneBits;
    BitWriter qtyCodeBits;

    for (std::size_t i = begin; i < end;) {
        const auto rawTs = trades[i].tsNs;
        const auto ts = timeDiv.divideSignedExact(rawTs);
        std::uint32_t priceGroups = 0u;
        while (i < end && trades[i].tsNs == rawTs) {
            const auto rawPrice = trades[i].price;
            const auto price = priceDiv.divideSignedExact(rawPrice);
            const auto side = trades[i].side;
            std::uint32_t tradeCount = 0u;
            while (i < end && trades[i].tsNs == rawTs &&
                   trades[i].price == rawPrice && trades[i].side == side) {
                ++tradeCount;
                ++i;
            }
            ++priceGroups;
            ++chunk.priceGroupCount;
            const auto dp = std::bit_cast<std::int64_t>(
                std::bit_cast<std::uint64_t>(price) - std::bit_cast<std::uint64_t>(previousPrice));
            const bool dpZero = (dp == 0);
            const bool countOne = (tradeCount == 1u);
            dpZeroBits.writeBits(dpZero ? 1u : 0u, 1u);
            countOneBits.writeBits(countOne ? 1u : 0u, 1u);
            if (dpZero) ++chunk.dpZeroCount;
            else writeVarint(chunk.priceStream, zigzag(dp));
            if (countOne) {
                ++chunk.countOneCount;
            } else {
                writeVarint(chunk.priceStream, static_cast<std::uint64_t>(tradeCount - 2u));
            }
            if (chunk.sideMode == 0u) {
                sideBits.writeBits(static_cast<std::uint64_t>(side & 1), 1u);
            } else {
                chunk.sideStream.push_back(static_cast<std::uint8_t>(side & 0xff));
            }
            previousPrice = price;
            for (std::size_t q = i - tradeCount; q < i; ++q) {
                const auto qty = qtyDiv.divideSignedExact(trades[q].qty);
                const auto code = qtyCode(chunk, qty);
                qtyCodeBits.writeBits(code, chunk.hotQtyBits);
                if (code == chunk.hotQtyCapacity) {
                    writeVarint(chunk.qtyEscapeStream, zigzag(qty));
                    ++chunk.qtyEscapeCount;
                }
            }
        }
        ++chunk.timeGroupCount;
        const auto dt = std::bit_cast<std::int64_t>(
            std::bit_cast<std::uint64_t>(ts) - std::bit_cast<std::uint64_t>(previousGroupTs));
        writeVarint(chunk.timeStream, zigzag(dt));
        const bool onePriceGroup = (priceGroups == 1u);
        priceGroupCountOneBits.writeBits(onePriceGroup ? 1u : 0u, 1u);
        if (onePriceGroup) ++chunk.priceGroupCountOneCount;
        else writeVarint(chunk.timeStream, priceGroups);
        previousGroupTs = ts;
    }

    if (chunk.sideMode == 0u) {
        chunk.sideStream = sideBits.finish();
    }
    chunk.dpZeroStream = dpZeroBits.finish();
    chunk.countOneStream = countOneBits.finish();
    chunk.priceGroupCountOneStream = priceGroupCountOneBits.finish();
    chunk.qtyCodeStream = qtyCodeBits.finish();

    const std::size_t stride = layout.recordBytes;
    const std::size_t chunkCount = end - begin;
    chunk.residualStream.reserve(chunkCount * stride);
    for (std::size_t col = 0u; col < stride; ++col) {
        std::uint8_t prevByte = 0u;
        for (std::size_t row = begin; row < end; ++row) {
            const std::uint8_t cur = scratchRecords[row * stride + col];
            chunk.residualStream.push_back(static_cast<std::uint8_t>(cur - prevByte));
            prevByte = cur;
        }
    }

    std::uint32_t crc = 0xffffffffu;
    for (const auto* s : {&chunk.hotQtyTableStream, &chunk.timeStream, &chunk.priceStream,
                          &chunk.sideStream, &chunk.dpZeroStream, &chunk.countOneStream,
                          &chunk.priceGroupCountOneStream, &chunk.qtyCodeStream,
                          &chunk.qtyEscapeStream, &chunk.residualStream}) {
        crc = format::updateCrc32c(crc, *s);
    }
    chunk.payloadCrc32c = ~crc;
    return chunk;
}

void appendChunkBytes(std::vector<std::uint8_t>& out, const EncodedChunk& chunk) {
    ChunkHeader header{};
    header.recordCount = chunk.recordCount;
    header.timeGroupCount = chunk.timeGroupCount;
    header.priceGroupCount = chunk.priceGroupCount;
    header.qtyEscapeCount = chunk.qtyEscapeCount;
    header.firstTsNs = chunk.firstTsNs;
    header.lastTsNs = chunk.lastTsNs;
    header.baseTsNs = chunk.baseTsNs;
    header.basePrice = chunk.basePrice;
    header.baseTsUnit = chunk.baseTsUnit;
    header.basePriceTick = chunk.basePriceTick;
    header.priceScale = chunk.priceScale;
    header.qtyScale = chunk.qtyScale;
    header.timeScale = chunk.timeScale;
    header.hotQtyCount = chunk.hotQtyCount;
    header.hotQtyBits = chunk.hotQtyBits;
    header.sideMode = chunk.sideMode;
    header.timeStreamBytes = static_cast<std::uint32_t>(chunk.timeStream.size());
    header.priceStreamBytes = static_cast<std::uint32_t>(chunk.priceStream.size());
    header.sideStreamBytes = static_cast<std::uint32_t>(chunk.sideStream.size());
    header.dpZeroStreamBytes = static_cast<std::uint32_t>(chunk.dpZeroStream.size());
    header.countOneStreamBytes = static_cast<std::uint32_t>(chunk.countOneStream.size());
    header.priceGroupCountOneStreamBytes = static_cast<std::uint32_t>(chunk.priceGroupCountOneStream.size());
    header.qtyCodeStreamBytes = static_cast<std::uint32_t>(chunk.qtyCodeStream.size());
    header.qtyEscapeStreamBytes = static_cast<std::uint32_t>(chunk.qtyEscapeStream.size());
    header.hotQtyTableBytes = static_cast<std::uint32_t>(chunk.hotQtyTableStream.size());
    header.residualStreamBytes = static_cast<std::uint32_t>(chunk.residualStream.size());
    header.dpZeroCount = chunk.dpZeroCount;
    header.countOneCount = chunk.countOneCount;
    header.priceGroupCountOneCount = chunk.priceGroupCountOneCount;
    header.payloadCrc32c = chunk.payloadCrc32c;

    const auto headerBytes = serializeChunkHeader(header);
    out.insert(out.end(), headerBytes.begin(), headerBytes.end());
    for (const auto* s : {&chunk.hotQtyTableStream, &chunk.timeStream, &chunk.priceStream,
                          &chunk.sideStream, &chunk.dpZeroStream, &chunk.countOneStream,
                          &chunk.priceGroupCountOneStream, &chunk.qtyCodeStream,
                          &chunk.qtyEscapeStream, &chunk.residualStream}) {
        out.insert(out.end(), s->begin(), s->end());
    }
}

}  // namespace hft_compressor::codecs::trades_grouped_delta_qtydict::codec_detail

namespace hft_compressor::codecs::trades_grouped_delta_qtydict {
using namespace codec_detail;

Status encodeSliceBytes(
    const ByteBlockLayout& layout,
    std::span<const std::uint8_t> records,
    std::vector<std::uint8_t>& outEncoded,
    std::uint64_t& outRecordCount,
    std::uint64_t& outEncodeCoreNs,
    std::uint64_t& outEncodeCycles,
    std::string& outError) {
    outEncoded.clear();
    outRecordCount = 0u;
    outEncodeCoreNs = 0u;
    outEncodeCycles = 0u;
    if (layout.streamType != StreamType::Trades || !validByteBlockLayout(layout)) {
        outError = "invalid trades ByteBlockLayout";
        return Status::InvalidArgument;
    }
    std::size_t count = 0u;
    if (!math::exactRecordCount(records.size(), layout.recordBytes, count)) {
        outError = "trades slice byte size is not a non-zero multiple of layout.recordBytes";
        return Status::InvalidArgument;
    }

    const std::size_t stride = layout.recordBytes;
    std::vector<std::uint8_t> scratchRecords(records.begin(), records.end());
    std::vector<Trade> trades(count);
    for (std::size_t i = 0u; i < count; ++i) {
        const std::size_t baseOff = i * stride;
        const std::size_t pOff = baseOff + layout.payloadOffset;
        const std::size_t tOff = baseOff + layout.timestampOffset;
        trades[i] = Trade{
            std::bit_cast<std::int64_t>(byte_block::word(records, pOff, 8u)),
            std::bit_cast<std::int64_t>(byte_block::word(records, pOff + 8u, 8u)),
            static_cast<std::int64_t>(records[pOff + 16u]),
            std::bit_cast<std::int64_t>(byte_block::word(records, tOff, 8u)),
        };
        byte_block::put(scratchRecords, pOff, 0u, 8u);
        byte_block::put(scratchRecords, pOff + 8u, 0u, 8u);
        scratchRecords[pOff + 16u] = 0u;
        byte_block::put(scratchRecords, tOff, 0u, 8u);
    }

    FileHeader fileHeader{};
    fileHeader.stream = format::streamToWire(StreamType::Trades);
    fileHeader.layout = layout;
    fileHeader.chunkRecords = kDefaultChunkRecords;
    fileHeader.inputBytes = kBinarySliceHeaderBytes + records.size();
    fileHeader.recordCount = count;
    fileHeader.recordsCrc32c = format::crc32c(records);

    outEncoded.resize(kFileHeaderBytes, 0u);
    const auto coreStartNs = timing::nowNs();
    const auto coreStartCycles = timing::readCycles();

    for (std::size_t begin = 0u; begin < count; begin += kDefaultChunkRecords) {
        const std::size_t end = std::min<std::size_t>(count, begin + kDefaultChunkRecords);
        const auto chunk = encodeChunk(layout, scratchRecords, trades, begin, end);
        appendChunkBytes(outEncoded, chunk);
        ++fileHeader.chunkCount;
        fileHeader.timeGroupCount += chunk.timeGroupCount;
        fileHeader.priceGroupCount += chunk.priceGroupCount;
        fileHeader.qtyEscapeCount += chunk.qtyEscapeCount;
    }

    outEncodeCycles = timing::readCycles() - coreStartCycles;
    outEncodeCoreNs = timing::nowNs() - coreStartNs;

    fileHeader.outputBytes = outEncoded.size();
    fileHeader.headerCrc32c = headerCrc32c(fileHeader);
    const auto hdrBytes = serializeFileHeader(fileHeader, true);
    std::copy(hdrBytes.begin(), hdrBytes.end(), outEncoded.begin());
    outRecordCount = count;
    return Status::Ok;
}

CompressionResult compress(const CompressionRequest& request, const PipelineDescriptor& pipeline) noexcept try {
    if (request.inputPath.empty()) {
        return internal::fail(Status::InvalidArgument, request, &pipeline, "input path is empty");
    }
    if (inferStreamTypeFromPath(request.inputPath) != StreamType::Trades) {
        return internal::fail(Status::UnsupportedStream, request, &pipeline, "expected trades binary slice (.bin/.hbs)");
    }
    CompressionResult result{};
    internal::applyPipeline(result, &pipeline);
    result.streamType = StreamType::Trades;
    result.inputPath = request.inputPath;
    const auto encodeTotalStartNs = timing::nowNs();

    const auto readStartNs = timing::nowNs();
    std::vector<std::uint8_t> input;
    if (!internal::readFileBytes(request.inputPath, input)) {
        return internal::fail(Status::IoError, request, &pipeline, "failed to read input file");
    }
    result.readNs = timing::nowNs() - readStartNs;
    result.inputBytes = input.size();

    const auto parseStartNs = timing::nowNs();
    BinarySliceView slice{};
    const auto sliceStatus = parseBinarySlice(input, slice);
    if (!isOk(sliceStatus) || slice.layout.streamType != StreamType::Trades) {
        return internal::fail(
            isOk(sliceStatus) ? Status::UnsupportedStream : sliceStatus,
            request,
            &pipeline,
            "input is not a valid canonical trades binary slice");
    }
    result.parseNs = timing::nowNs() - parseStartNs;

    std::vector<std::uint8_t> encoded;
    std::uint64_t recordCount = 0u;
    std::string encodeError;
    const auto encodeStatus = encodeSliceBytes(
        slice.layout, slice.records, encoded, recordCount, result.encodeCoreNs, result.encodeCycles, encodeError);
    if (!isOk(encodeStatus)) {
        return internal::fail(encodeStatus, request, &pipeline, encodeError);
    }

    const auto outputPath = internal::outputPathFor(request, pipeline, StreamType::Trades);
    std::error_code ec;
    std::filesystem::create_directories(outputPath.parent_path(), ec);
    if (ec) {
        return internal::fail(Status::IoError, request, &pipeline, "failed to create output directory");
    }

    result.outputPath = outputPath;
    result.metricsPath = outputPath.parent_path() / (outputPath.stem().string() + ".metrics.json");
    const auto writeStartNs = timing::nowNs();
    std::ofstream out(outputPath, std::ios::binary | std::ios::trunc);
    if (!out) {
        return internal::fail(Status::IoError, request, &pipeline, "failed to open output file");
    }
    out.write(reinterpret_cast<const char*>(encoded.data()), static_cast<std::streamsize>(encoded.size()));
    out.close();
    if (!out) {
        return internal::fail(Status::IoError, request, &pipeline, "failed to write output file");
    }
    result.writeNs = timing::nowNs() - writeStartNs;
    result.encodeNs = timing::nowNs() - encodeTotalStartNs;
    result.outputBytes = encoded.size();
    result.lineCount = recordCount;
    result.blockCount = (recordCount + kDefaultChunkRecords - 1u) >> 14u;

    std::size_t decodedOffset = 0u;
    bool matches = true;
    const auto decodeStartNs = timing::nowNs();
    const auto decodeStartCycles = timing::readCycles();
    const auto decodeStatus = decode(encoded, [&](std::span<const std::uint8_t> block) {
        if (decodedOffset + block.size() > input.size()) {
            matches = false;
            return false;
        }
        matches = std::equal(block.begin(), block.end(), input.begin() + static_cast<std::ptrdiff_t>(decodedOffset));
        decodedOffset += block.size();
        return matches;
    });
    result.decodeCycles = timing::readCycles() - decodeStartCycles;
    result.decodeNs = timing::nowNs() - decodeStartNs;
    result.decodeCoreNs = result.decodeNs;
    result.roundtripOk = isOk(decodeStatus) && matches && decodedOffset == input.size();
    result.status = result.roundtripOk ? Status::Ok : Status::DecodeError;
    if (!result.roundtripOk) result.error = "roundtrip check failed";

    (void)internal::writeTextFile(result.metricsPath, toMetricsJson(result));
    return result;
} catch (...) {
    CompressionResult failed{};
    failed.status = Status::DecodeError;
    return failed;
}

}  // namespace hft_compressor::codecs::trades_grouped_delta_qtydict

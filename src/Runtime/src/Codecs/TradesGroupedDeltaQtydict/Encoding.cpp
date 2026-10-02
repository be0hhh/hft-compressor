#include "TradesGroupedDeltaQtyDictInternal.hpp"

namespace hft_compressor::codecs::trades_grouped_delta_qtydict::codec_detail {


struct JsonCursor {
    std::string_view text{};
    std::size_t pos{0};

    void skipSpaces() noexcept {
        while (pos < text.size()) {
            const char c = text[pos];
            if (c != ' ' && c != '\t' && c != '\r') break;
            ++pos;
        }
    }

    bool consume(char c) noexcept {
        skipSpaces();
        if (pos >= text.size() || text[pos] != c) return false;
        ++pos;
        return true;
    }

    bool parseInt64(std::int64_t& out) noexcept {
        skipSpaces();
        if (pos >= text.size()) return false;
        const std::size_t beginPos = pos;
        if (text[pos] == '-') {
            ++pos;
            if (pos >= text.size()) return false;
        }
        if (text[pos] == '0') {
            ++pos;
            if (pos < text.size() && text[pos] >= '0' && text[pos] <= '9') return false;
        } else {
            if (text[pos] < '1' || text[pos] > '9') return false;
            do {
                ++pos;
            } while (pos < text.size() && text[pos] >= '0' && text[pos] <= '9');
        }
        const char* begin = text.data() + beginPos;
        const char* end = text.data() + pos;
        const auto [ptr, ec] = std::from_chars(begin, end, out);
        if (ec != std::errc{} || ptr != end) return false;
        return true;
    }

    bool finish() noexcept {
        skipSpaces();
        return pos == text.size();
    }
};

bool parseTradeLine(std::string_view line, Trade& out) noexcept {
    JsonCursor p{line};
    return p.consume('[')
        && p.parseInt64(out.price) && p.consume(',')
        && p.parseInt64(out.qty) && p.consume(',')
        && p.parseInt64(out.side) && p.consume(',')
        && p.parseInt64(out.tsNs)
        && p.consume(']') && p.finish()
        && out.price > 0 && out.qty > 0 && (out.side == 0 || out.side == 1);
}

bool parseTrades(std::span<const std::uint8_t> input, std::vector<Trade>& out) {
    if (input.empty()) return false;
    std::int64_t previousTs = 0;
    bool havePrevious = false;
    std::size_t lineStart = 0;
    while (lineStart < input.size()) {
        std::size_t lineEnd = lineStart;
        while (lineEnd < input.size() && input[lineEnd] != static_cast<std::uint8_t>('\n')) ++lineEnd;
        std::string_view line{reinterpret_cast<const char*>(input.data() + lineStart), lineEnd - lineStart};
        if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
        if (line.empty()) return false;
        Trade trade{};
        if (!parseTradeLine(line, trade)) return false;
        if (havePrevious && trade.tsNs < previousTs) return false;
        previousTs = trade.tsNs;
        havePrevious = true;
        out.push_back(trade);
        lineStart = lineEnd + (lineEnd < input.size() ? 1u : 0u);
    }
    return !out.empty();
}

void buildHotQty(const std::vector<Trade>& trades, std::size_t begin, std::size_t end, EncodedChunk& chunk, bool useCompactQuantityTable) {
    std::unordered_map<std::int64_t, std::uint32_t> counts;
    for (std::size_t i = begin; i < end; ++i) ++counts[useCompactQuantityTable ? trades[i].qty / chunk.qtyScale : trades[i].qty];
    std::vector<std::pair<std::int64_t, std::uint32_t>> values;
    values.reserve(counts.size());
    for (const auto& item : counts) values.push_back(item);
    std::sort(values.begin(), values.end(), [](const auto& a, const auto& b) {
        if (a.second != b.second) return a.second > b.second;
        return a.first < b.first;
    });

    std::array<std::uint32_t, 4> capacities{16u, 32u, 64u, 128u};
    std::uint64_t bestCost = UINT64_MAX;
    std::uint32_t bestCapacity = useCompactQuantityTable ? 16u : kHotQtyCount;
    if (!useCompactQuantityTable) {
        chunk.hotQtyCapacity = kHotQtyCount;
        chunk.hotQtyBits = 7u;
        chunk.hotQtyCount = static_cast<std::uint32_t>(std::min<std::size_t>(values.size(), kHotQtyCount));
        chunk.hotQty.reserve(chunk.hotQtyCount);
        for (std::uint32_t i = 0; i < chunk.hotQtyCount; ++i) chunk.hotQty.push_back(values[i].first);
        return;
    }

    for (const auto capacity : capacities) {
        const auto hotCount = std::min<std::size_t>(values.size(), capacity);
        std::uint64_t hotHits = 0;
        for (std::size_t i = 0; i < hotCount; ++i) hotHits += values[i].second;
        const auto escapes = static_cast<std::uint64_t>(end - begin) - hotHits;
        const auto bits = bitsForHotCapacity(capacity);
        std::uint64_t escapeBytes = 0;
        for (std::size_t i = hotCount; i < values.size(); ++i) {
            std::vector<std::uint8_t> tmp;
            writeVarint(tmp, static_cast<std::uint64_t>(values[i].first));
            escapeBytes += static_cast<std::uint64_t>(tmp.size()) * values[i].second;
        }
        const auto tableBytesEstimate = hotCount * 2u;
        const auto codeBytes = ((static_cast<std::uint64_t>(end - begin) * bits) + 7u) / 8u;
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
    for (std::uint32_t i = 0; i < chunk.hotQtyCount; ++i) chunk.hotQty.push_back(values[i].first);
}

std::uint64_t qtyCode(const EncodedChunk& chunk, std::int64_t qty) noexcept {
    for (std::uint32_t i = 0; i < chunk.hotQtyCount; ++i) {
        if (chunk.hotQty[i] == qty) return i;
    }
    return chunk.hotQtyCapacity;
}

EncodedChunk encodeChunk(const std::vector<Trade>& trades, std::size_t begin, std::size_t end) {
    EncodedChunk chunk{};
    chunk.recordCount = static_cast<std::uint32_t>(end - begin);
    chunk.firstTsNs = trades[begin].tsNs;
    chunk.lastTsNs = trades[end - 1u].tsNs;
    chunk.baseTsNs = trades[begin].tsNs;
    chunk.basePrice = trades[begin].price;

    {
        std::int64_t priceScale = 0;
        std::int64_t qtyScale = 0;
        std::int64_t timeScale = chunk.baseTsNs;
        for (std::size_t i = begin; i < end; ++i) {
            priceScale = gcdPositive(priceScale, trades[i].price);
            qtyScale = gcdPositive(qtyScale, trades[i].qty);
            timeScale = gcdPositive(timeScale, trades[i].tsNs - chunk.baseTsNs);
        }
        chunk.priceScale = safeScale(priceScale);
        chunk.qtyScale = safeScale(qtyScale);
        chunk.timeScale = safeScale(timeScale);
        chunk.baseTsUnit = chunk.baseTsNs / chunk.timeScale;
        chunk.basePriceTick = chunk.basePrice / chunk.priceScale;
    }

    buildHotQty(trades, begin, end, chunk, true);
    for (const auto qty : chunk.hotQty) writeVarint(chunk.hotQtyTableStream, static_cast<std::uint64_t>(qty));

    std::int64_t previousGroupTs = chunk.baseTsUnit;
    std::int64_t previousPrice = chunk.basePriceTick;
    BitWriter sideBits;
    BitWriter dpZeroBits;
    BitWriter countOneBits;
    BitWriter priceGroupCountOneBits;
    BitWriter qtyCodeBits;
    for (std::size_t i = begin; i < end;) {
        const auto rawTs = trades[i].tsNs;
        const auto ts = rawTs / chunk.timeScale;
        std::uint32_t priceGroups = 0;
        while (i < end && trades[i].tsNs == rawTs) {
            const auto rawPrice = trades[i].price;
            const auto price = rawPrice / chunk.priceScale;
            const auto side = trades[i].side;
            std::uint32_t tradeCount = 0;
            while (i < end && trades[i].tsNs == rawTs && trades[i].price == rawPrice && trades[i].side == side) {
                ++tradeCount;
                ++i;
            }
            ++priceGroups;
            ++chunk.priceGroupCount;
            const auto dp = price - previousPrice;
            const bool dpZero = dp == 0;
            const bool countOne = tradeCount == 1u;
            dpZeroBits.writeBits(dpZero ? 1u : 0u, 1u);
            countOneBits.writeBits(countOne ? 1u : 0u, 1u);
            if (dpZero) ++chunk.dpZeroCount;
            else writeVarint(chunk.priceStream, zigzag(dp));
            if (countOne) {
                ++chunk.countOneCount;
            } else {
                writeVarint(chunk.priceStream, static_cast<std::uint64_t>(tradeCount - 2u));
            }
            sideBits.writeBits(static_cast<std::uint64_t>(side), 1u);
            previousPrice = price;
            for (std::size_t q = i - tradeCount; q < i; ++q) {
                const auto qty = trades[q].qty / chunk.qtyScale;
                const auto code = qtyCode(chunk, qty);
                qtyCodeBits.writeBits(code, chunk.hotQtyBits);
                if (code == chunk.hotQtyCapacity) {
                    writeVarint(chunk.qtyEscapeStream, static_cast<std::uint64_t>(qty));
                    ++chunk.qtyEscapeCount;
                }
            }
        }
        ++chunk.timeGroupCount;
        writeVarint(chunk.timeStream, static_cast<std::uint64_t>(ts - previousGroupTs));
        const bool onePriceGroup = priceGroups == 1u;
        priceGroupCountOneBits.writeBits(onePriceGroup ? 1u : 0u, 1u);
        if (onePriceGroup) ++chunk.priceGroupCountOneCount;
        else writeVarint(chunk.timeStream, priceGroups);
        previousGroupTs = ts;
    }
    chunk.sideStream = sideBits.finish();
    chunk.dpZeroStream = dpZeroBits.finish();
    chunk.countOneStream = countOneBits.finish();
    chunk.priceGroupCountOneStream = priceGroupCountOneBits.finish();
    chunk.qtyCodeStream = qtyCodeBits.finish();

    std::vector<std::uint8_t> payload;
    payload.reserve(chunk.hotQtyTableStream.size() + chunk.timeStream.size() + chunk.priceStream.size() + chunk.sideStream.size()
        + chunk.dpZeroStream.size() + chunk.countOneStream.size() + chunk.priceGroupCountOneStream.size()
        + chunk.qtyCodeStream.size() + chunk.qtyEscapeStream.size());
    payload.insert(payload.end(), chunk.hotQtyTableStream.begin(), chunk.hotQtyTableStream.end());
    payload.insert(payload.end(), chunk.timeStream.begin(), chunk.timeStream.end());
    payload.insert(payload.end(), chunk.priceStream.begin(), chunk.priceStream.end());
    payload.insert(payload.end(), chunk.sideStream.begin(), chunk.sideStream.end());
    payload.insert(payload.end(), chunk.dpZeroStream.begin(), chunk.dpZeroStream.end());
    payload.insert(payload.end(), chunk.countOneStream.begin(), chunk.countOneStream.end());
    payload.insert(payload.end(), chunk.priceGroupCountOneStream.begin(), chunk.priceGroupCountOneStream.end());
    payload.insert(payload.end(), chunk.qtyCodeStream.begin(), chunk.qtyCodeStream.end());
    payload.insert(payload.end(), chunk.qtyEscapeStream.begin(), chunk.qtyEscapeStream.end());
    chunk.payloadCrc32c = format::crc32c(payload);
    return chunk;
}

void writeChunk(std::ofstream& out, const EncodedChunk& chunk) {
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
    header.timeStreamBytes = static_cast<std::uint32_t>(chunk.timeStream.size());
    header.priceStreamBytes = static_cast<std::uint32_t>(chunk.priceStream.size());
    header.sideStreamBytes = static_cast<std::uint32_t>(chunk.sideStream.size());
    header.dpZeroStreamBytes = static_cast<std::uint32_t>(chunk.dpZeroStream.size());
    header.countOneStreamBytes = static_cast<std::uint32_t>(chunk.countOneStream.size());
    header.priceGroupCountOneStreamBytes = static_cast<std::uint32_t>(chunk.priceGroupCountOneStream.size());
    header.qtyCodeStreamBytes = static_cast<std::uint32_t>(chunk.qtyCodeStream.size());
    header.qtyEscapeStreamBytes = static_cast<std::uint32_t>(chunk.qtyEscapeStream.size());
    header.hotQtyTableBytes = static_cast<std::uint32_t>(chunk.hotQtyTableStream.size());
    header.dpZeroCount = chunk.dpZeroCount;
    header.countOneCount = chunk.countOneCount;
    header.priceGroupCountOneCount = chunk.priceGroupCountOneCount;
    header.payloadCrc32c = chunk.payloadCrc32c;
    const auto headerBytes = serializeChunkHeader(header);
    out.write(reinterpret_cast<const char*>(headerBytes.data()), static_cast<std::streamsize>(headerBytes.size()));
    out.write(reinterpret_cast<const char*>(chunk.hotQtyTableStream.data()), static_cast<std::streamsize>(chunk.hotQtyTableStream.size()));
    out.write(reinterpret_cast<const char*>(chunk.timeStream.data()), static_cast<std::streamsize>(chunk.timeStream.size()));
    out.write(reinterpret_cast<const char*>(chunk.priceStream.data()), static_cast<std::streamsize>(chunk.priceStream.size()));
    out.write(reinterpret_cast<const char*>(chunk.sideStream.data()), static_cast<std::streamsize>(chunk.sideStream.size()));
    out.write(reinterpret_cast<const char*>(chunk.dpZeroStream.data()), static_cast<std::streamsize>(chunk.dpZeroStream.size()));
    out.write(reinterpret_cast<const char*>(chunk.countOneStream.data()), static_cast<std::streamsize>(chunk.countOneStream.size()));
    out.write(reinterpret_cast<const char*>(chunk.priceGroupCountOneStream.data()), static_cast<std::streamsize>(chunk.priceGroupCountOneStream.size()));
    out.write(reinterpret_cast<const char*>(chunk.qtyCodeStream.data()), static_cast<std::streamsize>(chunk.qtyCodeStream.size()));
    out.write(reinterpret_cast<const char*>(chunk.qtyEscapeStream.data()), static_cast<std::streamsize>(chunk.qtyEscapeStream.size()));
}
}  // namespace codec_detail

namespace hft_compressor::codecs::trades_grouped_delta_qtydict {
using namespace codec_detail;


CompressionResult compress(const CompressionRequest& request, const PipelineDescriptor& pipeline) noexcept {
    if (request.inputPath.empty()) {
        auto result = internal::fail(Status::InvalidArgument, request, &pipeline, "input path is empty");
        return result;
    }
    if (inferStreamTypeFromPath(request.inputPath) != StreamType::Trades) {
        auto result = internal::fail(Status::UnsupportedStream, request, &pipeline, "expected trades.jsonl");
        return result;
    }
    CompressionResult result{};
    internal::applyPipeline(result, &pipeline);
    result.streamType = StreamType::Trades;
    result.inputPath = request.inputPath;
    const auto encodeTotalStartNs = timing::nowNs();
    const auto readStartNs = timing::nowNs();
    std::vector<std::uint8_t> input;
    if (!internal::readFileBytes(request.inputPath, input)) {
        auto result = internal::fail(Status::IoError, request, &pipeline, "failed to read input file");
        return result;
    }
    result.readNs = timing::nowNs() - readStartNs;
    result.inputBytes = static_cast<std::uint64_t>(input.size());
    const auto parseStartNs = timing::nowNs();
    std::vector<Trade> trades;
    trades.reserve(static_cast<std::size_t>(std::count(input.begin(), input.end(), static_cast<std::uint8_t>('\n'))) + 1u);
    if (!parseTrades(input, trades)) {
        auto result = internal::fail(Status::CorruptData, request, &pipeline, "input is not clean canonical trades jsonl");
        return result;
    }
    result.parseNs = timing::nowNs() - parseStartNs;

    const auto outputPath = internal::outputPathFor(request, pipeline, StreamType::Trades);
    std::error_code ec;
    std::filesystem::create_directories(outputPath.parent_path(), ec);
    if (ec) {
        auto result = internal::fail(Status::IoError, request, &pipeline, "failed to create output directory");
        return result;
    }

    result.outputPath = outputPath;
    result.metricsPath = outputPath.parent_path() / (outputPath.stem().string() + ".metrics.json");

    std::ofstream out(outputPath, std::ios::binary | std::ios::trunc);
    if (!out) {
        auto failed = internal::fail(Status::IoError, request, &pipeline, "failed to open output file");
        return failed;
    }

    FileHeader fileHeader{};
    fileHeader.version = kCurrentArtifactVersion;
    fileHeader.stream = format::streamToWire(StreamType::Trades);
    fileHeader.lineEnding = detectLineEnding(input);
    fileHeader.chunkRecords = kDefaultChunkRecords;
    fileHeader.inputBytes = result.inputBytes;
    const auto placeholder = serializeFileHeader(fileHeader, true);
    out.write(reinterpret_cast<const char*>(placeholder.data()), static_cast<std::streamsize>(placeholder.size()));

    const auto encodeStartNs = timing::nowNs();
    const auto encodeStartCycles = timing::readCycles();
    for (std::size_t begin = 0; begin < trades.size(); begin += fileHeader.chunkRecords) {
        const auto end = std::min<std::size_t>(trades.size(), begin + fileHeader.chunkRecords);
        const auto chunk = encodeChunk(trades, begin, end);
        writeChunk(out, chunk);
        ++fileHeader.chunkCount;
        fileHeader.recordCount += chunk.recordCount;
        fileHeader.timeGroupCount += chunk.timeGroupCount;
        fileHeader.priceGroupCount += chunk.priceGroupCount;
        fileHeader.qtyEscapeCount += chunk.qtyEscapeCount;
        result.lineCount += chunk.recordCount;
        ++result.blockCount;
    }
    result.encodeCycles = timing::readCycles() - encodeStartCycles;
    result.encodeCoreNs = timing::nowNs() - encodeStartNs;
    const auto writeStartNs = timing::nowNs();
    out.flush();
    result.outputBytes = static_cast<std::uint64_t>(out.tellp());
    fileHeader.outputBytes = result.outputBytes;
    fileHeader.headerCrc32c = headerCrc32c(fileHeader);
    const auto finalHeader = serializeFileHeader(fileHeader, true);
    out.seekp(0, std::ios::beg);
    out.write(reinterpret_cast<const char*>(finalHeader.data()), static_cast<std::streamsize>(finalHeader.size()));
    out.close();
    result.writeNs = timing::nowNs() - writeStartNs;
    result.encodeNs = timing::nowNs() - encodeTotalStartNs;
    if (!out) {
        auto failed = internal::fail(Status::IoError, request, &pipeline, "failed to write trade grouped artifact");
        return failed;
    }

    std::size_t decodedOffset = 0;
    bool decodedMatchesInput = true;
    const auto decodeStartNs = timing::nowNs();
    const auto decodeStartCycles = timing::readCycles();
    const auto decodeStatus = decodeFile(outputPath, [&](std::span<const std::uint8_t> block) {
        if (decodedOffset + block.size() > input.size()) {
            decodedMatchesInput = false;
            return false;
        }
        decodedMatchesInput = std::equal(block.begin(), block.end(), input.begin() + static_cast<std::ptrdiff_t>(decodedOffset));
        decodedOffset += block.size();
        return decodedMatchesInput;
    });
    result.decodeCycles = timing::readCycles() - decodeStartCycles;
    result.decodeNs = timing::nowNs() - decodeStartNs;
    result.decodeCoreNs = result.decodeNs;
    result.roundtripOk = isOk(decodeStatus) && decodedMatchesInput && decodedOffset == input.size();
    result.status = result.roundtripOk ? Status::Ok : Status::DecodeError;
    if (!result.roundtripOk) result.error = "roundtrip check failed";

    (void)internal::writeTextFile(result.metricsPath, toMetricsJson(result));
    return result;
}
}  // namespace hft_compressor::codecs::trades_grouped_delta_qtydict

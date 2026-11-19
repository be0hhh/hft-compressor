#include "BookTickerDeltaMask.hpp"
#include "../BaseDecode.hpp"
#include "../ByteBlock/BinaryTransform.hpp"
#include "../../Common/CompressionInternals.hpp"
#include "../../Common/ExactScaleMath.hpp"
#include "../../Common/Timing.hpp"
#include "../../Container/Hfc/Format.hpp"

#include <algorithm>
#include <array>
#include <bit>
#include <fstream>
#include <limits>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

namespace hft_compressor::codecs::bookticker_delta_mask {
namespace {

constexpr std::uint32_t kMagic = 0x4b544243u; // BTCK
constexpr std::size_t kHeaderBytes = 200u;

struct Row {
    std::int64_t bid{0};
    std::int64_t bidQty{0};
    std::int64_t ask{0};
    std::int64_t askQty{0};
    std::int64_t ts{0};
};

struct Header {
    std::uint32_t magic{kMagic};
    std::uint16_t reserved{0};
    ByteBlockLayout layout{};
    std::uint64_t inputBytes{0};
    std::uint64_t outputBytes{0};
    std::uint64_t recordCount{0};
    std::int64_t timeScale{1};
    std::int64_t priceScale{1};
    std::int64_t qtyScale{1};
    std::int64_t baseTsUnit{0};
    std::int64_t baseBidTick{0};
    std::int64_t baseSpreadTick{0};
    std::int64_t baseBidQtyLot{0};
    std::int64_t baseAskQtyLot{0};
    std::uint32_t timeBytes{0};
    std::uint32_t maskBytes{0};
    std::uint32_t bidBytes{0};
    std::uint32_t spreadBytes{0};
    std::uint32_t bidQtyBytes{0};
    std::uint32_t askQtyBytes{0};
    std::uint32_t residualBytes{0};
    std::uint32_t maskNonZero{0};
    std::uint32_t bidChanged{0};
    std::uint32_t spreadChanged{0};
    std::uint32_t bidQtyChanged{0};
    std::uint32_t askQtyChanged{0};
    std::uint32_t recordsCrc32c{0};
    std::uint32_t headerCrc32c{0};
};

std::uint64_t zz(std::int64_t v) noexcept {
    return (static_cast<std::uint64_t>(v) << 1u) ^ static_cast<std::uint64_t>(v >> 63u);
}

void varint(std::vector<std::uint8_t>& out, std::uint64_t v) {
    while (v >= 0x80u) {
        out.push_back(static_cast<std::uint8_t>(v | 0x80u));
        v >>= 7u;
    }
    out.push_back(static_cast<std::uint8_t>(v));
}

template <class T>
void le(std::vector<std::uint8_t>& out, T v) {
    using U = std::make_unsigned_t<T>;
    U u = static_cast<U>(v);
    for (std::size_t i = 0; i < sizeof(T); ++i) {
        out.push_back(static_cast<std::uint8_t>((u >> (i * 8u)) & 0xffu));
    }
}

template <class T>
bool rd(const std::uint8_t*& p, const std::uint8_t* e, T& out) noexcept {
    if (static_cast<std::size_t>(e - p) < sizeof(T)) return false;
    using U = std::make_unsigned_t<T>;
    U u = 0;
    for (std::size_t i = 0; i < sizeof(T); ++i) {
        u |= static_cast<U>(*p++) << (i * 8u);
    }
    out = static_cast<T>(u);
    return true;
}

std::vector<std::uint8_t> serializeHeaderRaw(Header h, bool includeCrc) {
    if (!includeCrc) h.headerCrc32c = 0u;
    std::vector<std::uint8_t> out;
    out.reserve(kHeaderBytes);
    le(out, h.magic);
    le(out, h.reserved);
    le(out, static_cast<std::uint32_t>(h.layout.streamType));
    le(out, h.layout.recordBytes);
    le(out, h.layout.payloadOffset);
    le(out, h.layout.timestampOffset);
    le(out, h.layout.depthLevelsOffset);
    le(out, h.layout.depthLevelStride);
    le(out, h.layout.depthLevelCountOffset);
    le(out, h.layout.depthLevelCapacity);
    le(out, h.inputBytes);
    le(out, h.outputBytes);
    le(out, h.recordCount);
    le(out, h.timeScale);
    le(out, h.priceScale);
    le(out, h.qtyScale);
    le(out, h.baseTsUnit);
    le(out, h.baseBidTick);
    le(out, h.baseSpreadTick);
    le(out, h.baseBidQtyLot);
    le(out, h.baseAskQtyLot);
    le(out, h.timeBytes);
    le(out, h.maskBytes);
    le(out, h.bidBytes);
    le(out, h.spreadBytes);
    le(out, h.bidQtyBytes);
    le(out, h.askQtyBytes);
    le(out, h.residualBytes);
    le(out, h.maskNonZero);
    le(out, h.bidChanged);
    le(out, h.spreadChanged);
    le(out, h.bidQtyChanged);
    le(out, h.askQtyChanged);
    le(out, h.recordsCrc32c);
    le(out, h.headerCrc32c);
    out.resize(kHeaderBytes, 0u);
    return out;
}

std::uint32_t computeHeaderCrc(const Header& h) {
    return format::crc32c(serializeHeaderRaw(h, false));
}

bool readHeader(std::span<const std::uint8_t> data, Header& h) noexcept {
    if (data.size() < kHeaderBytes) return false;
    const auto* p = data.data();
    const auto* e = data.data() + kHeaderBytes;
    std::uint32_t streamRaw = 0u;
    const bool ok = rd(p, e, h.magic)
        && rd(p, e, h.reserved)
        && rd(p, e, streamRaw)
        && rd(p, e, h.layout.recordBytes)
        && rd(p, e, h.layout.payloadOffset)
        && rd(p, e, h.layout.timestampOffset)
        && rd(p, e, h.layout.depthLevelsOffset)
        && rd(p, e, h.layout.depthLevelStride)
        && rd(p, e, h.layout.depthLevelCountOffset)
        && rd(p, e, h.layout.depthLevelCapacity)
        && rd(p, e, h.inputBytes)
        && rd(p, e, h.outputBytes)
        && rd(p, e, h.recordCount)
        && rd(p, e, h.timeScale)
        && rd(p, e, h.priceScale)
        && rd(p, e, h.qtyScale)
        && rd(p, e, h.baseTsUnit)
        && rd(p, e, h.baseBidTick)
        && rd(p, e, h.baseSpreadTick)
        && rd(p, e, h.baseBidQtyLot)
        && rd(p, e, h.baseAskQtyLot)
        && rd(p, e, h.timeBytes)
        && rd(p, e, h.maskBytes)
        && rd(p, e, h.bidBytes)
        && rd(p, e, h.spreadBytes)
        && rd(p, e, h.bidQtyBytes)
        && rd(p, e, h.askQtyBytes)
        && rd(p, e, h.residualBytes)
        && rd(p, e, h.maskNonZero)
        && rd(p, e, h.bidChanged)
        && rd(p, e, h.spreadChanged)
        && rd(p, e, h.bidQtyChanged)
        && rd(p, e, h.askQtyChanged)
        && rd(p, e, h.recordsCrc32c)
        && rd(p, e, h.headerCrc32c);
    if (!ok || !std::all_of(p, e, [](auto byte) { return byte == 0u; })) return false;
    h.layout.streamType = static_cast<StreamType>(streamRaw);
    return h.magic == kMagic
        && h.reserved == 0u
        && h.layout.streamType == StreamType::BookTicker
        && validByteBlockLayout(h.layout)
        && h.timeScale > 0 && h.priceScale > 0 && h.qtyScale > 0
        && h.headerCrc32c == computeHeaderCrc(h);
}

void pushPackedMask(std::vector<std::uint8_t>& out, std::uint8_t mask, bool& highNibble) {
    mask &= 0x0fu;
    if (!highNibble) {
        out.push_back(mask);
        highNibble = true;
    } else {
        out.back() = static_cast<std::uint8_t>(out.back() | static_cast<std::uint8_t>(mask << 4u));
        highNibble = false;
    }
}

Status emitText(const std::string& s, const DecodedBlockCallback& cb) {
    return cb(std::span<const std::uint8_t>{
        reinterpret_cast<const std::uint8_t*>(s.data()), s.size()})
        ? Status::Ok
        : Status::CallbackStopped;
}

Status decodeColumns(const internal::DecodeSource& source,
                     const DecodedBlockCallback* outBlocks,
                     std::ostream* encoded) {
    std::array<std::uint8_t, kHeaderBytes> bytes{};
    Header h{};
    if (!source.read(0u, bytes) || !readHeader(bytes, h) || h.outputBytes != source.size()
        || h.recordCount == 0u || h.recordCount > std::numeric_limits<std::uint32_t>::max()) {
        return Status::CorruptData;
    }
    const auto updates = h.recordCount - 1u;
    if (updates > h.timeBytes || h.maskBytes != ((updates + 1u) >> 1u)) {
        return Status::CorruptData;
    }

    const std::uint64_t rawRecordBytes = h.recordCount * h.layout.recordBytes;
    if (h.residualBytes != rawRecordBytes ||
        h.inputBytes != kBinarySliceHeaderBytes + rawRecordBytes) {
        return Status::CorruptData;
    }

    std::uint64_t offset = kHeaderBytes;
    auto next = [&](std::uint32_t count) {
        auto result = source.cursor(offset, count);
        if (source.contains(offset, count)) offset += count;
        return result;
    };
    auto time = next(h.timeBytes);
    auto mask = next(h.maskBytes);
    auto bidInput = next(h.bidBytes);
    auto spreadInput = next(h.spreadBytes);
    auto bidQty = next(h.bidQtyBytes);
    auto askQty = next(h.askQtyBytes);
    auto residual = next(h.residualBytes);
    if (!time || !mask || !bidInput || !spreadInput || !bidQty || !askQty || !residual ||
        offset != source.size()) {
        return Status::CorruptData;
    }

    std::int64_t ts = h.baseTsUnit;
    std::int64_t bid = h.baseBidTick;
    std::int64_t spread = h.baseSpreadTick;
    std::int64_t bq = h.baseBidQtyLot;
    std::int64_t aq = h.baseAskQtyLot;
    std::uint8_t packedMask{};
    bool highNibble = false;
    std::uint64_t masksNonzero{}, bidsChanged{}, spreadsChanged{}, bidsQtyChanged{}, asksQtyChanged{};

    std::int64_t baseTime{}, baseBid{}, baseSpread{}, baseBq{}, baseAq{};
    if (!internal::multiplyI64(ts, h.timeScale, baseTime) ||
        !internal::multiplyI64(bid, h.priceScale, baseBid) ||
        !internal::multiplyI64(spread, h.priceScale, baseSpread) ||
        !internal::multiplyI64(bq, h.qtyScale, baseBq) ||
        !internal::multiplyI64(aq, h.qtyScale, baseAq)) {
        return Status::CorruptData;
    }

    if (encoded) {
        *encoded << "{\n  \"pipeline_id\": \"hftmac.bookticker_delta_mask\",\n  \"record_count\": "
                 << h.recordCount << ",\n  \"base_state\": {\"ts\": " << baseTime
                 << ", \"bid\": " << baseBid << ", \"spread\": " << baseSpread
                 << ", \"bid_qty\": " << baseBq << ", \"ask_qty\": " << baseAq << "},\n  \"updates\": [\n";
    }

    std::vector<Row> decodedRows;
    if (outBlocks != nullptr) decodedRows.reserve(static_cast<std::size_t>(h.recordCount));

    for (std::uint64_t i = 0; i < h.recordCount; ++i) {
        std::uint8_t m{};
        std::int64_t dt{};
        std::int64_t dbid{}, dspread{}, dbq{}, daq{};
        if (i != 0u) {
            std::uint64_t rawDt{};
            if (!time->varint(rawDt)) return Status::CorruptData;
            dt = internal::decodeZigzag(rawDt);
            if (!internal::addI64(ts, dt, ts)) return Status::CorruptData;

            if (!highNibble && !mask->byte(packedMask)) return Status::CorruptData;
            m = highNibble ? static_cast<std::uint8_t>(packedMask >> 4u) : static_cast<std::uint8_t>(packedMask & 15u);
            highNibble = !highNibble;
            if (m) ++masksNonzero;

            auto delta = [](internal::DecodeCursor& input, std::int64_t& change, std::int64_t& state) {
                std::uint64_t raw{};
                if (!input.varint(raw)) return false;
                change = internal::decodeZigzag(raw);
                return internal::addI64(state, change, state);
            };
            if (m & 1u) { if (!delta(*bidInput, dbid, bid)) return Status::CorruptData; ++bidsChanged; }
            if (m & 2u) { if (!delta(*spreadInput, dspread, spread)) return Status::CorruptData; ++spreadsChanged; }
            if (m & 4u) { if (!delta(*bidQty, dbq, bq)) return Status::CorruptData; ++bidsQtyChanged; }
            if (m & 8u) { if (!delta(*askQty, daq, aq)) return Status::CorruptData; ++asksQtyChanged; }
        }
        std::int64_t bidValue{}, spreadValue{}, bqValue{}, aqValue{}, timeValue{};
        if (!internal::multiplyI64(bid, h.priceScale, bidValue) ||
            !internal::multiplyI64(spread, h.priceScale, spreadValue) ||
            !internal::multiplyI64(bq, h.qtyScale, bqValue) ||
            !internal::multiplyI64(aq, h.qtyScale, aqValue) ||
            !internal::multiplyI64(ts, h.timeScale, timeValue)) {
            return Status::CorruptData;
        }
        const auto askValue = std::bit_cast<std::int64_t>(
            std::bit_cast<std::uint64_t>(bidValue) + std::bit_cast<std::uint64_t>(spreadValue));

        if (outBlocks != nullptr) {
            decodedRows.push_back(Row{bidValue, bqValue, askValue, aqValue, timeValue});
        }
        if (encoded) {
            *encoded << (i ? ",\n" : "") << "    {\"dt\": " << dt << ", \"mask\": " << static_cast<unsigned>(m)
                     << ", \"d\": {\"bid\": " << dbid << ", \"spread\": " << dspread
                     << ", \"bid_qty\": " << dbq << ", \"ask_qty\": " << daq << "}"
                     << ", \"state\": [" << bidValue << "," << bqValue << "," << askValue
                     << "," << aqValue << "," << timeValue << "]}";
        }
    }

    if (time->remaining() || mask->remaining() || (highNibble && (packedMask >> 4u) != 0u)
        || bidInput->remaining() || spreadInput->remaining() || bidQty->remaining() || askQty->remaining()
        || masksNonzero != h.maskNonZero || bidsChanged != h.bidChanged || spreadsChanged != h.spreadChanged
        || bidsQtyChanged != h.bidQtyChanged || asksQtyChanged != h.askQtyChanged) {
        return Status::CorruptData;
    }

    if (outBlocks != nullptr) {
        const auto count = static_cast<std::size_t>(h.recordCount);
        const auto stride = static_cast<std::size_t>(h.layout.recordBytes);
        const bool hasTimestamp = h.layout.timestampOffset + 8u <= stride &&
            (h.layout.timestampOffset + 8u <= h.layout.payloadOffset ||
             h.layout.timestampOffset >= h.layout.payloadOffset + 32u);

        std::vector<std::uint8_t> rawRecords(count * stride);
        for (std::size_t col = 0u; col < stride; ++col) {
            std::uint8_t prevByte = 0u;
            for (std::size_t row = 0u; row < count; ++row) {
                std::uint8_t d{};
                if (!residual->byte(d)) return Status::CorruptData;
                prevByte = static_cast<std::uint8_t>(prevByte + d);
                rawRecords[row * stride + col] = prevByte;
            }
        }
        if (residual->remaining() != 0u) return Status::CorruptData;

        for (std::size_t row = 0u; row < count; ++row) {
            const std::size_t baseOff = row * stride;
            const std::size_t pOff = baseOff + h.layout.payloadOffset;
            for (unsigned f = 0u; f < 4u; ++f) {
                if (byte_block::word(rawRecords, pOff + 8u * f, 8u) != 0u) return Status::CorruptData;
            }
            byte_block::put(rawRecords, pOff, std::bit_cast<std::uint64_t>(decodedRows[row].bid), 8u);
            byte_block::put(rawRecords, pOff + 8u, std::bit_cast<std::uint64_t>(decodedRows[row].bidQty), 8u);
            byte_block::put(rawRecords, pOff + 16u, std::bit_cast<std::uint64_t>(decodedRows[row].ask), 8u);
            byte_block::put(rawRecords, pOff + 24u, std::bit_cast<std::uint64_t>(decodedRows[row].askQty), 8u);
            if (hasTimestamp) {
                if (byte_block::word(rawRecords, baseOff + h.layout.timestampOffset, 8u) != 0u) {
                    return Status::CorruptData;
                }
                byte_block::put(rawRecords, baseOff + h.layout.timestampOffset,
                                std::bit_cast<std::uint64_t>(decodedRows[row].ts), 8u);
            }
        }

        if (format::crc32c(rawRecords) != h.recordsCrc32c || !source.unchanged()) {
            return Status::CorruptData;
        }

        std::array<std::uint8_t, kBinarySliceHeaderBytes> sliceHdr{};
        if (!isOk(encodeBinarySliceHeader(h.layout, rawRecords, sliceHdr))) {
            return Status::CorruptData;
        }
        if (!(*outBlocks)(sliceHdr)) return Status::CallbackStopped;
        for (std::size_t pos = 0u; pos < rawRecords.size(); pos += 65536u) {
            const std::size_t chunkBytes = std::min<std::size_t>(65536u, rawRecords.size() - pos);
            if (!(*outBlocks)({rawRecords.data() + pos, chunkBytes})) return Status::CallbackStopped;
        }
    }

    if (encoded) *encoded << "\n  ]\n}\n";
    return Status::Ok;
}

std::string statsJson(const Header& h) {
    std::ostringstream o;
    o << "{\n  \"pipeline_id\": \"hftmac.bookticker_delta_mask\",\n  \"record_count\": " << h.recordCount
      << ",\n  \"record_bytes\": " << h.layout.recordBytes
      << ",\n  \"encoded_bytes\": " << h.outputBytes
      << ",\n  \"mask_nonzero_count\": " << h.maskNonZero
      << ",\n  \"bid_changed_count\": " << h.bidChanged
      << ",\n  \"spread_changed_count\": " << h.spreadChanged
      << ",\n  \"bid_qty_changed_count\": " << h.bidQtyChanged
      << ",\n  \"ask_qty_changed_count\": " << h.askQtyChanged
      << ",\n  \"time_stream_bytes\": " << h.timeBytes
      << ",\n  \"mask_stream_bytes\": " << h.maskBytes
      << ",\n  \"bid_delta_stream_bytes\": " << h.bidBytes
      << ",\n  \"spread_delta_stream_bytes\": " << h.spreadBytes
      << ",\n  \"bid_qty_delta_stream_bytes\": " << h.bidQtyBytes
      << ",\n  \"ask_qty_delta_stream_bytes\": " << h.askQtyBytes
      << ",\n  \"residual_stream_bytes\": " << h.residualBytes << "\n}\n";
    return o.str();
}

} // namespace

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
    if (layout.streamType != StreamType::BookTicker || !validByteBlockLayout(layout)) {
        outError = "invalid bookticker ByteBlockLayout";
        return Status::InvalidArgument;
    }
    std::size_t count = 0u;
    if (!math::exactRecordCount(records.size(), layout.recordBytes, count)) {
        outError = "bookticker slice byte size is not a non-zero multiple of layout.recordBytes";
        return Status::InvalidArgument;
    }

    const std::size_t stride = layout.recordBytes;
    const bool hasTimestamp = layout.timestampOffset + 8u <= stride &&
        (layout.timestampOffset + 8u <= layout.payloadOffset ||
         layout.timestampOffset >= layout.payloadOffset + 32u);

    std::vector<std::uint8_t> scratchRecords(records.begin(), records.end());
    std::vector<Row> rows(count);

    std::int64_t timeG = 0;
    std::int64_t priceG = 0;
    std::int64_t qtyG = 0;
    bool spreadsFitI64 = true;

    for (std::size_t i = 0u; i < count; ++i) {
        const std::size_t baseOff = i * stride;
        const std::size_t pOff = baseOff + layout.payloadOffset;
        auto& r = rows[i];
        r.bid = std::bit_cast<std::int64_t>(byte_block::word(records, pOff, 8u));
        r.bidQty = std::bit_cast<std::int64_t>(byte_block::word(records, pOff + 8u, 8u));
        r.ask = std::bit_cast<std::int64_t>(byte_block::word(records, pOff + 16u, 8u));
        r.askQty = std::bit_cast<std::int64_t>(byte_block::word(records, pOff + 24u, 8u));
        if (hasTimestamp) {
            r.ts = std::bit_cast<std::int64_t>(byte_block::word(records, baseOff + layout.timestampOffset, 8u));
            byte_block::put(scratchRecords, baseOff + layout.timestampOffset, 0u, 8u);
        }
        for (unsigned f = 0u; f < 4u; ++f) {
            byte_block::put(scratchRecords, pOff + 8u * f, 0u, 8u);
        }
        if (i == 0u) {
            timeG = r.ts;
        } else {
            std::int64_t dtDiff{};
            if (internal::subtractI64(r.ts, rows[0].ts, dtDiff)) {
                timeG = math::binaryGcdAbs(timeG, dtDiff);
            } else {
                timeG = 1;
            }
        }
        std::int64_t sprCheck{};
        if (!internal::subtractI64(r.ask, r.bid, sprCheck)) {
            spreadsFitI64 = false;
        }
        priceG = math::binaryGcdAbs(priceG, r.bid);
        priceG = math::binaryGcdAbs(priceG, r.ask);
        qtyG = math::binaryGcdAbs(qtyG, r.bidQty);
        qtyG = math::binaryGcdAbs(qtyG, r.askQty);
    }

    const auto es = timing::nowNs();
    const auto ec0 = timing::readCycles();

    Header h{};
    h.layout = layout;
    h.inputBytes = kBinarySliceHeaderBytes + records.size();
    h.recordCount = count;
    h.recordsCrc32c = format::crc32c(records);
    h.timeScale = math::safePositiveScale(timeG);
    h.priceScale = spreadsFitI64 ? math::safePositiveScale(priceG) : std::int64_t{1};
    h.qtyScale = math::safePositiveScale(qtyG);

    const auto timeDiv = math::ExactScaleDivisor::prepare(h.timeScale);
    const auto priceDiv = math::ExactScaleDivisor::prepare(h.priceScale);
    const auto qtyDiv = math::ExactScaleDivisor::prepare(h.qtyScale);

    const auto computeSpreadTick = [&](const Row& r) noexcept -> std::int64_t {
        if (spreadsFitI64) {
            return priceDiv.divideSignedExact(r.ask - r.bid);
        }
        return std::bit_cast<std::int64_t>(
            std::bit_cast<std::uint64_t>(r.ask) - std::bit_cast<std::uint64_t>(r.bid));
    };

    h.baseTsUnit = timeDiv.divideSignedExact(rows.front().ts);
    h.baseBidTick = priceDiv.divideSignedExact(rows.front().bid);
    h.baseSpreadTick = computeSpreadTick(rows.front());
    h.baseBidQtyLot = qtyDiv.divideSignedExact(rows.front().bidQty);
    h.baseAskQtyLot = qtyDiv.divideSignedExact(rows.front().askQty);

    std::vector<std::uint8_t> timeS, maskS, bidS, spreadS, bidQtyS, askQtyS, residualS;
    std::int64_t pts = h.baseTsUnit;
    std::int64_t pb = h.baseBidTick;
    std::int64_t pspr = h.baseSpreadTick;
    std::int64_t pbq = h.baseBidQtyLot;
    std::int64_t paq = h.baseAskQtyLot;
    bool maskHighNibble = false;

    for (std::size_t i = 1u; i < rows.size(); ++i) {
        const auto ts = timeDiv.divideSignedExact(rows[i].ts);
        const auto b = priceDiv.divideSignedExact(rows[i].bid);
        const auto spr = computeSpreadTick(rows[i]);
        const auto bq = qtyDiv.divideSignedExact(rows[i].bidQty);
        const auto aq = qtyDiv.divideSignedExact(rows[i].askQty);

        std::int64_t dt{}, db{}, dspr{}, dbq{}, daq{};
        if (!internal::subtractI64(ts, pts, dt) ||
            (b != pb && !internal::subtractI64(b, pb, db)) ||
            (spr != pspr && !internal::subtractI64(spr, pspr, dspr)) ||
            (bq != pbq && !internal::subtractI64(bq, pbq, dbq)) ||
            (aq != paq && !internal::subtractI64(aq, paq, daq))) {
            outError = "bookticker column delta overflow";
            return Status::InvalidArgument;
        }
        varint(timeS, zz(dt));

        std::uint8_t m = 0u;
        if (b != pb) m |= 1u;
        if (spr != pspr) m |= 2u;
        if (bq != pbq) m |= 4u;
        if (aq != paq) m |= 8u;
        pushPackedMask(maskS, m, maskHighNibble);
        if (m) ++h.maskNonZero;
        if (m & 1u) { varint(bidS, zz(db)); ++h.bidChanged; }
        if (m & 2u) { varint(spreadS, zz(dspr)); ++h.spreadChanged; }
        if (m & 4u) { varint(bidQtyS, zz(dbq)); ++h.bidQtyChanged; }
        if (m & 8u) { varint(askQtyS, zz(daq)); ++h.askQtyChanged; }
        pts = ts;
        pb = b;
        pspr = spr;
        pbq = bq;
        paq = aq;
    }

    residualS.reserve(scratchRecords.size());
    for (std::size_t col = 0u; col < stride; ++col) {
        std::uint8_t prevByte = 0u;
        for (std::size_t row = 0u; row < count; ++row) {
            const std::uint8_t cur = scratchRecords[row * stride + col];
            residualS.push_back(static_cast<std::uint8_t>(cur - prevByte));
            prevByte = cur;
        }
    }

    outEncodeCycles = timing::readCycles() - ec0;
    outEncodeCoreNs = timing::nowNs() - es;

    h.timeBytes = static_cast<std::uint32_t>(timeS.size());
    h.maskBytes = static_cast<std::uint32_t>(maskS.size());
    h.bidBytes = static_cast<std::uint32_t>(bidS.size());
    h.spreadBytes = static_cast<std::uint32_t>(spreadS.size());
    h.bidQtyBytes = static_cast<std::uint32_t>(bidQtyS.size());
    h.askQtyBytes = static_cast<std::uint32_t>(askQtyS.size());
    h.residualBytes = static_cast<std::uint32_t>(residualS.size());
    h.outputBytes = kHeaderBytes + h.timeBytes + h.maskBytes + h.bidBytes +
        h.spreadBytes + h.bidQtyBytes + h.askQtyBytes + h.residualBytes;
    h.headerCrc32c = computeHeaderCrc(h);

    const auto hb = serializeHeaderRaw(h, true);
    outEncoded.reserve(static_cast<std::size_t>(h.outputBytes));
    outEncoded.insert(outEncoded.end(), hb.begin(), hb.end());
    for (const auto* s : {&timeS, &maskS, &bidS, &spreadS, &bidQtyS, &askQtyS, &residualS}) {
        outEncoded.insert(outEncoded.end(), s->begin(), s->end());
    }
    outRecordCount = count;
    return Status::Ok;
}

CompressionResult compress(const CompressionRequest& request, const PipelineDescriptor& pipeline) noexcept try {
    if (request.inputPath.empty()) {
        return internal::fail(Status::InvalidArgument, request, &pipeline, "input path is empty");
    }
    if (inferStreamTypeFromPath(request.inputPath) != StreamType::BookTicker) {
        return internal::fail(Status::UnsupportedStream, request, &pipeline, "expected bookticker binary slice (.bin/.hbs)");
    }
    CompressionResult r{};
    internal::applyPipeline(r, &pipeline);
    r.streamType = StreamType::BookTicker;
    r.inputPath = request.inputPath;
    const auto total = timing::nowNs();

    std::vector<std::uint8_t> input;
    const auto rs = timing::nowNs();
    if (!internal::readFileBytes(request.inputPath, input)) {
        return internal::fail(Status::IoError, request, &pipeline, "failed to read input file");
    }
    r.readNs = timing::nowNs() - rs;
    r.inputBytes = input.size();

    const auto ps = timing::nowNs();
    BinarySliceView slice{};
    const auto sliceStatus = parseBinarySlice(input, slice);
    if (!isOk(sliceStatus) || slice.layout.streamType != StreamType::BookTicker) {
        return internal::fail(
            isOk(sliceStatus) ? Status::UnsupportedStream : sliceStatus,
            request,
            &pipeline,
            "input is not a valid canonical bookticker binary slice");
    }
    r.parseNs = timing::nowNs() - ps;

    std::vector<std::uint8_t> encoded;
    std::uint64_t recordCount = 0u;
    std::string encodeError;
    const auto encodeStatus = encodeSliceBytes(
        slice.layout, slice.records, encoded, recordCount, r.encodeCoreNs, r.encodeCycles, encodeError);
    if (!isOk(encodeStatus)) {
        return internal::fail(encodeStatus, request, &pipeline, encodeError);
    }

    const auto outPath = internal::outputPathFor(request, pipeline, StreamType::BookTicker);
    std::error_code dirEc;
    std::filesystem::create_directories(outPath.parent_path(), dirEc);
    if (dirEc) {
        return internal::fail(Status::IoError, request, &pipeline, "failed to create output directory");
    }

    r.outputPath = outPath;
    r.metricsPath = outPath.parent_path() / (outPath.stem().string() + ".metrics.json");
    const auto ws = timing::nowNs();
    std::ofstream out(outPath, std::ios::binary | std::ios::trunc);
    if (!out) {
        return internal::fail(Status::IoError, request, &pipeline, "failed to open output file");
    }
    out.write(reinterpret_cast<const char*>(encoded.data()), static_cast<std::streamsize>(encoded.size()));
    out.close();
    if (!out) {
        return internal::fail(Status::IoError, request, &pipeline, "failed to write output file");
    }
    r.writeNs = timing::nowNs() - ws;
    r.encodeNs = timing::nowNs() - total;
    r.outputBytes = encoded.size();
    r.lineCount = recordCount;
    r.blockCount = 1u;

    std::size_t decodedOffset = 0u;
    bool matches = true;
    const auto ds = timing::nowNs();
    const auto dc = timing::readCycles();
    const auto st = decode(encoded, [&](std::span<const std::uint8_t> block) {
        if (decodedOffset + block.size() > input.size()) {
            matches = false;
            return false;
        }
        matches = std::equal(block.begin(), block.end(), input.begin() + static_cast<std::ptrdiff_t>(decodedOffset));
        decodedOffset += block.size();
        return matches;
    });
    r.decodeCycles = timing::readCycles() - dc;
    r.decodeNs = timing::nowNs() - ds;
    r.decodeCoreNs = r.decodeNs;
    r.roundtripOk = isOk(st) && matches && decodedOffset == input.size();
    r.status = r.roundtripOk ? Status::Ok : Status::DecodeError;
    if (!r.roundtripOk) r.error = "roundtrip check failed";

    (void)internal::writeTextFile(r.metricsPath, toMetricsJson(r));
    return r;
} catch (...) {
    CompressionResult failed{};
    failed.status = Status::DecodeError;
    return failed;
}

ReplayArtifactInfo inspectArtifact(const std::filesystem::path& path, const PipelineDescriptor& pipeline) noexcept try {
    internal::FileDecodeSource source(path);
    std::array<std::uint8_t, kHeaderBytes> data{};
    ReplayArtifactInfo i{};
    i.path = path;
    if (!source.valid() || !source.read(0u, data)) {
        i.status = Status::IoError;
        i.error = "failed to read artifact";
        return i;
    }
    Header h{};
    if (!readHeader(data, h) || h.outputBytes != source.size() || !source.unchanged()) {
        i.status = Status::CorruptData;
        i.error = "invalid bookticker artifact";
        return i;
    }
    i.status = Status::Ok;
    i.found = true;
    i.formatId = "hftmac.bookticker_delta_mask";
    i.pipelineId = std::string{pipeline.id};
    i.transform = std::string{pipeline.transform};
    i.entropy = std::string{pipeline.entropy};
    i.streamType = StreamType::BookTicker;
    i.inputBytes = h.inputBytes;
    i.outputBytes = h.outputBytes;
    i.lineCount = h.recordCount;
    i.blockCount = 1u;
    return i;
} catch (...) {
    ReplayArtifactInfo failed{};
    failed.status = Status::DecodeError;
    return failed;
}

Status decodeSource(const internal::DecodeSource& source, const DecodedBlockCallback& onBlock) {
    if (!onBlock) return Status::InvalidArgument;
    return decodeColumns(source, &onBlock, nullptr);
}

Status decode(std::span<const std::uint8_t> bytes, const DecodedBlockCallback& onBlock) noexcept {
    if (!onBlock) return Status::InvalidArgument;
    try {
        internal::SpanDecodeSource source(bytes);
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

Status inspectEncodedJsonFile(const std::filesystem::path& path, const DecodedBlockCallback& cb) noexcept try {
    if (!cb) return Status::InvalidArgument;
    internal::FileDecodeSource source(path);
    if (!source.valid()) return Status::IoError;
    std::ostringstream out;
    const auto st = decodeColumns(source, nullptr, &out);
    if (!isOk(st)) return st;
    if (!source.unchanged()) return Status::CorruptData;
    return emitText(out.str(), cb);
} catch (...) {
    return Status::DecodeError;
}

Status inspectEncodedBinaryFile(const std::filesystem::path& path, const DecodedBlockCallback& cb) noexcept try {
    if (!cb) return Status::InvalidArgument;
    internal::FileDecodeSource source(path);
    std::array<std::uint8_t, kHeaderBytes> data{};
    if (!source.valid() || !source.read(0u, data)) return Status::IoError;
    Header h{};
    if (!readHeader(data, h) || h.outputBytes != source.size() || !source.unchanged()) return Status::CorruptData;
    std::ostringstream o;
    o << "bookticker_delta_mask"
      << " bytes=" << source.size()
      << " header=" << kHeaderBytes
      << " time=" << h.timeBytes
      << " mask=" << h.maskBytes
      << " bid=" << h.bidBytes
      << " spread=" << h.spreadBytes
      << " bid_qty=" << h.bidQtyBytes
      << " ask_qty=" << h.askQtyBytes
      << " residual=" << h.residualBytes << "\n";
    return emitText(o.str(), cb);
} catch (...) {
    return Status::DecodeError;
}

Status inspectStatsJsonFile(const std::filesystem::path& path, const DecodedBlockCallback& cb) noexcept try {
    if (!cb) return Status::InvalidArgument;
    internal::FileDecodeSource source(path);
    std::array<std::uint8_t, kHeaderBytes> data{};
    if (!source.valid() || !source.read(0u, data)) return Status::IoError;
    Header h{};
    if (!readHeader(data, h) || h.outputBytes != source.size() || !source.unchanged()) return Status::CorruptData;
    return emitText(statsJson(h), cb);
} catch (...) {
    return Status::DecodeError;
}

}  // namespace hft_compressor::codecs::bookticker_delta_mask

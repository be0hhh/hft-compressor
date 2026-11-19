#include "DepthLadderOffset.hpp"
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
#include <utility>
#include <vector>

namespace hft_compressor::codecs::depth_ladder_offset {
namespace {

constexpr std::uint32_t kMagic = 0x00454443u; // CDE
constexpr std::size_t kHeaderBytes = 208u;

constexpr std::uint32_t kHotQtyCount = 64u;
constexpr std::uint32_t kInspectBatchLimit = 128u;

struct Level {
    std::int64_t price{0};
    std::int64_t qty{0};
    std::uint8_t side{0};
};

struct Batch {
    std::int64_t ts{0};
    std::vector<Level> levels{};
};

struct Header {
    std::uint32_t magic{kMagic};
    std::uint16_t bidHotCount{0};
    ByteBlockLayout layout{};
    std::uint64_t inputBytes{0};
    std::uint64_t outputBytes{0};
    std::uint64_t batchCount{0};
    std::uint64_t levelCount{0};
    std::int64_t timeScale{1};
    std::int64_t priceScale{1};
    std::int64_t qtyScale{1};
    std::int64_t baseTsUnit{0};
    std::uint32_t hotQtyCount{0};
    std::uint32_t hotQtyBytes{0};
    std::uint32_t batchBytes{0};
    std::uint32_t sideBytes{0};
    std::uint32_t priceModeBytes{0};
    std::uint32_t deleteBytes{0};
    std::uint32_t priceBytes{0};
    std::uint32_t qtyCodeBytes{0};
    std::uint32_t qtyEscapeBytes{0};
    std::uint32_t residualBytes{0};
    std::uint32_t deleteCount{0};
    std::uint32_t qtyEscapeCount{0};
    std::uint32_t runModeBatchCount{0};
    std::uint32_t fallbackBatchCount{0};
    std::uint32_t offsetPriceCount{0};
    std::uint32_t absolutePriceCount{0};
    std::uint32_t recordsCrc32c{0};
    std::uint32_t headerCrc32c{0};
};

std::uint64_t zigzag(std::int64_t value) noexcept {
    return (static_cast<std::uint64_t>(value) << 1u) ^ static_cast<std::uint64_t>(value >> 63u);
}

void writeVarint(std::vector<std::uint8_t>& out, std::uint64_t value) {
    while (value >= 0x80u) {
        out.push_back(static_cast<std::uint8_t>(value | 0x80u));
        value >>= 7u;
    }
    out.push_back(static_cast<std::uint8_t>(value));
}

template <class T>
void writeLe(std::vector<std::uint8_t>& out, T value) {
    using U = std::make_unsigned_t<T>;
    U raw = static_cast<U>(value);
    for (std::size_t i = 0; i < sizeof(T); ++i) {
        out.push_back(static_cast<std::uint8_t>((raw >> (i * 8u)) & 0xffu));
    }
}

template <class T>
bool readLe(const std::uint8_t*& p, const std::uint8_t* end, T& out) noexcept {
    if (static_cast<std::size_t>(end - p) < sizeof(T)) return false;
    using U = std::make_unsigned_t<T>;
    U raw = 0;
    for (std::size_t i = 0; i < sizeof(T); ++i) {
        raw |= static_cast<U>(*p++) << (i * 8u);
    }
    out = static_cast<T>(raw);
    return true;
}

struct BitWriter {
    std::vector<std::uint8_t> bytes;
    std::uint8_t current{0};
    unsigned used{0};

    void bit(bool value) {
        if (value) current |= static_cast<std::uint8_t>(1u << used);
        if (++used == 8u) {
            bytes.push_back(current);
            current = 0;
            used = 0;
        }
    }

    std::vector<std::uint8_t> finish() {
        if (used != 0u) bytes.push_back(current);
        return std::move(bytes);
    }
};

// Flat sorted vector order-book state:
// - `bid` sorted ascending by price -> bestBid == bid.back().first in O(1).
// - `ask` sorted descending by price -> bestAsk == ask.back().first in O(1).
// Top-of-book updates mutate near vector tails with zero hash-node allocations.
struct BookState {
    std::vector<std::pair<std::int64_t, std::int64_t>> bid{};
    std::vector<std::pair<std::int64_t, std::int64_t>> ask{};
    bool haveBid{false};
    bool haveAsk{false};
    std::int64_t bestBid{0};
    std::int64_t bestAsk{0};

    void recompute() noexcept {
        haveBid = !bid.empty();
        haveAsk = !ask.empty();
        if (haveBid) bestBid = bid.back().first;
        if (haveAsk) bestAsk = ask.back().first;
    }

    void apply(std::uint8_t side, std::int64_t price, std::int64_t qty) {
        if (side >= 2u) return;
        if (side == 0u) {
            const auto it = std::lower_bound(
                bid.begin(), bid.end(), price,
                [](const auto& item, std::int64_t target) { return item.first < target; });
            if (qty == 0) {
                if (it != bid.end() && it->first == price) bid.erase(it);
            } else if (it != bid.end() && it->first == price) {
                it->second = qty;
            } else {
                bid.insert(it, {price, qty});
            }
        } else {
            const auto it = std::lower_bound(
                ask.begin(), ask.end(), price,
                [](const auto& item, std::int64_t target) { return item.first > target; });
            if (qty == 0) {
                if (it != ask.end() && it->first == price) ask.erase(it);
            } else if (it != ask.end() && it->first == price) {
                it->second = qty;
            } else {
                ask.insert(it, {price, qty});
            }
        }
    }
};

std::vector<std::int64_t> buildHotQtySorted(
    std::vector<std::int64_t>& rawQuantities,
    const math::ExactScaleDivisor& qtyDiv) {
    if (rawQuantities.empty()) return {};
    std::sort(rawQuantities.begin(), rawQuantities.end());
    std::vector<std::pair<std::int64_t, std::uint32_t>> freq;
    for (std::size_t i = 0; i < rawQuantities.size();) {
        std::size_t end = i + 1u;
        while (end < rawQuantities.size() && rawQuantities[end] == rawQuantities[i]) ++end;
        freq.push_back({qtyDiv.divideSignedExact(rawQuantities[i]), static_cast<std::uint32_t>(end - i)});
        i = end;
    }
    std::sort(freq.begin(), freq.end(), [](const auto& lhs, const auto& rhs) {
        return lhs.second == rhs.second ? lhs.first < rhs.first : lhs.second > rhs.second;
    });
    std::vector<std::int64_t> out;
    const std::size_t take = std::min<std::size_t>(freq.size(), kHotQtyCount);
    out.reserve(take);
    for (std::size_t i = 0; i < take; ++i) out.push_back(freq[i].first);
    return out;
}

std::uint64_t qtyCode(std::span<const std::int64_t> hot, std::int64_t qty) noexcept {
    for (std::size_t i = 0; i < hot.size(); ++i) {
        if (hot[i] == qty) return i;
    }
    return hot.size();
}

void writeQty(std::uint8_t side,
              std::int64_t qty,
              std::span<const std::int64_t> bidHot,
              std::span<const std::int64_t> askHot,
              std::vector<std::uint8_t>& codeStream,
              std::vector<std::uint8_t>& escapeStream,
              Header& h) {
    const auto hot = (side == 0u) ? bidHot : askHot;
    const auto code = qtyCode(hot, qty);
    writeVarint(codeStream, code);
    if (code == hot.size()) {
        writeVarint(escapeStream, zigzag(qty));
        ++h.qtyEscapeCount;
    }
}

bool runModeCandidate(const Batch& batch,
                      const BookState& state,
                      const math::ExactScaleDivisor& priceDiv,
                      std::uint64_t& bidCount,
                      std::uint64_t& askCount) noexcept {
    if (batch.levels.empty()) return false;
    bidCount = 0u;
    askCount = 0u;
    bool seenAsk = false;
    std::int64_t prevBid = -1;
    std::int64_t prevAsk = -1;
    for (const auto& raw : batch.levels) {
        if (raw.side >= 2u) return false;
        const auto price = priceDiv.divideSignedExact(raw.price);
        if (raw.side == 0u) {
            if (seenAsk || !state.haveBid) return false;
            std::int64_t off{};
            if (!internal::subtractI64(state.bestBid, price, off) || off < 0 || off < prevBid) return false;
            prevBid = off;
            ++bidCount;
        } else {
            seenAsk = true;
            if (!state.haveAsk) return false;
            std::int64_t off{};
            if (!internal::subtractI64(price, state.bestAsk, off) || off < 0 || off < prevAsk) return false;
            prevAsk = off;
            ++askCount;
        }
    }
    return bidCount != 0u || askCount != 0u;
}

std::vector<std::uint8_t> serializeHeaderRaw(Header h, bool includeCrc) {
    if (!includeCrc) h.headerCrc32c = 0u;
    std::vector<std::uint8_t> out;
    out.reserve(kHeaderBytes);
    writeLe(out, h.magic);
    writeLe(out, h.bidHotCount);
    writeLe(out, static_cast<std::uint32_t>(h.layout.streamType));
    writeLe(out, h.layout.recordBytes);
    writeLe(out, h.layout.payloadOffset);
    writeLe(out, h.layout.timestampOffset);
    writeLe(out, h.layout.depthLevelsOffset);
    writeLe(out, h.layout.depthLevelStride);
    writeLe(out, h.layout.depthLevelCountOffset);
    writeLe(out, h.layout.depthLevelCapacity);
    writeLe(out, h.inputBytes);
    writeLe(out, h.outputBytes);
    writeLe(out, h.batchCount);
    writeLe(out, h.levelCount);
    writeLe(out, h.timeScale);
    writeLe(out, h.priceScale);
    writeLe(out, h.qtyScale);
    writeLe(out, h.baseTsUnit);
    writeLe(out, h.hotQtyCount);
    writeLe(out, h.hotQtyBytes);
    writeLe(out, h.batchBytes);
    writeLe(out, h.sideBytes);
    writeLe(out, h.priceModeBytes);
    writeLe(out, h.deleteBytes);
    writeLe(out, h.priceBytes);
    writeLe(out, h.qtyCodeBytes);
    writeLe(out, h.qtyEscapeBytes);
    writeLe(out, h.residualBytes);
    writeLe(out, h.deleteCount);
    writeLe(out, h.qtyEscapeCount);
    writeLe(out, h.runModeBatchCount);
    writeLe(out, h.fallbackBatchCount);
    writeLe(out, h.offsetPriceCount);
    writeLe(out, h.absolutePriceCount);
    writeLe(out, h.recordsCrc32c);
    writeLe(out, h.headerCrc32c);
    out.resize(kHeaderBytes, 0u);
    return out;
}

std::uint32_t computeHeaderCrc(const Header& h) {
    return format::crc32c(serializeHeaderRaw(h, false));
}

bool readHeader(std::span<const std::uint8_t> data, Header& h) noexcept {
    if (data.size() < kHeaderBytes) return false;
    const auto* p = data.data();
    const auto* end = data.data() + kHeaderBytes;
    std::uint32_t streamRaw = 0u;
    const bool ok = readLe(p, end, h.magic)
        && readLe(p, end, h.bidHotCount)
        && readLe(p, end, streamRaw)
        && readLe(p, end, h.layout.recordBytes)
        && readLe(p, end, h.layout.payloadOffset)
        && readLe(p, end, h.layout.timestampOffset)
        && readLe(p, end, h.layout.depthLevelsOffset)
        && readLe(p, end, h.layout.depthLevelStride)
        && readLe(p, end, h.layout.depthLevelCountOffset)
        && readLe(p, end, h.layout.depthLevelCapacity)
        && readLe(p, end, h.inputBytes)
        && readLe(p, end, h.outputBytes)
        && readLe(p, end, h.batchCount)
        && readLe(p, end, h.levelCount)
        && readLe(p, end, h.timeScale)
        && readLe(p, end, h.priceScale)
        && readLe(p, end, h.qtyScale)
        && readLe(p, end, h.baseTsUnit)
        && readLe(p, end, h.hotQtyCount)
        && readLe(p, end, h.hotQtyBytes)
        && readLe(p, end, h.batchBytes)
        && readLe(p, end, h.sideBytes)
        && readLe(p, end, h.priceModeBytes)
        && readLe(p, end, h.deleteBytes)
        && readLe(p, end, h.priceBytes)
        && readLe(p, end, h.qtyCodeBytes)
        && readLe(p, end, h.qtyEscapeBytes)
        && readLe(p, end, h.residualBytes)
        && readLe(p, end, h.deleteCount)
        && readLe(p, end, h.qtyEscapeCount)
        && readLe(p, end, h.runModeBatchCount)
        && readLe(p, end, h.fallbackBatchCount)
        && readLe(p, end, h.offsetPriceCount)
        && readLe(p, end, h.absolutePriceCount)
        && readLe(p, end, h.recordsCrc32c)
        && readLe(p, end, h.headerCrc32c);
    if (!ok || !std::all_of(p, end, [](auto b) { return b == 0u; })) return false;
    h.layout.streamType = static_cast<StreamType>(streamRaw);
    return h.magic == kMagic
        && h.layout.streamType == StreamType::Depth
        && validByteBlockLayout(h.layout)
        && h.hotQtyCount <= kHotQtyCount * 2u
        && h.bidHotCount <= h.hotQtyCount
        && h.hotQtyBytes == h.hotQtyCount * sizeof(std::int64_t)
        && h.headerCrc32c == computeHeaderCrc(h);
}

Status emitText(const std::string& text, const DecodedBlockCallback& onBlock) {
    return onBlock(std::span<const std::uint8_t>{
        reinterpret_cast<const std::uint8_t*>(text.data()), text.size()})
        ? Status::Ok
        : Status::CallbackStopped;
}

Status decodeColumns(const internal::DecodeSource& source,
                     const DecodedBlockCallback* outBlocks,
                     std::ostream* encoded) {
    std::array<std::uint8_t, kHeaderBytes> bytes{};
    Header h{};
    if (!source.read(0u, bytes) || !readHeader(bytes, h) || h.outputBytes != source.size()
        || h.priceModeBytes != 0u || h.timeScale <= 0 || h.priceScale <= 0 || h.qtyScale <= 0
        || h.batchCount == 0u || h.batchCount > std::numeric_limits<std::uint32_t>::max()
        || h.batchCount > std::numeric_limits<std::uint64_t>::max() / 3u
        || h.batchCount * 3u > h.batchBytes
        || h.levelCount > h.priceBytes
        || h.deleteBytes != ((h.levelCount + 7u) >> 3u)) {
        return Status::CorruptData;
    }

    const std::uint64_t rawRecordBytes = h.batchCount * h.layout.recordBytes;
    if (h.residualBytes != rawRecordBytes ||
        h.inputBytes != kBinarySliceHeaderBytes + rawRecordBytes) {
        return Status::CorruptData;
    }

    std::uint64_t offset = kHeaderBytes;
    std::array<std::int64_t, kHotQtyCount * 2u> hot{};
    std::array<std::uint8_t, kHotQtyCount * 2u * sizeof(std::int64_t)> hotBytes{};
    if (!source.read(offset, std::span{hotBytes}.first(h.hotQtyBytes))) return Status::CorruptData;
    const auto* hp = hotBytes.data();
    const auto* he = hp + h.hotQtyBytes;
    for (std::uint32_t i = 0; i < h.hotQtyCount; ++i) {
        if (!readLe(hp, he, hot[i])) return Status::CorruptData;
    }
    offset += h.hotQtyBytes;

    const std::span<const std::int64_t> bidHot{hot.data(), h.bidHotCount};
    const std::span<const std::int64_t> askHot{hot.data() + h.bidHotCount, h.hotQtyCount - h.bidHotCount};

    auto next = [&](std::uint32_t count) {
        auto result = source.cursor(offset, count);
        if (source.contains(offset, count)) offset += count;
        return result;
    };
    auto batch = next(h.batchBytes);
    auto side = next(h.sideBytes);
    auto priceMode = next(h.priceModeBytes);
    auto deleted = next(h.deleteBytes);
    auto price = next(h.priceBytes);
    auto code = next(h.qtyCodeBytes);
    auto escape = next(h.qtyEscapeBytes);
    auto residual = next(h.residualBytes);
    if (!batch || !side || !priceMode || !deleted || !price || !code || !escape || !residual ||
        offset != source.size()) {
        return Status::CorruptData;
    }

    internal::DecodeBits sideBits{*side}, deleteBits{*deleted};
    BookState state;
    std::int64_t ts = h.baseTsUnit;
    std::uint64_t totalLevels{}, deletes{}, escapes{}, runs{}, explicitBatches{}, offsetPrices{}, absolutePrices{};

    struct DecodedBatchMeta {
        std::int64_t timeValue{};
        std::vector<Level> levels{};
    };
    std::vector<DecodedBatchMeta> decodedBatches;

    if (encoded) {
        *encoded << "{\n  \"pipeline_id\": \"hftmac.depth_ladder_offset\",\n  \"batch_count\": " << h.batchCount
                 << ",\n  \"hot_qty\": {\"bid\": [";
        for (std::size_t i = 0; i < bidHot.size(); ++i) *encoded << (i ? ", " : "") << bidHot[i];
        *encoded << "], \"ask\": [";
        for (std::size_t i = 0; i < askHot.size(); ++i) *encoded << (i ? ", " : "") << askHot[i];
        *encoded << "]},\n  \"batches\": [\n";
    }

    for (std::uint64_t bi = 0; bi < h.batchCount; ++bi) {
        std::uint64_t rawDt{}, levelCount{}, mode{};
        if (!batch->varint(rawDt) || !batch->varint(levelCount) || !batch->varint(mode) || mode > 1u
            || levelCount > h.layout.depthLevelCapacity
            || levelCount > h.levelCount - totalLevels
            || levelCount > price->remaining()) {
            return Status::CorruptData;
        }
        const std::int64_t dt = internal::decodeZigzag(rawDt);
        if (!internal::addI64(ts, dt, ts)) return Status::CorruptData;

        const bool runMode = mode != 0u;
        std::uint64_t bidCount{}, askCount{};
        if (runMode) {
            if (levelCount == 0u || !batch->varint(bidCount) || !batch->varint(askCount) ||
                bidCount > levelCount || askCount != levelCount - bidCount ||
                (bidCount && !state.haveBid) || (askCount && !state.haveAsk)) {
                return Status::CorruptData;
            }
            ++runs;
        } else {
            ++explicitBatches;
        }

        const auto bestBid = state.bestBid;
        const auto bestAsk = state.bestAsk;
        const bool haveBid = state.haveBid;
        const bool haveAsk = state.haveAsk;
        if (encoded && bi < kInspectBatchLimit) {
            *encoded << (bi ? ",\n" : "") << "    {\"dt\": " << dt
                     << ", \"level_count\": " << levelCount
                     << ", \"mode\": \"" << (runMode ? "side_runs_offset_gaps" : "explicit")
                     << "\", \"best_before\": {\"bid\": " << bestBid << ", \"ask\": " << bestAsk << "}, \"levels\": [";
        }

        DecodedBatchMeta batchMeta{};
        if (!internal::multiplyI64(ts, h.timeScale, batchMeta.timeValue)) return Status::CorruptData;

        std::uint64_t emittedLevels{};
        auto readOne = [&](std::uint8_t sideValue, bool offsetMode, std::int64_t& prevOffset, std::uint64_t localIndex) -> Status {
            if (!offsetMode) {
                std::uint64_t sBit{};
                if (!sideBits.bits(1u, sBit)) return Status::CorruptData;
                sideValue = static_cast<std::uint8_t>(sBit);
            }
            std::uint64_t rawPrice{};
            if (!price->varint(rawPrice)) return Status::CorruptData;
            std::int64_t offsetValue{}, priceTick{};
            const bool haveSide = (sideValue == 0u) ? haveBid : (sideValue == 1u ? haveAsk : false);
            if (offsetMode) {
                if (rawPrice > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())) {
                    return Status::CorruptData;
                }
                offsetValue = static_cast<std::int64_t>(rawPrice);
                if (localIndex != 0u && !internal::addI64(prevOffset, offsetValue, offsetValue)) {
                    return Status::CorruptData;
                }
                prevOffset = offsetValue;
            } else {
                offsetValue = internal::decodeZigzag(rawPrice);
            }
            if (offsetMode || haveSide) {
                const bool ok = (sideValue == 0u)
                    ? internal::subtractI64(bestBid, offsetValue, priceTick)
                    : internal::addI64(bestAsk, offsetValue, priceTick);
                if (!ok) return Status::CorruptData;
                ++offsetPrices;
            } else {
                priceTick = offsetValue;
                ++absolutePrices;
            }
            std::uint64_t deleteValue{};
            if (!deleteBits.bits(1u, deleteValue)) return Status::CorruptData;
            std::int64_t qty{};
            if (deleteValue) {
                ++deletes;
            } else {
                const auto table = (sideValue == 0u) ? bidHot : askHot;
                std::uint64_t rawCode{};
                if (!code->varint(rawCode)) return Status::CorruptData;
                if (rawCode < table.size()) {
                    qty = table[static_cast<std::size_t>(rawCode)];
                } else {
                    std::uint64_t rawQty{};
                    if (rawCode != table.size() || !escape->varint(rawQty)) return Status::CorruptData;
                    qty = internal::decodeZigzag(rawQty);
                    ++escapes;
                }
            }
            std::int64_t priceValue{}, qtyValue{};
            if (!internal::multiplyI64(priceTick, h.priceScale, priceValue) ||
                !internal::multiplyI64(qty, h.qtyScale, qtyValue)) {
                return Status::CorruptData;
            }
            if (encoded && bi < kInspectBatchLimit) {
                *encoded << (emittedLevels ? ", " : "")
                         << "{\"side\": " << static_cast<unsigned>(sideValue)
                         << ", \"price\": " << priceTick
                         << ", \"offset\": " << (offsetMode ? offsetValue : 0)
                         << ", \"delete\": " << (deleteValue ? "true" : "false")
                         << ", \"qty\": " << qty << "}";
            }
            state.apply(sideValue, priceTick, qty);
            if (outBlocks != nullptr) {
                batchMeta.levels.push_back(Level{priceValue, qtyValue, sideValue});
            }
            ++emittedLevels;
            ++totalLevels;
            return Status::Ok;
        };

        std::int64_t prev{};
        if (runMode) {
            for (std::uint64_t i = 0; i < bidCount; ++i) {
                const auto st = readOne(0u, true, prev, i);
                if (!isOk(st)) return st;
            }
            prev = 0;
            for (std::uint64_t i = 0; i < askCount; ++i) {
                const auto st = readOne(1u, true, prev, i);
                if (!isOk(st)) return st;
            }
        } else {
            for (std::uint64_t i = 0; i < levelCount; ++i) {
                const auto st = readOne(0u, false, prev, 0u);
                if (!isOk(st)) return st;
            }
        }
        if (encoded && bi < kInspectBatchLimit) *encoded << "]}";
        state.recompute();
        if (outBlocks != nullptr) {
            decodedBatches.push_back(std::move(batchMeta));
        }
    }

    if (batch->remaining() || price->remaining() || code->remaining() || escape->remaining()
        || !sideBits.finished() || !deleteBits.finished() || totalLevels != h.levelCount
        || deletes != h.deleteCount || escapes != h.qtyEscapeCount || runs != h.runModeBatchCount
        || explicitBatches != h.fallbackBatchCount || offsetPrices != h.offsetPriceCount
        || absolutePrices != h.absolutePriceCount) {
        return Status::CorruptData;
    }

    if (outBlocks != nullptr) {
        const auto count = static_cast<std::size_t>(h.batchCount);
        const auto stride = static_cast<std::size_t>(h.layout.recordBytes);
        std::vector<std::uint8_t> rawRecords(count * stride);
        for (std::size_t col = 0u; col < stride; ++col) {
            std::uint8_t prevByte = 0u;
            for (std::size_t row = 0u; row < count; ++row) {
                std::uint8_t delta{};
                if (!residual->byte(delta)) return Status::CorruptData;
                prevByte = static_cast<std::uint8_t>(prevByte + delta);
                rawRecords[row * stride + col] = prevByte;
            }
        }
        if (residual->remaining() != 0u) return Status::CorruptData;

        for (std::size_t row = 0u; row < count; ++row) {
            const std::size_t baseOff = row * stride;
            const auto storedCount = byte_block::word(rawRecords, baseOff + h.layout.depthLevelCountOffset, 2u);
            const auto& meta = decodedBatches[row];
            if (storedCount != meta.levels.size()) return Status::CorruptData;
            if (h.layout.timestampOffset + 8u <= stride) {
                if (byte_block::word(rawRecords, baseOff + h.layout.timestampOffset, 8u) != 0u) {
                    return Status::CorruptData;
                }
                byte_block::put(rawRecords, baseOff + h.layout.timestampOffset,
                                std::bit_cast<std::uint64_t>(meta.timeValue), 8u);
            }
            for (std::size_t li = 0u; li < meta.levels.size(); ++li) {
                const std::size_t loff = baseOff + h.layout.depthLevelsOffset + li * h.layout.depthLevelStride;
                if (byte_block::word(rawRecords, loff, 8u) != 0u ||
                    byte_block::word(rawRecords, loff + 8u, 8u) != 0u) {
                    return Status::CorruptData;
                }
                byte_block::put(rawRecords, loff, std::bit_cast<std::uint64_t>(meta.levels[li].price), 8u);
                byte_block::put(rawRecords, loff + 8u, std::bit_cast<std::uint64_t>(meta.levels[li].qty), 8u);
                if (rawRecords[loff + 16u] < 2u && rawRecords[loff + 16u] != meta.levels[li].side) {
                    return Status::CorruptData;
                }
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

    if (encoded) {
        if (h.batchCount > kInspectBatchLimit) {
            *encoded << "\n  ],\n  \"truncated\": true,\n  \"shown_batches\": " << kInspectBatchLimit << "\n}\n";
        } else {
            *encoded << "\n  ],\n  \"truncated\": false\n}\n";
        }
    }
    return Status::Ok;
}

std::string statsJson(const Header& h) {
    std::ostringstream out;
    out << "{\n  \"pipeline_id\": \"hftmac.depth_ladder_offset\",\n  \"batch_count\": " << h.batchCount
        << ",\n  \"total_level_count\": " << h.levelCount
        << ",\n  \"record_bytes\": " << h.layout.recordBytes
        << ",\n  \"encoded_bytes\": " << h.outputBytes
        << ",\n  \"delete_count\": " << h.deleteCount
        << ",\n  \"qty_escape_count\": " << h.qtyEscapeCount
        << ",\n  \"run_mode_batch_count\": " << h.runModeBatchCount
        << ",\n  \"fallback_batch_count\": " << h.fallbackBatchCount
        << ",\n  \"offset_price_count\": " << h.offsetPriceCount
        << ",\n  \"absolute_price_count\": " << h.absolutePriceCount
        << ",\n  \"batch_stream_bytes\": " << h.batchBytes
        << ",\n  \"side_stream_bytes\": " << h.sideBytes
        << ",\n  \"delete_bit_stream_bytes\": " << h.deleteBytes
        << ",\n  \"price_stream_bytes\": " << h.priceBytes
        << ",\n  \"qty_code_stream_bytes\": " << h.qtyCodeBytes
        << ",\n  \"qty_escape_stream_bytes\": " << h.qtyEscapeBytes
        << ",\n  \"residual_stream_bytes\": " << h.residualBytes << "\n}\n";
    return out.str();
}

}  // namespace

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
    if (layout.streamType != StreamType::Depth || !validByteBlockLayout(layout)) {
        outError = "invalid depth ByteBlockLayout";
        return Status::InvalidArgument;
    }
    std::size_t count = 0u;
    if (!math::exactRecordCount(records.size(), layout.recordBytes, count)) {
        outError = "depth slice byte size is not a non-zero multiple of layout.recordBytes";
        return Status::InvalidArgument;
    }

    const std::size_t stride = layout.recordBytes;
    const bool hasTimestamp = layout.timestampOffset + 8u <= stride &&
        (layout.timestampOffset + 8u <= layout.depthLevelCountOffset ||
         layout.timestampOffset >= layout.depthLevelsOffset +
             std::size_t{layout.depthLevelStride} * layout.depthLevelCapacity);

    std::vector<std::uint8_t> scratchRecords(records.begin(), records.end());
    std::vector<Batch> batches(count);
    std::vector<std::int64_t> bidRawQtys;
    std::vector<std::int64_t> askRawQtys;

    Header h{};
    h.layout = layout;
    h.inputBytes = kBinarySliceHeaderBytes + records.size();
    h.batchCount = count;
    h.recordsCrc32c = format::crc32c(records);

    std::int64_t firstTs = 0;
    std::int64_t timeScale = 0;
    std::int64_t priceScale = 0;
    std::int64_t qtyScale = 0;

    for (std::size_t row = 0u; row < count; ++row) {
        const std::size_t baseOff = row * stride;
        const auto levelCount = byte_block::word(records, baseOff + layout.depthLevelCountOffset, 2u);
        if (levelCount > layout.depthLevelCapacity) {
            outError = "depth record levelCount exceeds layout.depthLevelCapacity";
            return Status::CorruptData;
        }
        auto& batch = batches[row];
        if (hasTimestamp) {
            batch.ts = std::bit_cast<std::int64_t>(
                byte_block::word(records, baseOff + layout.timestampOffset, 8u));
            byte_block::put(scratchRecords, baseOff + layout.timestampOffset, 0u, 8u);
        }
        if (row == 0u) {
            firstTs = batch.ts;
            timeScale = firstTs;
        } else {
            std::int64_t diff{};
            if (internal::subtractI64(batch.ts, firstTs, diff)) {
                timeScale = math::binaryGcdAbs(timeScale, diff);
            } else {
                timeScale = 1;
            }
        }
        batch.levels.resize(static_cast<std::size_t>(levelCount));
        h.levelCount += levelCount;
        for (std::size_t li = 0u; li < levelCount; ++li) {
            const std::size_t loff = baseOff + layout.depthLevelsOffset + li * layout.depthLevelStride;
            const auto p = std::bit_cast<std::int64_t>(byte_block::word(records, loff, 8u));
            const auto q = std::bit_cast<std::int64_t>(byte_block::word(records, loff + 8u, 8u));
            const std::uint8_t s = records[loff + 16u];
            const std::uint8_t codedSide = (s == 1u) ? 1u : 0u;
            batch.levels[li] = Level{p, q, codedSide};
            priceScale = math::binaryGcdAbs(priceScale, p);
            qtyScale = math::binaryGcdAbs(qtyScale, q);
            if (q != 0) {
                if (codedSide == 0u) bidRawQtys.push_back(q);
                else askRawQtys.push_back(q);
            }
            byte_block::put(scratchRecords, loff, 0u, 8u);
            byte_block::put(scratchRecords, loff + 8u, 0u, 8u);
        }
    }

    const auto encodeStart = timing::nowNs();
    const auto cycleStart = timing::readCycles();

    h.timeScale = math::safePositiveScale(timeScale);
    h.priceScale = math::safePositiveScale(priceScale);
    h.qtyScale = math::safePositiveScale(qtyScale);

    const auto timeDiv = math::ExactScaleDivisor::prepare(h.timeScale);
    const auto priceDiv = math::ExactScaleDivisor::prepare(h.priceScale);
    const auto qtyDiv = math::ExactScaleDivisor::prepare(h.qtyScale);

    h.baseTsUnit = timeDiv.divideSignedExact(firstTs);

    auto bidHotVec = buildHotQtySorted(bidRawQtys, qtyDiv);
    auto askHotVec = buildHotQtySorted(askRawQtys, qtyDiv);
    h.bidHotCount = static_cast<std::uint16_t>(bidHotVec.size());
    std::vector<std::int64_t> hot;
    hot.reserve(bidHotVec.size() + askHotVec.size());
    hot.insert(hot.end(), bidHotVec.begin(), bidHotVec.end());
    hot.insert(hot.end(), askHotVec.begin(), askHotVec.end());
    h.hotQtyCount = static_cast<std::uint32_t>(hot.size());
    h.hotQtyBytes = static_cast<std::uint32_t>(hot.size() * sizeof(std::int64_t));
    const std::span<const std::int64_t> bidHot{hot.data(), h.bidHotCount};
    const std::span<const std::int64_t> askHot{hot.data() + h.bidHotCount, hot.size() - h.bidHotCount};

    std::vector<std::uint8_t> batchStream, priceStream, qtyCodeStream, qtyEscapeStream, residualStream;
    BitWriter sideBits, deleteBits;
    BookState state;
    std::int64_t previousTs = h.baseTsUnit;

    for (const auto& batch : batches) {
        const auto ts = timeDiv.divideSignedExact(batch.ts);
        std::int64_t dt{};
        if (!internal::subtractI64(ts, previousTs, dt)) {
            outError = "depth timestamp delta overflow";
            return Status::InvalidArgument;
        }
        writeVarint(batchStream, zigzag(dt));
        writeVarint(batchStream, static_cast<std::uint64_t>(batch.levels.size()));
        previousTs = ts;

        std::uint64_t bidCount = 0u, askCount = 0u;
        const bool runMode = runModeCandidate(batch, state, priceDiv, bidCount, askCount);
        writeVarint(batchStream, runMode ? 1u : 0u);
        const auto bestBid = state.bestBid;
        const auto bestAsk = state.bestAsk;

        if (runMode) {
            ++h.runModeBatchCount;
            writeVarint(batchStream, bidCount);
            writeVarint(batchStream, askCount);
            std::int64_t previousOffset = 0;
            bool first = true;
            for (const auto& raw : batch.levels) {
                if (raw.side != 0u) continue;
                const auto price = priceDiv.divideSignedExact(raw.price);
                const auto qty = qtyDiv.divideSignedExact(raw.qty);
                const auto offset = bestBid - price;
                writeVarint(priceStream, static_cast<std::uint64_t>(first ? offset : offset - previousOffset));
                previousOffset = offset;
                first = false;
                ++h.offsetPriceCount;
                const bool deleted = (qty == 0);
                deleteBits.bit(deleted);
                if (deleted) ++h.deleteCount;
                else writeQty(0u, qty, bidHot, askHot, qtyCodeStream, qtyEscapeStream, h);
            }
            previousOffset = 0;
            first = true;
            for (const auto& raw : batch.levels) {
                if (raw.side != 1u) continue;
                const auto price = priceDiv.divideSignedExact(raw.price);
                const auto qty = qtyDiv.divideSignedExact(raw.qty);
                const auto offset = price - bestAsk;
                writeVarint(priceStream, static_cast<std::uint64_t>(first ? offset : offset - previousOffset));
                previousOffset = offset;
                first = false;
                ++h.offsetPriceCount;
                const bool deleted = (qty == 0);
                deleteBits.bit(deleted);
                if (deleted) ++h.deleteCount;
                else writeQty(1u, qty, bidHot, askHot, qtyCodeStream, qtyEscapeStream, h);
            }
        } else {
            ++h.fallbackBatchCount;
            for (const auto& raw : batch.levels) {
                const auto side = raw.side;
                const auto price = priceDiv.divideSignedExact(raw.price);
                const auto qty = qtyDiv.divideSignedExact(raw.qty);
                sideBits.bit(side != 0u);
                const bool haveSide = (side == 0u) ? state.haveBid : state.haveAsk;
                std::int64_t rel{};
                if (haveSide && (side == 0u
                        ? internal::subtractI64(bestBid, price, rel)
                        : internal::subtractI64(price, bestAsk, rel))) {
                    writeVarint(priceStream, zigzag(rel));
                    ++h.offsetPriceCount;
                } else if (!haveSide) {
                    writeVarint(priceStream, zigzag(price));
                    ++h.absolutePriceCount;
                } else {
                    outError = "depth price offset overflow";
                    return Status::InvalidArgument;
                }
                const bool deleted = (qty == 0);
                deleteBits.bit(deleted);
                if (deleted) ++h.deleteCount;
                else writeQty(side, qty, bidHot, askHot, qtyCodeStream, qtyEscapeStream, h);
            }
        }
        for (const auto& raw : batch.levels) {
            state.apply(raw.side, priceDiv.divideSignedExact(raw.price), qtyDiv.divideSignedExact(raw.qty));
        }
        state.recompute();
    }

    residualStream.reserve(scratchRecords.size());
    for (std::size_t col = 0u; col < stride; ++col) {
        std::uint8_t prevByte = 0u;
        for (std::size_t row = 0u; row < count; ++row) {
            const std::uint8_t cur = scratchRecords[row * stride + col];
            residualStream.push_back(static_cast<std::uint8_t>(cur - prevByte));
            prevByte = cur;
        }
    }

    const auto sideStream = sideBits.finish();
    const auto deleteStream = deleteBits.finish();

    outEncodeCycles = timing::readCycles() - cycleStart;
    outEncodeCoreNs = timing::nowNs() - encodeStart;

    h.batchBytes = static_cast<std::uint32_t>(batchStream.size());
    h.sideBytes = static_cast<std::uint32_t>(sideStream.size());
    h.priceModeBytes = 0u;
    h.deleteBytes = static_cast<std::uint32_t>(deleteStream.size());
    h.priceBytes = static_cast<std::uint32_t>(priceStream.size());
    h.qtyCodeBytes = static_cast<std::uint32_t>(qtyCodeStream.size());
    h.qtyEscapeBytes = static_cast<std::uint32_t>(qtyEscapeStream.size());
    h.residualBytes = static_cast<std::uint32_t>(residualStream.size());
    h.outputBytes = kHeaderBytes + h.hotQtyBytes + h.batchBytes + h.sideBytes +
        h.priceModeBytes + h.deleteBytes + h.priceBytes + h.qtyCodeBytes +
        h.qtyEscapeBytes + h.residualBytes;
    h.headerCrc32c = computeHeaderCrc(h);

    const auto headerBytes = serializeHeaderRaw(h, true);
    outEncoded.reserve(static_cast<std::size_t>(h.outputBytes));
    outEncoded.insert(outEncoded.end(), headerBytes.begin(), headerBytes.end());
    for (const auto qty : hot) writeLe(outEncoded, qty);
    for (const auto* s : {&batchStream, &sideStream, &deleteStream, &priceStream,
                          &qtyCodeStream, &qtyEscapeStream, &residualStream}) {
        outEncoded.insert(outEncoded.end(), s->begin(), s->end());
    }
    outRecordCount = count;
    return Status::Ok;
}

CompressionResult compress(const CompressionRequest& request, const PipelineDescriptor& pipeline) noexcept try {
    if (request.inputPath.empty()) {
        return internal::fail(Status::InvalidArgument, request, &pipeline, "input path is empty");
    }
    if (inferStreamTypeFromPath(request.inputPath) != StreamType::Depth) {
        return internal::fail(Status::UnsupportedStream, request, &pipeline, "expected depth binary slice (.bin/.hbs)");
    }
    CompressionResult result{};
    internal::applyPipeline(result, &pipeline);
    result.streamType = StreamType::Depth;
    result.inputPath = request.inputPath;
    const auto totalStart = timing::nowNs();

    std::vector<std::uint8_t> input;
    const auto readStart = timing::nowNs();
    if (!internal::readFileBytes(request.inputPath, input)) {
        return internal::fail(Status::IoError, request, &pipeline, "failed to read input file");
    }
    result.readNs = timing::nowNs() - readStart;
    result.inputBytes = input.size();

    const auto parseStart = timing::nowNs();
    BinarySliceView slice{};
    const auto sliceStatus = parseBinarySlice(input, slice);
    if (!isOk(sliceStatus) || slice.layout.streamType != StreamType::Depth) {
        return internal::fail(
            isOk(sliceStatus) ? Status::UnsupportedStream : sliceStatus,
            request,
            &pipeline,
            "input is not a valid canonical depth binary slice");
    }
    result.parseNs = timing::nowNs() - parseStart;

    std::vector<std::uint8_t> encoded;
    std::uint64_t recordCount = 0u;
    std::string encodeError;
    const auto encodeStatus = encodeSliceBytes(
        slice.layout, slice.records, encoded, recordCount, result.encodeCoreNs, result.encodeCycles, encodeError);
    if (!isOk(encodeStatus)) {
        return internal::fail(encodeStatus, request, &pipeline, encodeError);
    }

    const auto outputPath = internal::outputPathFor(request, pipeline, StreamType::Depth);
    std::error_code dirEc;
    std::filesystem::create_directories(outputPath.parent_path(), dirEc);
    if (dirEc) {
        return internal::fail(Status::IoError, request, &pipeline, "failed to create output directory");
    }

    result.outputPath = outputPath;
    result.metricsPath = outputPath.parent_path() / (outputPath.stem().string() + ".metrics.json");
    const auto writeStart = timing::nowNs();
    std::ofstream out(outputPath, std::ios::binary | std::ios::trunc);
    if (!out) {
        return internal::fail(Status::IoError, request, &pipeline, "failed to open output file");
    }
    out.write(reinterpret_cast<const char*>(encoded.data()), static_cast<std::streamsize>(encoded.size()));
    out.close();
    if (!out) {
        return internal::fail(Status::IoError, request, &pipeline, "failed to write output file");
    }
    result.writeNs = timing::nowNs() - writeStart;
    result.outputBytes = encoded.size();
    result.lineCount = recordCount;
    result.blockCount = 1u;
    result.encodeNs = timing::nowNs() - totalStart;

    std::size_t decodedOffset = 0u;
    bool matches = true;
    const auto decodeStart = timing::nowNs();
    const auto decodeCycles = timing::readCycles();
    const auto decodeStatus = decode(encoded, [&](std::span<const std::uint8_t> block) {
        if (decodedOffset + block.size() > input.size()) {
            matches = false;
            return false;
        }
        matches = std::equal(block.begin(), block.end(), input.begin() + static_cast<std::ptrdiff_t>(decodedOffset));
        decodedOffset += block.size();
        return matches;
    });
    result.decodeCycles = timing::readCycles() - decodeCycles;
    result.decodeNs = timing::nowNs() - decodeStart;
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

ReplayArtifactInfo inspectArtifact(const std::filesystem::path& path, const PipelineDescriptor& pipeline) noexcept try {
    internal::FileDecodeSource source(path);
    std::array<std::uint8_t, kHeaderBytes> data{};
    ReplayArtifactInfo info{};
    info.path = path;
    if (!source.valid() || !source.read(0u, data)) {
        info.status = Status::IoError;
        info.error = "failed to read artifact";
        return info;
    }
    Header h{};
    if (!readHeader(data, h) || h.outputBytes != source.size() || !source.unchanged()) {
        info.status = Status::CorruptData;
        info.error = "invalid depth ladder artifact";
        return info;
    }
    info.status = Status::Ok;
    info.found = true;
    info.formatId = "hftmac.depth_ladder_offset";
    info.pipelineId = std::string{pipeline.id};
    info.transform = std::string{pipeline.transform};
    info.entropy = std::string{pipeline.entropy};
    info.streamType = StreamType::Depth;
    info.inputBytes = h.inputBytes;
    info.outputBytes = h.outputBytes;
    info.lineCount = h.batchCount;
    info.blockCount = 1u;
    return info;
} catch (...) {
    ReplayArtifactInfo failed{};
    failed.status = Status::DecodeError;
    return failed;
}

Status decodeSource(const internal::DecodeSource& source, const DecodedBlockCallback& onBlock) {
    if (!onBlock) return Status::InvalidArgument;
    return decodeColumns(source, &onBlock, nullptr);
}

// Single-pass decode: decodeColumns validates all column streams, CRCs, and
// source.unchanged() BEFORE invoking onBlock even once.
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

Status inspectEncodedJsonFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept try {
    if (!onBlock) return Status::InvalidArgument;
    internal::FileDecodeSource source(path);
    if (!source.valid()) return Status::IoError;
    std::ostringstream out;
    const auto status = decodeColumns(source, nullptr, &out);
    if (!isOk(status)) return status;
    if (!source.unchanged()) return Status::CorruptData;
    return emitText(out.str(), onBlock);
} catch (...) {
    return Status::DecodeError;
}

Status inspectEncodedBinaryFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept try {
    if (!onBlock) return Status::InvalidArgument;
    internal::FileDecodeSource source(path);
    std::array<std::uint8_t, kHeaderBytes> data{};
    if (!source.valid() || !source.read(0u, data)) return Status::IoError;
    Header h{};
    if (!readHeader(data, h) || h.outputBytes != source.size() || !source.unchanged()) return Status::CorruptData;
    std::ostringstream out;
    out << "depth_ladder_offset"
        << " bytes=" << source.size()
        << " header=" << kHeaderBytes
        << " hot_qty=" << h.hotQtyBytes
        << " batch=" << h.batchBytes
        << " side=" << h.sideBytes
        << " delete=" << h.deleteBytes
        << " price=" << h.priceBytes
        << " qty_code=" << h.qtyCodeBytes
        << " qty_escape=" << h.qtyEscapeBytes
        << " residual=" << h.residualBytes << "\n";
    return emitText(out.str(), onBlock);
} catch (...) {
    return Status::DecodeError;
}

Status inspectStatsJsonFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept try {
    if (!onBlock) return Status::InvalidArgument;
    internal::FileDecodeSource source(path);
    std::array<std::uint8_t, kHeaderBytes> data{};
    if (!source.valid() || !source.read(0u, data)) return Status::IoError;
    Header h{};
    if (!readHeader(data, h) || h.outputBytes != source.size() || !source.unchanged()) return Status::CorruptData;
    return emitText(statsJson(h), onBlock);
} catch (...) {
    return Status::DecodeError;
}

}  // namespace hft_compressor::codecs::depth_ladder_offset

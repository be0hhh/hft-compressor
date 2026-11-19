#include "hft_compressor/ByteBlock.hpp"
#include "BinaryTransform.hpp"
#include "../ExternalBlockPrimitives.hpp"
#include "../EntropyHftmac/EntropyHftMacInternal.hpp"
#include "../../Common/ExactScaleMath.hpp"
#include "../../Container/Hfc/Format.hpp"

#include <algorithm>
#include <array>
#include <cstdint>
#include <string_view>
#include <vector>

namespace hft_compressor {
namespace {

namespace binary = codecs::byte_block;
namespace entropy = codecs::entropy_hftmac::detail;
namespace ext = codecs::external_primitives;

constexpr std::uint32_t kMagic = 0x00434248u; // HBC

// Single-current HBC\0 wire storage encodings.
enum class Storage : std::uint8_t {
    Opaque = 0u,
    Transform = 1u,
    Lz4 = 2u,
    Zstd = 3u,
    Brotli = 4u,
    XzLzma = 5u,
    ArithmeticShift = 6u,
    RansByteStatic = 7u,
};

constexpr bool validStreamType(StreamType streamType) noexcept {
    switch (streamType) {
        case StreamType::Unknown:
        case StreamType::Trades:
        case StreamType::BookTicker:
        case StreamType::Depth:
        case StreamType::DepthCoverage:
            return true;
    }
    return false;
}

constexpr bool fits(std::size_t offset, std::size_t count, std::size_t bytes) noexcept {
    return offset <= bytes && count <= bytes - offset;
}

bool valid(const ByteBlockLayout& l) noexcept {
    if (!validStreamType(l.streamType) ||
        l.recordBytes == 0u || l.recordBytes > kByteBlockMaxRecordBytes ||
        l.payloadOffset > l.recordBytes) {
        return false;
    }
    const bool noDepth = l.depthLevelsOffset == 0u && l.depthLevelStride == 0u &&
                         l.depthLevelCountOffset == 0u && l.depthLevelCapacity == 0u;
    switch (l.streamType) {
        case StreamType::Unknown:
        case StreamType::DepthCoverage:
            return noDepth;
        case StreamType::Trades:
            return noDepth && fits(l.payloadOffset, 17u, l.recordBytes) &&
                   fits(l.timestampOffset, 8u, l.recordBytes) &&
                   (l.timestampOffset + 8u <= l.payloadOffset || l.timestampOffset >= l.payloadOffset + 17u);
        case StreamType::BookTicker:
            return noDepth && fits(l.payloadOffset, 32u, l.recordBytes);
        case StreamType::Depth:
            return l.depthLevelCapacity > 0u && l.depthLevelCapacity <= kByteBlockMaxDepthLevels &&
                   l.depthLevelStride >= 18u && l.depthLevelsOffset >= l.payloadOffset &&
                   fits(l.depthLevelCountOffset, 2u, l.recordBytes) &&
                   l.depthLevelCountOffset + 2u <= l.depthLevelsOffset &&
                   fits(l.depthLevelsOffset,
                        std::size_t{l.depthLevelStride} * l.depthLevelCapacity,
                        l.recordBytes);
    }
    return false;
}

bool validRecords(const ByteBlockLayout& l, std::span<const std::uint8_t> records) noexcept {
    if (l.streamType != StreamType::Depth) return true;
    for (std::size_t offset = 0u; offset < records.size(); offset += l.recordBytes) {
        if (binary::word(records, offset + l.depthLevelCountOffset, 2u) > l.depthLevelCapacity) {
            return false;
        }
    }
    return true;
}

std::uint32_t layoutCrc(const ByteBlockLayout& l) noexcept {
    const std::array<std::uint32_t, 8u> values{
        static_cast<std::uint32_t>(l.streamType),
        l.recordBytes,
        l.payloadOffset,
        l.timestampOffset,
        l.depthLevelsOffset,
        l.depthLevelStride,
        l.depthLevelCountOffset,
        l.depthLevelCapacity,
    };
    std::array<std::uint8_t, 32u> bytes{};
    for (std::size_t i = 0u; i < values.size(); ++i) {
        binary::put(bytes, i * 4u, values[i], 4u);
    }
    return format::crc32c(bytes);
}

bool overlap(std::span<const std::uint8_t> input, std::span<std::uint8_t> output) noexcept {
    if (input.empty() || output.empty()) return false;
    const auto a = reinterpret_cast<std::uintptr_t>(input.data());
    const auto b = reinterpret_cast<std::uintptr_t>(output.data());
    return a <= b ? b - a < input.size() : a - b < output.size();
}

constexpr std::size_t transformBound(std::size_t rawBytes) noexcept {
    return rawBytes * 3u + 1024u;
}

void transform(const ByteBlockLayout& l,
               std::size_t count,
               std::span<const std::uint8_t> input,
               std::vector<std::uint8_t>& records,
               std::vector<std::uint8_t>& columns,
               std::vector<std::uint8_t>& result) {
    records.assign(input.begin(), input.end());
    columns.clear();
    result.clear();
    switch (l.streamType) {
        case StreamType::Trades:
            binary::encodeTradesCount(l, count, records, columns);
            break;
        case StreamType::BookTicker:
            binary::encodeBookTicker(l, records, columns);
            break;
        case StreamType::Depth:
            binary::encodeDepth(l, records, columns);
            break;
        case StreamType::Unknown:
        case StreamType::DepthCoverage:
            break;
    }
    if (l.streamType == StreamType::Trades ||
        l.streamType == StreamType::BookTicker ||
        l.streamType == StreamType::Depth) {
        result.resize(4u);
        binary::put(result, 0u, columns.size(), 4u);
        result.insert(result.end(), columns.begin(), columns.end());
    }
    // Plane-wise modular byte deltas retain metadata and all inactive/padding
    // bytes across compact per-channel strides (zero division on l.recordBytes).
    for (std::size_t column = 0u; column < l.recordBytes; ++column) {
        std::uint8_t previous{};
        for (std::size_t row = 0u; row < count; ++row) {
            const auto value = records[row * l.recordBytes + column];
            result.push_back(static_cast<std::uint8_t>(value - previous));
            previous = value;
        }
    }
}

bool restore(const ByteBlockLayout& l,
             std::size_t count,
             std::span<const std::uint8_t> base,
             std::span<std::uint8_t> records) {
    std::span<const std::uint8_t> columns;
    if (l.streamType == StreamType::Trades ||
        l.streamType == StreamType::BookTicker ||
        l.streamType == StreamType::Depth) {
        if (base.size() < 4u) return false;
        const auto bytes = static_cast<std::size_t>(binary::word(base, 0u, 4u));
        if (!fits(4u, bytes, base.size())) return false;
        columns = base.subspan(4u, bytes);
        base = base.subspan(4u + bytes);
    }
    if (base.size() != records.size()) return false;
    for (std::size_t column = 0u; column < l.recordBytes; ++column) {
        std::uint8_t previous{};
        for (std::size_t row = 0u; row < count; ++row) {
            previous = static_cast<std::uint8_t>(previous + base[column * count + row]);
            records[row * l.recordBytes + column] = previous;
        }
    }
    if (!validRecords(l, records)) return false;
    binary::Reader reader{columns};
    switch (l.streamType) {
        case StreamType::Trades:
            return binary::decodeTradesCount(l, count, records, reader);
        case StreamType::BookTicker:
            return binary::decodeBookTicker(l, records, reader);
        case StreamType::Depth:
            return binary::decodeDepth(l, records, reader);
        case StreamType::Unknown:
        case StreamType::DepthCoverage:
            return true;
    }
    return false;
}

constexpr bool validModeValue(ByteBlockCodecMode mode) noexcept {
    switch (mode) {
        case ByteBlockCodecMode::Opaque:
        case ByteBlockCodecMode::TransformOnly:
        case ByteBlockCodecMode::Lz4Fast:
        case ByteBlockCodecMode::ZstdFast:
        case ByteBlockCodecMode::ZstdMax:
        case ByteBlockCodecMode::Brotli:
        case ByteBlockCodecMode::XzLzma:
        case ByteBlockCodecMode::ArithmeticShift:
        case ByteBlockCodecMode::RansByteStatic:
            return true;
    }
    return false;
}

} // namespace

struct ByteBlockWorkspace::Scratch {
    std::vector<std::uint8_t> raw;
    std::vector<std::uint8_t> columns;
    std::vector<std::uint8_t> base;
    std::vector<std::uint8_t> compressed;
    std::vector<std::uint8_t> lz4State;
    entropy::ArithmeticWorkspace entropyModels;
    void* zstdCCtx{nullptr};
    void* zstdDCtx{nullptr};

    Scratch() {
        constexpr auto maxRaw = kByteBlockMaxRecords * kByteBlockMaxRecordBytes;
        constexpr auto maxBase = transformBound(maxRaw);
        raw.reserve(maxRaw);
        columns.reserve(maxBase - maxRaw);
        base.reserve(maxBase);

        std::size_t maxComp = maxBase + 4096u;
        if (ext::lz4Available()) {
            maxComp = std::max(maxComp, ext::lz4CompressBound(maxBase));
            lz4State.resize(ext::lz4StateBytes());
        }
        if (ext::zstdAvailable()) {
            maxComp = std::max(maxComp, ext::zstdCompressBound(maxBase));
        }
        if (ext::brotliAvailable()) {
            maxComp = std::max(maxComp, ext::brotliCompressBound(maxBase));
        }
        if (ext::xzLzmaAvailable()) {
            maxComp = std::max(maxComp, ext::xzLzmaCompressBound(maxBase));
        }
        compressed.reserve(maxComp);
    }

    ~Scratch() {
        if (zstdCCtx != nullptr) ext::freeZstdCCtx(zstdCCtx);
        if (zstdDCtx != nullptr) ext::freeZstdDCtx(zstdDCtx);
    }

    Scratch(const Scratch&) = delete;
    Scratch& operator=(const Scratch&) = delete;

    void* ensureZstdCCtx() noexcept {
        if (zstdCCtx == nullptr) zstdCCtx = ext::createZstdCCtx();
        return zstdCCtx;
    }

    void* ensureZstdDCtx() noexcept {
        if (zstdDCtx == nullptr) zstdDCtx = ext::createZstdDCtx();
        return zstdDCtx;
    }
};

ByteBlockWorkspace::ByteBlockWorkspace() noexcept = default;
ByteBlockWorkspace::~ByteBlockWorkspace() = default;

Status ByteBlockWorkspace::prepare() noexcept {
    if (scratch_) return Status::Ok;
    try {
        scratch_ = std::make_unique<Scratch>();
        return Status::Ok;
    } catch (...) {
        return Status::DecodeError;
    }
}

void ByteBlockWorkspace::reset() noexcept {
    if (!scratch_) return;
    scratch_->raw.clear();
    scratch_->columns.clear();
    scratch_->base.clear();
    scratch_->compressed.clear();
}

bool validByteBlockLayout(const ByteBlockLayout& layout) noexcept {
    return valid(layout);
}

ByteBlockCodecMode defaultByteBlockCodecMode() noexcept {
    if (ext::lz4Available()) return ByteBlockCodecMode::Lz4Fast;
    if (ext::zstdAvailable()) return ByteBlockCodecMode::ZstdFast;
    return ByteBlockCodecMode::RansByteStatic;
}

bool byteBlockCodecModeAvailable(ByteBlockCodecMode mode) noexcept {
    switch (mode) {
        case ByteBlockCodecMode::Opaque:
        case ByteBlockCodecMode::TransformOnly:
        case ByteBlockCodecMode::ArithmeticShift:
        case ByteBlockCodecMode::RansByteStatic:
            return true;
        case ByteBlockCodecMode::Lz4Fast:
            return ext::lz4Available();
        case ByteBlockCodecMode::ZstdFast:
        case ByteBlockCodecMode::ZstdMax:
            return ext::zstdAvailable();
        case ByteBlockCodecMode::Brotli:
            return ext::brotliAvailable();
        case ByteBlockCodecMode::XzLzma:
            return ext::xzLzmaAvailable();
    }
    return false;
}

std::string_view byteBlockCodecModeName(ByteBlockCodecMode mode) noexcept {
    switch (mode) {
        case ByteBlockCodecMode::Opaque: return "opaque";
        case ByteBlockCodecMode::TransformOnly: return "transform_only";
        case ByteBlockCodecMode::Lz4Fast: return "lz4_fast";
        case ByteBlockCodecMode::ZstdFast: return "zstd_fast";
        case ByteBlockCodecMode::ZstdMax: return "zstd_max";
        case ByteBlockCodecMode::Brotli: return "brotli";
        case ByteBlockCodecMode::XzLzma: return "xz_lzma";
        case ByteBlockCodecMode::ArithmeticShift: return "arithmetic_shift";
        case ByteBlockCodecMode::RansByteStatic: return "rans_byte_static";
    }
    return "unknown";
}

bool parseByteBlockCodecMode(std::string_view text, ByteBlockCodecMode& out) noexcept {
    if (text == "opaque") { out = ByteBlockCodecMode::Opaque; return true; }
    if (text == "transform_only" || text == "transform") { out = ByteBlockCodecMode::TransformOnly; return true; }
    if (text == "lz4_fast" || text == "lz4") { out = ByteBlockCodecMode::Lz4Fast; return true; }
    if (text == "zstd_fast" || text == "zstd") { out = ByteBlockCodecMode::ZstdFast; return true; }
    if (text == "zstd_max") { out = ByteBlockCodecMode::ZstdMax; return true; }
    if (text == "brotli") { out = ByteBlockCodecMode::Brotli; return true; }
    if (text == "xz_lzma" || text == "xz" || text == "lzma") { out = ByteBlockCodecMode::XzLzma; return true; }
    if (text == "arithmetic_shift" || text == "arithmetic") { out = ByteBlockCodecMode::ArithmeticShift; return true; }
    if (text == "rans_byte_static" || text == "rans") { out = ByteBlockCodecMode::RansByteStatic; return true; }
    return false;
}

std::size_t byteBlockEncodeBound(const ByteBlockLayout& layout, std::size_t count) noexcept {
    return valid(layout) && count > 0u && count <= kByteBlockMaxRecords
        ? kByteBlockHeaderBytes + count * layout.recordBytes
        : 0u;
}

Status encodeByteBlock(ByteBlockWorkspace& workspace,
                       const ByteBlockLayout& l,
                       std::span<const std::uint8_t> records,
                       ByteBlockCodecMode mode,
                       std::span<std::uint8_t> encoded,
                       std::size_t& bytes) noexcept {
    bytes = 0u;
    if (!workspace.scratch_ || !valid(l) || !validModeValue(mode) ||
        records.empty() || overlap(records, encoded)) {
        return Status::InvalidArgument;
    }
    std::size_t count = 0u;
    if (!math::exactRecordCount(records.size(), l.recordBytes, count) ||
        count > kByteBlockMaxRecords) {
        return Status::InvalidArgument;
    }
    const auto bound = byteBlockEncodeBound(l, count);
    if (!bound || encoded.size() < bound || !validRecords(l, records)) {
        return Status::InvalidArgument;
    }
    if (!byteBlockCodecModeAvailable(mode)) {
        return Status::DependencyUnavailable;
    }

    try {
        workspace.reset();
        auto& scratch = *workspace.scratch_;
        Storage storage = Storage::Opaque;
        std::span<const std::uint8_t> payload = records;
        std::size_t baseSize = records.size();

        if (mode != ByteBlockCodecMode::Opaque) {
            auto& base = scratch.base;
            transform(l, count, records, scratch.raw, scratch.columns, base);
            if (base.size() > transformBound(records.size())) return Status::InvalidArgument;
            baseSize = base.size();
            if (base.size() <= records.size()) {
                storage = Storage::Transform;
                payload = base;
            }

            if (mode != ByteBlockCodecMode::TransformOnly && payload.size() > 1u) {
                const std::size_t strictLimit = payload.size() - 1u;
                auto& comp = scratch.compressed;
                std::size_t written = 0u;
                bool improved = false;
                Storage candidateStorage = storage;

                switch (mode) {
                    case ByteBlockCodecMode::Lz4Fast: {
                        const std::size_t cap = ext::lz4CompressBound(base.size());
                        comp.resize(cap);
                        if (isOk(ext::lz4CompressFast(scratch.lz4State, base, comp, written)) &&
                            written > 0u && written <= strictLimit) {
                            comp.resize(written);
                            improved = true;
                            candidateStorage = Storage::Lz4;
                        }
                        break;
                    }
                    case ByteBlockCodecMode::ZstdFast:
                    case ByteBlockCodecMode::ZstdMax: {
                        void* cctx = scratch.ensureZstdCCtx();
                        if (cctx == nullptr) return Status::DecodeError;
                        const int level = (mode == ByteBlockCodecMode::ZstdMax) ? 19 : 1;
                        const std::size_t cap = ext::zstdCompressBound(base.size());
                        comp.resize(cap);
                        if (isOk(ext::zstdCompressWithCtx(cctx, level, base, comp, written)) &&
                            written > 0u && written <= strictLimit) {
                            comp.resize(written);
                            improved = true;
                            candidateStorage = Storage::Zstd;
                        }
                        break;
                    }
                    case ByteBlockCodecMode::Brotli: {
                        const std::size_t cap = ext::brotliCompressBound(base.size());
                        comp.resize(cap);
                        if (isOk(ext::brotliCompressBlock(6, base, comp, written)) &&
                            written > 0u && written <= strictLimit) {
                            comp.resize(written);
                            improved = true;
                            candidateStorage = Storage::Brotli;
                        }
                        break;
                    }
                    case ByteBlockCodecMode::XzLzma: {
                        const std::size_t cap = ext::xzLzmaCompressBound(base.size());
                        comp.resize(cap);
                        if (isOk(ext::xzLzmaCompressBlock(4, base, comp, written)) &&
                            written > 0u && written <= strictLimit) {
                            comp.resize(written);
                            improved = true;
                            candidateStorage = Storage::XzLzma;
                        }
                        break;
                    }
                    case ByteBlockCodecMode::ArithmeticShift: {
                        if (entropy::arithmeticEncodeBounded(
                                scratch.entropyModels,
                                base,
                                entropy::EntropyKind::Ac16Ctx8,
                                strictLimit,
                                comp) &&
                            !comp.empty() && comp.size() <= strictLimit) {
                            improved = true;
                            candidateStorage = Storage::ArithmeticShift;
                        }
                        break;
                    }
                    case ByteBlockCodecMode::RansByteStatic: {
                        if (entropy::arithmeticEncodeBounded(
                                scratch.entropyModels,
                                base,
                                entropy::EntropyKind::RansByteStatic,
                                strictLimit,
                                comp) &&
                            !comp.empty() && comp.size() <= strictLimit) {
                            improved = true;
                            candidateStorage = Storage::RansByteStatic;
                        }
                        break;
                    }
                    case ByteBlockCodecMode::Opaque:
                    case ByteBlockCodecMode::TransformOnly:
                        break;
                }

                if (improved) {
                    storage = candidateStorage;
                    payload = comp;
                }
            }
        }

        std::array<std::uint8_t, kByteBlockHeaderBytes> header{};
        binary::put(header, 0u, kMagic, 4u);
        binary::put(header, 4u, kByteBlockCodec, 2u);
        header[6u] = static_cast<std::uint8_t>(storage);
        header[7u] = static_cast<std::uint8_t>(l.streamType);
        binary::put(header, 8u, count, 2u);
        binary::put(header, 10u, l.recordBytes, 4u);
        binary::put(header, 14u, records.size(), 4u);
        binary::put(header, 18u, storage == Storage::Opaque ? records.size() : baseSize, 4u);
        binary::put(header, 22u, payload.size(), 4u);
        binary::put(header, 26u, format::crc32c(records), 4u);
        binary::put(header, 30u, format::crc32c(payload), 4u);
        binary::put(header, 34u, layoutCrc(l), 4u);
        binary::put(header, 38u, format::crc32c(header), 4u);
        std::copy(header.begin(), header.end(), encoded.begin());
        std::copy(payload.begin(), payload.end(), encoded.begin() + kByteBlockHeaderBytes);
        bytes = kByteBlockHeaderBytes + payload.size();
        return Status::Ok;
    } catch (...) {
        return Status::DecodeError;
    }
}

Status encodeByteBlock(ByteBlockWorkspace& workspace,
                       const ByteBlockLayout& l,
                       std::span<const std::uint8_t> records,
                       std::span<std::uint8_t> encoded,
                       std::size_t& bytes) noexcept {
    return encodeByteBlock(workspace, l, records, defaultByteBlockCodecMode(), encoded, bytes);
}

Status decodeByteBlock(ByteBlockWorkspace& workspace,
                       const ByteBlockLayout& l,
                       std::span<const std::uint8_t> encoded,
                       std::span<std::uint8_t> records,
                       std::size_t& bytes) noexcept {
    bytes = 0u;
    if (!workspace.scratch_ || !valid(l) || overlap(encoded, records)) {
        return Status::InvalidArgument;
    }
    if (encoded.size() < kByteBlockHeaderBytes) return Status::CorruptData;

    std::array<std::uint8_t, kByteBlockHeaderBytes> header{};
    std::copy_n(encoded.begin(), header.size(), header.begin());
    const auto crc = binary::word(header, 38u, 4u);
    binary::put(header, 38u, 0u, 4u);
    if (format::crc32c(header) != crc ||
        binary::word(header, 0u, 4u) != kMagic ||
        binary::word(header, 4u, 2u) != kByteBlockCodec ||
        header[6u] > static_cast<std::uint8_t>(Storage::RansByteStatic) ||
        header[7u] != static_cast<std::uint8_t>(l.streamType) ||
        binary::word(header, 10u, 4u) != l.recordBytes ||
        binary::word(header, 34u, 4u) != layoutCrc(l) ||
        !std::all_of(header.begin() + 42u, header.end(), [](auto byte) { return byte == 0u; })) {
        return Status::CorruptData;
    }

    const auto count = static_cast<std::size_t>(binary::word(header, 8u, 2u));
    const auto rawBytes = static_cast<std::size_t>(binary::word(header, 14u, 4u));
    const auto baseBytes = static_cast<std::size_t>(binary::word(header, 18u, 4u));
    const auto payloadBytes = static_cast<std::size_t>(binary::word(header, 22u, 4u));
    const auto bound = byteBlockEncodeBound(l, count);
    if (!bound ||
        rawBytes != count * l.recordBytes ||
        baseBytes == 0u || baseBytes > transformBound(rawBytes) ||
        payloadBytes == 0u || payloadBytes > rawBytes ||
        encoded.size() != kByteBlockHeaderBytes + payloadBytes) {
        return Status::CorruptData;
    }

    const auto storage = static_cast<Storage>(header[6u]);
    if ((storage == Storage::Opaque && (baseBytes != rawBytes || payloadBytes != rawBytes)) ||
        (storage == Storage::Transform && baseBytes != payloadBytes) ||
        (storage >= Storage::Lz4 && payloadBytes >= rawBytes)) {
        return Status::CorruptData;
    }
    if (records.size() < rawBytes) return Status::InvalidArgument;

    const auto payload = encoded.subspan(kByteBlockHeaderBytes);
    if (format::crc32c(payload) != binary::word(header, 30u, 4u)) {
        return Status::CorruptData;
    }

    if ((storage == Storage::Lz4 && !ext::lz4Available()) ||
        (storage == Storage::Zstd && !ext::zstdAvailable()) ||
        (storage == Storage::Brotli && !ext::brotliAvailable()) ||
        (storage == Storage::XzLzma && !ext::xzLzmaAvailable())) {
        return Status::DependencyUnavailable;
    }

    try {
        workspace.reset();
        auto& scratch = *workspace.scratch_;
        auto& raw = scratch.raw;
        raw.resize(rawBytes);

        if (storage == Storage::Opaque) {
            std::copy(payload.begin(), payload.end(), raw.begin());
        } else {
            auto& base = scratch.base;
            base.resize(baseBytes);
            switch (storage) {
                case Storage::Transform:
                    std::copy(payload.begin(), payload.end(), base.begin());
                    break;
                case Storage::Lz4:
                    if (!isOk(ext::lz4DecompressBlock(payload, base))) return Status::CorruptData;
                    break;
                case Storage::Zstd: {
                    void* dctx = scratch.ensureZstdDCtx();
                    if (dctx == nullptr) return Status::DecodeError;
                    if (!isOk(ext::zstdDecompressWithCtx(dctx, payload, base))) return Status::CorruptData;
                    break;
                }
                case Storage::Brotli:
                    if (!isOk(ext::brotliDecompressBlock(payload, base))) return Status::CorruptData;
                    break;
                case Storage::XzLzma:
                    if (!isOk(ext::xzLzmaDecompressBlock(payload, base))) return Status::CorruptData;
                    break;
                case Storage::ArithmeticShift:
                    if (!entropy::arithmeticDecodeBytes(
                            scratch.entropyModels, payload, entropy::EntropyKind::Ac16Ctx8, base)) {
                        return Status::CorruptData;
                    }
                    break;
                case Storage::RansByteStatic:
                    if (!entropy::arithmeticDecodeBytes(
                            scratch.entropyModels, payload, entropy::EntropyKind::RansByteStatic, base)) {
                        return Status::CorruptData;
                    }
                    break;
                case Storage::Opaque:
                    break;
            }
            if (!restore(l, count, base, raw)) return Status::CorruptData;
        }

        if (!validRecords(l, raw) || format::crc32c(raw) != binary::word(header, 26u, 4u)) {
            return Status::CorruptData;
        }
        std::copy(raw.begin(), raw.end(), records.begin());
        bytes = rawBytes;
        return Status::Ok;
    } catch (...) {
        return Status::DecodeError;
    }
}

Status encodeByteBlock(const ByteBlockLayout& l,
                       std::span<const std::uint8_t> records,
                       std::span<std::uint8_t> encoded,
                       std::size_t& bytes) noexcept {
    return encodeByteBlock(l, records, defaultByteBlockCodecMode(), encoded, bytes);
}

Status encodeByteBlock(const ByteBlockLayout& l,
                       std::span<const std::uint8_t> records,
                       ByteBlockCodecMode mode,
                       std::span<std::uint8_t> encoded,
                       std::size_t& bytes) noexcept {
    bytes = 0u;
    ByteBlockWorkspace workspace;
    const auto status = workspace.prepare();
    return isOk(status) ? encodeByteBlock(workspace, l, records, mode, encoded, bytes) : status;
}

Status decodeByteBlock(const ByteBlockLayout& l,
                       std::span<const std::uint8_t> encoded,
                       std::span<std::uint8_t> records,
                       std::size_t& bytes) noexcept {
    bytes = 0u;
    ByteBlockWorkspace workspace;
    const auto status = workspace.prepare();
    return isOk(status) ? decodeByteBlock(workspace, l, encoded, records, bytes) : status;
}

} // namespace hft_compressor

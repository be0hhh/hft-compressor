#include "codecs/entropy_hftmac/EntropyHftMac.hpp"

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

#include "codecs/bookticker_delta_mask/BookTickerDeltaMask.hpp"
#include "codecs/depth_ladder_offset/DepthLadderOffsetV2.hpp"
#include "codecs/trades_grouped_delta_qtydict/TradesGroupedDeltaQtyDict.hpp"
#include "common/CompressionInternals.hpp"
#include "common/timing.hpp"
#include "container/hfc/format.hpp"
#include "hft_compressor/metrics.hpp"

namespace hft_compressor::codecs::entropy_hftmac {
namespace {

constexpr std::uint32_t kMagic = 0x31464845u;  // EHF1
constexpr std::uint16_t kVersion = 1u;
constexpr std::size_t kHeaderBytes = 128u;
constexpr std::uint32_t kTopValue = 0xffffffffu;
constexpr std::uint32_t kFirstQuarter = 0x40000000u;
constexpr std::uint32_t kHalf = 0x80000000u;
constexpr std::uint32_t kThirdQuarter = 0xc0000000u;

enum class BaseKind : std::uint16_t {
    Trades = 1u,
    BookTicker = 2u,
    Depth = 3u,
};

enum class EntropyKind : std::uint16_t {
    Ac16Ctx0 = 1u,
    Ac16Ctx8 = 2u,
    Ac16Ctx12 = 3u,
    Ac32Ctx8 = 4u,
    RangeByteCtx8 = 5u,
    RansByteStatic = 6u,
};

struct Header {
    std::uint32_t magic{kMagic};
    std::uint16_t version{kVersion};
    std::uint16_t entropy{0};
    std::uint16_t base{0};
    std::uint16_t stream{0};
    std::uint32_t headerCrc32c{0};
    std::uint64_t inputBytes{0};
    std::uint64_t baseBytes{0};
    std::uint64_t outputBytes{0};
    std::uint64_t lineCount{0};
    std::uint64_t payloadBytes{0};
    std::uint32_t payloadCrc32c{0};
    std::uint32_t decodedCrc32c{0};
};

template <typename T>
void writeLe(std::vector<std::uint8_t>& out, T value) {
    for (std::size_t i = 0; i < sizeof(T); ++i) {
        out.push_back(static_cast<std::uint8_t>((static_cast<std::uint64_t>(value) >> (i * 8u)) & 0xffu));
    }
}

template <typename T>
bool readLe(const std::uint8_t*& p, const std::uint8_t* end, T& out) noexcept {
    if (static_cast<std::size_t>(end - p) < sizeof(T)) return false;
    std::uint64_t value = 0;
    for (std::size_t i = 0; i < sizeof(T); ++i) value |= static_cast<std::uint64_t>(p[i]) << (i * 8u);
    p += sizeof(T);
    out = static_cast<T>(value);
    return true;
}

std::vector<std::uint8_t> serializeHeader(Header header, bool includeCrc) {
    if (!includeCrc) header.headerCrc32c = 0u;
    std::vector<std::uint8_t> out;
    out.reserve(kHeaderBytes);
    writeLe(out, header.magic);
    writeLe(out, header.version);
    writeLe(out, header.entropy);
    writeLe(out, header.base);
    writeLe(out, header.stream);
    writeLe(out, header.headerCrc32c);
    writeLe(out, header.inputBytes);
    writeLe(out, header.baseBytes);
    writeLe(out, header.outputBytes);
    writeLe(out, header.lineCount);
    writeLe(out, header.payloadBytes);
    writeLe(out, header.payloadCrc32c);
    writeLe(out, header.decodedCrc32c);
    out.resize(kHeaderBytes, 0u);
    return out;
}

std::uint32_t headerCrc32c(const Header& header) {
    return format::crc32c(serializeHeader(header, false));
}

bool parseHeader(const std::uint8_t* data, std::size_t size, Header& out) noexcept {
    if (data == nullptr || size < kHeaderBytes) return false;
    const auto* p = data;
    const auto* end = data + kHeaderBytes;
    return readLe(p, end, out.magic)
        && readLe(p, end, out.version)
        && readLe(p, end, out.entropy)
        && readLe(p, end, out.base)
        && readLe(p, end, out.stream)
        && readLe(p, end, out.headerCrc32c)
        && readLe(p, end, out.inputBytes)
        && readLe(p, end, out.baseBytes)
        && readLe(p, end, out.outputBytes)
        && readLe(p, end, out.lineCount)
        && readLe(p, end, out.payloadBytes)
        && readLe(p, end, out.payloadCrc32c)
        && readLe(p, end, out.decodedCrc32c);
}

bool validHeader(const Header& header) noexcept {
    return header.magic == kMagic
        && header.version == kVersion
        && header.base >= static_cast<std::uint16_t>(BaseKind::Trades)
        && header.base <= static_cast<std::uint16_t>(BaseKind::Depth)
        && header.entropy >= static_cast<std::uint16_t>(EntropyKind::Ac16Ctx0)
        && header.entropy <= static_cast<std::uint16_t>(EntropyKind::RansByteStatic)
        && format::streamFromWire(header.stream) != StreamType::Unknown
        && header.headerCrc32c == headerCrc32c(header)
        && header.outputBytes == kHeaderBytes + header.payloadBytes;
}

struct BitWriter {
    std::vector<std::uint8_t> bytes{};
    std::uint8_t current{0};
    std::uint8_t bitCount{0};

    void bit(bool value) {
        current = static_cast<std::uint8_t>((current << 1u) | (value ? 1u : 0u));
        ++bitCount;
        if (bitCount == 8u) {
            bytes.push_back(current);
            current = 0;
            bitCount = 0;
        }
    }

    void finish() {
        if (bitCount == 0u) return;
        current = static_cast<std::uint8_t>(current << (8u - bitCount));
        bytes.push_back(current);
        current = 0;
        bitCount = 0;
    }
};

struct BitReader {
    std::span<const std::uint8_t> bytes{};
    std::size_t bytePos{0};
    std::uint8_t bitPos{0};

    bool bit() noexcept {
        if (bytePos >= bytes.size()) return false;
        const bool out = ((bytes[bytePos] >> (7u - bitPos)) & 1u) != 0u;
        ++bitPos;
        if (bitPos == 8u) {
            bitPos = 0;
            ++bytePos;
        }
        return out;
    }
};

struct BitModel {
    std::uint32_t zero{1};
    std::uint32_t one{1};

    std::uint32_t total() const noexcept { return zero + one; }

    void update(bool bit, std::uint32_t maxTotal) noexcept {
        if (bit) ++one;
        else ++zero;
        if (total() <= maxTotal) return;
        zero = (zero + 1u) >> 1u;
        one = (one + 1u) >> 1u;
        if (zero == 0u) zero = 1u;
        if (one == 0u) one = 1u;
    }
};

struct ArithmeticProfile {
    EntropyKind kind{EntropyKind::Ac16Ctx0};
    std::uint32_t contextCount{1};
    std::uint32_t maxTotal{1u << 16u};
};

ArithmeticProfile profileFor(EntropyKind kind) noexcept {
    switch (kind) {
        case EntropyKind::Ac16Ctx0: return {kind, 1u, 1u << 16u};
        case EntropyKind::Ac16Ctx8: return {kind, 256u, 1u << 16u};
        case EntropyKind::Ac16Ctx12: return {kind, 4096u, 1u << 16u};
        case EntropyKind::Ac32Ctx8: return {kind, 256u, 1u << 20u};
        case EntropyKind::RangeByteCtx8: return {kind, 2048u, 1u << 16u};
        case EntropyKind::RansByteStatic: return {kind, 4096u, 1u << 16u};
    }
    return {kind, 1u, 1u << 16u};
}

std::uint32_t contextFor(const ArithmeticProfile& profile, std::uint8_t previous, std::uint8_t bitIndex) noexcept {
    switch (profile.kind) {
        case EntropyKind::Ac16Ctx0: return 0u;
        case EntropyKind::Ac16Ctx8:
        case EntropyKind::Ac32Ctx8: return previous;
        case EntropyKind::Ac16Ctx12:
        case EntropyKind::RansByteStatic: return (static_cast<std::uint32_t>(previous) << 4u) | bitIndex;
        case EntropyKind::RangeByteCtx8: return (static_cast<std::uint32_t>(previous) << 3u) | (bitIndex & 7u);
    }
    return 0u;
}

void emitBitPlusPending(BitWriter& out, bool bit, std::uint32_t& pending) {
    out.bit(bit);
    while (pending != 0u) {
        out.bit(!bit);
        --pending;
    }
}

// EN: Integer binary arithmetic coding. The interval [low, high] is split by
// the adaptive zero/one frequencies, so each bit costs about -log2(p(bit)).
// RU: Целочисленное бинарное арифметическое кодирование. Интервал [low, high]
// делится адаптивными частотами нулей/единиц, поэтому каждый бит стоит около
// -log2(p(bit)).
std::vector<std::uint8_t> arithmeticEncode(std::span<const std::uint8_t> input, EntropyKind kind) {
    const ArithmeticProfile profile = profileFor(kind);
    std::vector<BitModel> models(profile.contextCount);
    BitWriter out;
    std::uint32_t low = 0;
    std::uint32_t high = kTopValue;
    std::uint32_t pending = 0;
    std::uint8_t previous = 0;

    for (const auto byte : input) {
        for (std::uint8_t bitIndex = 0; bitIndex < 8u; ++bitIndex) {
            const bool bit = ((byte >> (7u - bitIndex)) & 1u) != 0u;
            auto& model = models[contextFor(profile, previous, bitIndex)];
            const std::uint64_t range = static_cast<std::uint64_t>(high) - low + 1u;
            const std::uint32_t split = static_cast<std::uint32_t>(low + ((range * model.zero) / model.total()) - 1u);
            if (bit) low = split + 1u;
            else high = split;

            for (;;) {
                if (high < kHalf) emitBitPlusPending(out, false, pending);
                else if (low >= kHalf) {
                    emitBitPlusPending(out, true, pending);
                    low -= kHalf;
                    high -= kHalf;
                } else if (low >= kFirstQuarter && high < kThirdQuarter) {
                    ++pending;
                    low -= kFirstQuarter;
                    high -= kFirstQuarter;
                } else {
                    break;
                }
                low <<= 1u;
                high = (high << 1u) | 1u;
            }
            model.update(bit, profile.maxTotal);
        }
        previous = byte;
    }

    ++pending;
    emitBitPlusPending(out, low >= kFirstQuarter, pending);
    out.finish();
    return out.bytes;
}

Status arithmeticDecode(std::span<const std::uint8_t> encoded,
                        EntropyKind kind,
                        std::uint64_t decodedBytes,
                        std::vector<std::uint8_t>& out) noexcept {
    const ArithmeticProfile profile = profileFor(kind);
    std::vector<BitModel> models(profile.contextCount);
    BitReader bits{encoded};
    std::uint32_t low = 0;
    std::uint32_t high = kTopValue;
    std::uint32_t code = 0;
    for (std::uint32_t i = 0; i < 32u; ++i) code = (code << 1u) | (bits.bit() ? 1u : 0u);

    out.clear();
    out.reserve(static_cast<std::size_t>(decodedBytes));
    std::uint8_t previous = 0;
    for (std::uint64_t i = 0; i < decodedBytes; ++i) {
        std::uint8_t byte = 0;
        for (std::uint8_t bitIndex = 0; bitIndex < 8u; ++bitIndex) {
            auto& model = models[contextFor(profile, previous, bitIndex)];
            const std::uint64_t range = static_cast<std::uint64_t>(high) - low + 1u;
            const std::uint64_t scaled = (((static_cast<std::uint64_t>(code) - low + 1u) * model.total()) - 1u) / range;
            const bool bit = scaled >= model.zero;
            const std::uint32_t split = static_cast<std::uint32_t>(low + ((range * model.zero) / model.total()) - 1u);
            if (bit) low = split + 1u;
            else high = split;

            for (;;) {
                if (high < kHalf) {
                } else if (low >= kHalf) {
                    code -= kHalf;
                    low -= kHalf;
                    high -= kHalf;
                } else if (low >= kFirstQuarter && high < kThirdQuarter) {
                    code -= kFirstQuarter;
                    low -= kFirstQuarter;
                    high -= kFirstQuarter;
                } else {
                    break;
                }
                low <<= 1u;
                high = (high << 1u) | 1u;
                code = (code << 1u) | (bits.bit() ? 1u : 0u);
            }
            model.update(bit, profile.maxTotal);
            byte = static_cast<std::uint8_t>((byte << 1u) | (bit ? 1u : 0u));
        }
        out.push_back(byte);
        previous = byte;
    }
    return Status::Ok;
}

EntropyKind entropyKindFor(std::string_view id) noexcept {
    if (id.find("ac16_ctx0") != std::string_view::npos) return EntropyKind::Ac16Ctx0;
    if (id.find("ac16_ctx8") != std::string_view::npos) return EntropyKind::Ac16Ctx8;
    if (id.find("ac16_ctx12") != std::string_view::npos) return EntropyKind::Ac16Ctx12;
    if (id.find("ac32_ctx8") != std::string_view::npos) return EntropyKind::Ac32Ctx8;
    if (id.find("range_byte_ctx8") != std::string_view::npos) return EntropyKind::RangeByteCtx8;
    return EntropyKind::RansByteStatic;
}

BaseKind baseKindFor(StreamType streamType) noexcept {
    if (streamType == StreamType::Trades) return BaseKind::Trades;
    if (streamType == StreamType::BookTicker) return BaseKind::BookTicker;
    return BaseKind::Depth;
}

std::string_view basePipelineId(BaseKind base) noexcept {
    switch (base) {
        case BaseKind::Trades: return "hftmac.trades_grouped_delta_qtydict_math_v3";
        case BaseKind::BookTicker: return "hftmac.bookticker_delta_mask_v2";
        case BaseKind::Depth: return "hftmac.depth_ladder_offset_v3";
    }
    return {};
}

std::string_view formatIdFor(BaseKind base, EntropyKind kind) noexcept {
    (void)kind;
    switch (base) {
        case BaseKind::Trades: return "hftmac.trades_grouped_delta_qtydict.entropy.v1";
        case BaseKind::BookTicker: return "hftmac.bookticker_delta_mask.entropy.v1";
        case BaseKind::Depth: return "hftmac.depth_ladder_offset.entropy.v1";
    }
    return "hftmac.entropy.v1";
}

std::string_view entropyName(EntropyKind kind) noexcept {
    switch (kind) {
        case EntropyKind::Ac16Ctx0: return "ac16_ctx0";
        case EntropyKind::Ac16Ctx8: return "ac16_ctx8";
        case EntropyKind::Ac16Ctx12: return "ac16_ctx12";
        case EntropyKind::Ac32Ctx8: return "ac32_ctx8";
        case EntropyKind::RangeByteCtx8: return "range_byte_ctx8";
        case EntropyKind::RansByteStatic: return "rans_byte_static";
    }
    return "unknown";
}

ReplayArtifactInfo failArtifact(const std::filesystem::path& path, Status status, std::string error) {
    ReplayArtifactInfo info{};
    info.status = status;
    info.path = path;
    info.error = std::move(error);
    return info;
}

Status decodeBase(BaseKind base, std::span<const std::uint8_t> bytes, const DecodedBlockCallback& onBlock) noexcept {
    switch (base) {
        case BaseKind::Trades: return trades_grouped_delta_qtydict::decode(bytes, onBlock);
        case BaseKind::BookTicker: return bookticker_delta_mask::decode(bytes, onBlock);
        case BaseKind::Depth: return depth_ladder_offset_v2::decode(bytes, onBlock);
    }
    return Status::CorruptData;
}

Status decodePayload(std::span<const std::uint8_t> file, Header& header, std::vector<std::uint8_t>& baseBytes) noexcept {
    if (file.size() < kHeaderBytes) return Status::InvalidArgument;
    if (!parseHeader(file.data(), file.size(), header) || !validHeader(header)) return Status::CorruptData;
    if (file.size() != header.outputBytes) return Status::CorruptData;
    std::span<const std::uint8_t> payload{file.data() + kHeaderBytes, static_cast<std::size_t>(header.payloadBytes)};
    if (format::crc32c(payload) != header.payloadCrc32c) return Status::CorruptData;
    const auto status = arithmeticDecode(payload, static_cast<EntropyKind>(header.entropy), header.baseBytes, baseBytes);
    if (!isOk(status)) return status;
    if (baseBytes.size() != header.baseBytes || format::crc32c(baseBytes) != header.decodedCrc32c) return Status::CorruptData;
    return Status::Ok;
}

Status readFile(const std::filesystem::path& path, std::vector<std::uint8_t>& out) noexcept {
    return internal::readFileBytes(path, out) ? Status::Ok : Status::IoError;
}

}  // namespace

CompressionResult compress(const CompressionRequest& request, const PipelineDescriptor& pipeline) noexcept {
    const StreamType streamType = inferStreamTypeFromPath(request.inputPath);
    if (streamType == StreamType::Unknown) {
        auto result = internal::fail(Status::UnsupportedStream, request, &pipeline, "expected trades.jsonl, bookticker.jsonl, or depth.jsonl");
        metrics::recordRun(result);
        return result;
    }

    const auto outputPath = internal::outputPathFor(request, pipeline, streamType);
    const auto base = baseKindFor(streamType);
    const auto* basePipeline = findPipeline(basePipelineId(base));
    if (basePipeline == nullptr) {
        auto result = internal::fail(Status::UnsupportedPipeline, request, &pipeline, "base HFT-MAC pipeline is missing");
        metrics::recordRun(result);
        return result;
    }

    std::error_code ec;
    std::filesystem::create_directories(outputPath.parent_path(), ec);
    if (ec) {
        auto result = internal::fail(Status::IoError, request, &pipeline, "failed to create output directory");
        metrics::recordRun(result);
        return result;
    }

    CompressionRequest baseRequest = request;
    baseRequest.pipelineId = std::string{basePipeline->id};
    baseRequest.outputPathOverride = outputPath.parent_path() / (outputPath.stem().string() + ".base.tmp");
    const auto baseResult = hft_compressor::compress(baseRequest);
    if (!isOk(baseResult.status) || !baseResult.roundtripOk) {
        auto result = internal::fail(baseResult.status, request, &pipeline, baseResult.error.empty() ? "base HFT-MAC compression failed" : baseResult.error);
        metrics::recordRun(result);
        return result;
    }

    std::vector<std::uint8_t> baseBytes;
    if (!internal::readFileBytes(baseResult.outputPath, baseBytes)) {
        auto result = internal::fail(Status::IoError, request, &pipeline, "failed to read base artifact");
        metrics::recordRun(result);
        return result;
    }

    CompressionResult result{};
    internal::applyPipeline(result, &pipeline);
    result.streamType = streamType;
    result.inputPath = request.inputPath;
    result.outputPath = outputPath;
    result.metricsPath = outputPath.parent_path() / (outputPath.stem().string() + ".metrics.json");
    result.inputBytes = baseResult.inputBytes;
    result.lineCount = baseResult.lineCount;
    result.blockCount = 1u;

    const auto entropy = entropyKindFor(pipeline.id);
    const auto encodeStartNs = timing::nowNs();
    const auto encodeStartCycles = timing::readCycles();
    auto payload = arithmeticEncode(baseBytes, entropy);
    result.encodeCycles = timing::readCycles() - encodeStartCycles;
    result.encodeNs = timing::nowNs() - encodeStartNs + baseResult.encodeNs;
    result.encodeCoreNs = result.encodeNs;

    Header header{};
    header.entropy = static_cast<std::uint16_t>(entropy);
    header.base = static_cast<std::uint16_t>(base);
    header.stream = format::streamToWire(streamType);
    header.inputBytes = result.inputBytes;
    header.baseBytes = baseBytes.size();
    header.payloadBytes = payload.size();
    header.outputBytes = kHeaderBytes + payload.size();
    header.lineCount = result.lineCount;
    header.payloadCrc32c = format::crc32c(payload);
    header.decodedCrc32c = format::crc32c(baseBytes);
    header.headerCrc32c = headerCrc32c(header);
    const auto headerBytes = serializeHeader(header, true);

    const auto writeStartNs = timing::nowNs();
    std::ofstream out(outputPath, std::ios::binary | std::ios::trunc);
    if (!out) {
        auto failed = internal::fail(Status::IoError, request, &pipeline, "failed to open entropy artifact");
        metrics::recordRun(failed);
        return failed;
    }
    out.write(reinterpret_cast<const char*>(headerBytes.data()), static_cast<std::streamsize>(headerBytes.size()));
    out.write(reinterpret_cast<const char*>(payload.data()), static_cast<std::streamsize>(payload.size()));
    out.close();
    result.writeNs = timing::nowNs() - writeStartNs;
    if (!out) {
        auto failed = internal::fail(Status::IoError, request, &pipeline, "failed to write entropy artifact");
        metrics::recordRun(failed);
        return failed;
    }
    result.outputBytes = header.outputBytes;

    std::size_t decodedOffset = 0;
    bool decodedMatchesInput = true;
    std::vector<std::uint8_t> original;
    if (!internal::readFileBytes(request.inputPath, original)) decodedMatchesInput = false;
    const auto decodeStartNs = timing::nowNs();
    const auto decodeStartCycles = timing::readCycles();
    const auto decodeStatus = decodeFile(outputPath, [&](std::span<const std::uint8_t> block) {
        if (decodedOffset + block.size() > original.size()) {
            decodedMatchesInput = false;
            return false;
        }
        decodedMatchesInput = std::equal(block.begin(), block.end(), original.begin() + static_cast<std::ptrdiff_t>(decodedOffset));
        decodedOffset += block.size();
        return decodedMatchesInput;
    });
    result.decodeCycles = timing::readCycles() - decodeStartCycles;
    result.decodeNs = timing::nowNs() - decodeStartNs;
    result.decodeCoreNs = result.decodeNs;
    result.roundtripOk = isOk(decodeStatus) && decodedMatchesInput && decodedOffset == original.size();
    result.status = result.roundtripOk ? Status::Ok : Status::DecodeError;
    if (!result.roundtripOk) result.error = "entropy roundtrip check failed";

    std::filesystem::remove(baseResult.outputPath, ec);
    std::filesystem::remove(baseResult.metricsPath, ec);
    (void)internal::writeTextFile(result.metricsPath, toMetricsJson(result));
    metrics::recordRun(result);
    return result;
}

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

Status decode(std::span<const std::uint8_t> file, const DecodedBlockCallback& onBlock) noexcept {
    if (!onBlock) return Status::InvalidArgument;
    Header header{};
    std::vector<std::uint8_t> baseBytes;
    const auto status = decodePayload(file, header, baseBytes);
    if (!isOk(status)) return status;
    return decodeBase(static_cast<BaseKind>(header.base), baseBytes, onBlock);
}

Status decodeFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept {
    if (path.empty() || !onBlock) return Status::InvalidArgument;
    std::vector<std::uint8_t> bytes;
    const auto readStatus = readFile(path, bytes);
    if (!isOk(readStatus)) return readStatus;
    return decode(bytes, onBlock);
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
        case BaseKind::Depth: return depth_ladder_offset_v2::decode(baseBytes, onBlock);
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

}  // namespace hft_compressor::codecs::entropy_hftmac

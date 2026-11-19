#pragma once

#include "TradesGroupedDeltaQtyDict.hpp"
#include "hft_compressor/ByteBlock.hpp"

#include <algorithm>
#include <array>
#include <fstream>
#include <sstream>
#include <string>
#include <string_view>
#include <system_error>
#include <vector>

#include "../../Common/CompressionInternals.hpp"
#include "../../Common/ExactScaleMath.hpp"
#include "../../Common/Timing.hpp"
#include "../../Container/Hfc/Format.hpp"

namespace hft_compressor::internal { class DecodeSource; }

namespace hft_compressor::codecs::trades_grouped_delta_qtydict::codec_detail {

constexpr std::uint32_t kFileMagic = 0x46435843u;  // CXCF
constexpr std::uint32_t kChunkMagic = 0x4b484354u; // TCHK
constexpr std::size_t kFileHeaderBytes = 128u;
constexpr std::size_t kChunkHeaderBytes = 168u;

constexpr std::uint32_t kDefaultChunkRecords = 16u * 1024u;
constexpr std::uint32_t kHotQtyCount = 64u;
constexpr std::uint32_t kMaxHotQtyCount = 128u;

struct Trade {
    std::int64_t price{0};
    std::int64_t qty{0};
    std::int64_t side{0};
    std::int64_t tsNs{0};
};

struct EncodedChunk {
    std::uint32_t recordCount{0};
    std::uint32_t timeGroupCount{0};
    std::uint32_t priceGroupCount{0};
    std::uint32_t qtyEscapeCount{0};
    std::int64_t firstTsNs{0};
    std::int64_t lastTsNs{0};
    std::int64_t baseTsNs{0};
    std::int64_t basePrice{0};
    std::int64_t baseTsUnit{0};
    std::int64_t basePriceTick{0};
    std::int64_t priceScale{1};
    std::int64_t qtyScale{1};
    std::int64_t timeScale{1};
    std::uint32_t hotQtyCount{0};
    std::uint32_t hotQtyBits{7};
    std::uint32_t hotQtyCapacity{64};
    std::uint32_t sideMode{0}; // 0 = 1-bit binary side, 1 = 8-bit full side
    std::uint32_t dpZeroCount{0};
    std::uint32_t countOneCount{0};
    std::uint32_t priceGroupCountOneCount{0};
    std::vector<std::int64_t> hotQty;
    std::vector<std::uint8_t> hotQtyTableStream;
    std::vector<std::uint8_t> timeStream;
    std::vector<std::uint8_t> priceStream;
    std::vector<std::uint8_t> sideStream;
    std::vector<std::uint8_t> dpZeroStream;
    std::vector<std::uint8_t> countOneStream;
    std::vector<std::uint8_t> priceGroupCountOneStream;
    std::vector<std::uint8_t> qtyCodeStream;
    std::vector<std::uint8_t> qtyEscapeStream;
    std::vector<std::uint8_t> residualStream;
    std::uint32_t payloadCrc32c{0};
};

struct FileHeader {
    std::uint32_t magic{kFileMagic};
    std::uint16_t stream{0};
    std::uint16_t reserved{0};
    ByteBlockLayout layout{};
    std::uint32_t chunkRecords{kDefaultChunkRecords};
    std::uint64_t inputBytes{0};
    std::uint64_t outputBytes{0};
    std::uint64_t recordCount{0};
    std::uint64_t chunkCount{0};
    std::uint64_t timeGroupCount{0};
    std::uint64_t priceGroupCount{0};
    std::uint64_t qtyEscapeCount{0};
    std::uint32_t recordsCrc32c{0};
    std::uint32_t headerCrc32c{0};
};

struct ChunkHeader {
    std::uint32_t magic{kChunkMagic};
    std::uint32_t recordCount{0};
    std::uint32_t timeGroupCount{0};
    std::uint32_t priceGroupCount{0};
    std::uint32_t qtyEscapeCount{0};
    std::int64_t firstTsNs{0};
    std::int64_t lastTsNs{0};
    std::int64_t baseTsNs{0};
    std::int64_t basePrice{0};
    std::int64_t baseTsUnit{0};
    std::int64_t basePriceTick{0};
    std::int64_t priceScale{1};
    std::int64_t qtyScale{1};
    std::int64_t timeScale{1};
    std::uint32_t hotQtyCount{0};
    std::uint32_t hotQtyBits{7};
    std::uint32_t sideMode{0};
    std::uint32_t timeStreamBytes{0};
    std::uint32_t priceStreamBytes{0};
    std::uint32_t sideStreamBytes{0};
    std::uint32_t dpZeroStreamBytes{0};
    std::uint32_t countOneStreamBytes{0};
    std::uint32_t priceGroupCountOneStreamBytes{0};
    std::uint32_t qtyCodeStreamBytes{0};
    std::uint32_t qtyEscapeStreamBytes{0};
    std::uint32_t hotQtyTableBytes{0};
    std::uint32_t residualStreamBytes{0};
    std::uint32_t dpZeroCount{0};
    std::uint32_t countOneCount{0};
    std::uint32_t priceGroupCountOneCount{0};
    std::uint32_t payloadCrc32c{0};
};

template <typename T>
void writeLe(std::vector<std::uint8_t>& out, T value) {
    const auto raw = static_cast<std::uint64_t>(value);
    for (std::size_t i = 0; i < sizeof(T); ++i) {
        out.push_back(static_cast<std::uint8_t>((raw >> (i * 8u)) & 0xffu));
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

struct BitWriter {
    std::vector<std::uint8_t> bytes;
    std::uint8_t current{0};
    unsigned bitCount{0};

    void writeBits(std::uint64_t value, unsigned bits) {
        for (unsigned i = 0; i < bits; ++i) {
            current |= static_cast<std::uint8_t>(((value >> i) & 1u) << bitCount);
            ++bitCount;
            if (bitCount == 8u) {
                bytes.push_back(current);
                current = 0;
                bitCount = 0;
            }
        }
    }

    std::vector<std::uint8_t> finish() {
        if (bitCount != 0u) {
            bytes.push_back(current);
            current = 0;
            bitCount = 0;
        }
        return std::move(bytes);
    }
};

void writeVarint(std::vector<std::uint8_t>& out, std::uint64_t value);
bool readVarint(const std::uint8_t*& p, const std::uint8_t* end, std::uint64_t& out) noexcept;
std::uint64_t zigzag(std::int64_t value) noexcept;
std::int64_t unzigzag(std::uint64_t value) noexcept;
std::int64_t gcdPositive(std::int64_t a, std::int64_t b) noexcept;
unsigned bitsForHotCapacity(std::uint32_t capacity) noexcept;
std::int64_t safeScale(std::int64_t value) noexcept;
std::vector<std::uint8_t> serializeFileHeader(FileHeader header, bool includeCrc);
std::vector<std::uint8_t> serializeChunkHeader(const ChunkHeader& header);
bool parseFileHeader(const std::uint8_t* data, std::size_t size, FileHeader& out) noexcept;
bool parseChunkHeader(const std::uint8_t* data, std::size_t size, ChunkHeader& out) noexcept;
std::uint32_t headerCrc32c(const FileHeader& header);
bool validHeader(const FileHeader& header) noexcept;
bool validChunkHeader(const ChunkHeader& header) noexcept;
void buildHotQty(const std::vector<Trade>& trades, std::size_t begin, std::size_t end,
                 EncodedChunk& chunk, bool useCompactQuantityTable);
std::uint64_t qtyCode(const EncodedChunk& chunk, std::int64_t qty) noexcept;
EncodedChunk encodeChunk(const ByteBlockLayout& layout,
                         std::span<const std::uint8_t> scratchRecords,
                         const std::vector<Trade>& trades,
                         std::size_t begin,
                         std::size_t end);
void appendChunkBytes(std::vector<std::uint8_t>& out, const EncodedChunk& chunk);
ReplayArtifactInfo failArtifact(const std::filesystem::path& path, Status status, std::string error);
bool readFile(const std::filesystem::path& path, std::vector<std::uint8_t>& out);
Status walkSource(const internal::DecodeSource& file,
                  const DecodedBlockCallback* onSlice,
                  std::ostream* encodedJsonOut,
                  std::ostream* binaryDumpOut,
                  FileHeader* parsedHeader);
Status walkFile(std::span<const std::uint8_t> file,
                const DecodedBlockCallback* onSlice,
                std::ostream* encodedJsonOut,
                std::ostream* binaryDumpOut,
                FileHeader* parsedHeader = nullptr) noexcept;
Status writeStringBlock(const std::string& text, const DecodedBlockCallback& onBlock);

}  // namespace hft_compressor::codecs::trades_grouped_delta_qtydict::codec_detail

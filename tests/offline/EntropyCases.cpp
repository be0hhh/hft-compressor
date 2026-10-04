#include "OfflineCase.hpp"
#include "hft_compressor/Compressor.hpp"
#include "../../src/Runtime/src/Codecs/EntropyHftmac/EntropyHftMacInternal.hpp"

#include <algorithm>
#include <array>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <limits>
#include <new>
#include <stdexcept>
#include <string>
#include <unistd.h>

// This executable links --wrap=_Znwm. Fail real C++ allocations by ordinal;
// do not assume unordered_map node/bucket sizes or add production test hooks.
namespace allocation_probe {
thread_local bool enabled = false;
thread_local std::size_t ordinal = 0;
thread_local std::size_t failAt = 0;
struct Scope {
  explicit Scope(std::size_t failure) { ordinal = 0; failAt = failure; enabled = true; }
  ~Scope() { enabled = false; }
};
}
extern "C" void* __real__Znwm(std::size_t);
extern "C" void* __wrap__Znwm(std::size_t size) {
  if (allocation_probe::enabled && ++allocation_probe::ordinal == allocation_probe::failAt) {
    allocation_probe::enabled = false;
    throw std::bad_alloc{};
  }
  return __real__Znwm(size);
}

namespace {
using namespace hft_compressor;
namespace entropy = codecs::entropy_hftmac;
namespace detail = entropy::detail;
constexpr const char* kTrades = "[100000001,200000003,0,1000000000]\n[100000002,300000004,1,1000000001]\n";
constexpr const char* kBbo = "[100000001,200000003,100000011,400000005,1000000000]\n[100000002,200000003,100000012,400000006,1000000001]\n";
constexpr const char* kDepth = "[[100000001,200000003,0],[100000011,400000005,1],1000000000]\n[[100000001,0,0],[100000012,400000006,1],1000000001]\n";

struct Fixture {
  std::filesystem::path root = std::filesystem::current_path() /
      ("entropy-fixture-" + std::to_string(getpid()) + "-" +
       std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
  Fixture() { CXET_CHECK(std::filesystem::create_directories(root)); }
  ~Fixture() { std::error_code error; std::filesystem::remove_all(root, error); }
  std::vector<std::uint8_t> encoded(const char* file, const std::string& rows,
                                  const std::string& pipeline) {
    auto input = root / file;
    { std::ofstream out(input, std::ios::binary); out << rows; CXET_CHECK(out.good()); }
    CompressionRequest request{};
    request.inputPath = input;
    request.outputRoot = root / "output";
    request.pipelineId = pipeline;
    const auto result = compress(request);
    CXET_CHECK(result.status == Status::Ok && result.roundtripOk);
    std::ifstream in(result.outputPath, std::ios::binary);
    CXET_CHECK(in.good());
    return {std::istreambuf_iterator<char>(in), std::istreambuf_iterator<char>()};
  }
};

void rewriteHeader(std::vector<std::uint8_t>& bytes, detail::Header header) {
  header.headerCrc32c = detail::headerCrc32c(header);
  const auto encoded = detail::serializeHeader(header, true);
  std::copy(encoded.begin(), encoded.end(), bytes.begin());
}

std::uint64_t readLe(const std::vector<std::uint8_t>& bytes, std::size_t offset, unsigned count) {
  CXET_CHECK(offset + count <= bytes.size());
  std::uint64_t out{};
  for (unsigned i = 0; i < count; ++i) out |= static_cast<std::uint64_t>(bytes[offset + i]) << (8u * i);
  return out;
}
void writeLe(std::vector<std::uint8_t>& bytes, std::size_t offset, unsigned count, std::uint64_t value) {
  CXET_CHECK(offset + count <= bytes.size());
  for (unsigned i = 0; i < count; ++i) bytes[offset + i] = static_cast<std::uint8_t>(value >> (8u * i));
}
std::vector<std::uint8_t> wrapDepth(const std::vector<std::uint8_t>& base) {
  const auto payload = detail::arithmeticEncode(base, detail::EntropyKind::Ac16Ctx0);
  detail::Header header{};
  header.base = static_cast<std::uint16_t>(detail::BaseKind::Depth);
  header.entropy = static_cast<std::uint16_t>(detail::EntropyKind::Ac16Ctx0);
  header.stream = format::streamToWire(StreamType::Depth);
  header.baseBytes = base.size(); header.inputBytes = std::string_view{kDepth}.size(); header.lineCount = 2;
  header.payloadBytes = payload.size(); header.outputBytes = detail::kHeaderBytes + payload.size();
  header.payloadCrc32c = format::crc32c(payload); header.decodedCrc32c = format::crc32c(base);
  header.headerCrc32c = detail::headerCrc32c(header);
  auto out = detail::serializeHeader(header, true);
  out.insert(out.end(), payload.begin(), payload.end());
  return out;
}

void enormousDeclaredBaseSizeCannotAllocate() {
  detail::Header header{};
  header.entropy = 1;
  header.base = 1;
  header.stream = 1;
  header.baseBytes = std::numeric_limits<std::uint64_t>::max();
  header.payloadBytes = 1;
  header.outputBytes = detail::kHeaderBytes + 1;
  const std::array<std::uint8_t, 1> payload{};
  header.payloadCrc32c = format::crc32c(payload);
  auto bytes = detail::serializeHeader(header, false);
  bytes.push_back(0);
  rewriteHeader(bytes, header);
  CXET_CHECK(detail::parseHeader(bytes.data(), bytes.size(), header) && detail::validHeader(header));
  std::size_t callbacks = 0;
  CXET_CHECK(entropy::decode(bytes, [&](auto) { ++callbacks; return true; }) == Status::CorruptData);
  CXET_CHECK(callbacks == 0);
  Fixture fixture;
  const auto path = fixture.root / "malicious.ehf";
  { std::ofstream out(path, std::ios::binary); out.write(reinterpret_cast<const char*>(bytes.data()), bytes.size()); CXET_CHECK(out.good()); }
  CXET_CHECK(entropy::decodeFile(path, [&](auto) { ++callbacks; return true; }) == Status::CorruptData);
  CXET_CHECK(callbacks == 0);
}

void overflowingContainerSizesRefuseHeader() {
  detail::Header header{};
  header.entropy = 1;
  header.base = 1;
  header.stream = 1;
  header.baseBytes = 96;
  header.payloadBytes = std::numeric_limits<std::uint64_t>::max();
  header.outputBytes = detail::kHeaderBytes - 1;
  header.headerCrc32c = detail::headerCrc32c(header);
  CXET_CHECK(!detail::validHeader(header));
}

void truncationAndCorruptPayloadNeverPublish() {
  Fixture fixture;
  auto bytes = fixture.encoded("trades.jsonl", kTrades, "hftmac.trades_grouped_delta_qtydict_ac16_ctx0_v1");
  std::size_t callbacks = 0;
  auto callback = [&](auto) { ++callbacks; return true; };
  auto truncated = bytes;
  truncated.pop_back();
  CXET_CHECK(entropy::decode(truncated, callback) == Status::CorruptData);
  detail::Header shortened{};
  CXET_CHECK(detail::parseHeader(truncated.data(), truncated.size(), shortened));
  --shortened.payloadBytes; --shortened.outputBytes;
  shortened.payloadCrc32c = format::crc32c(std::span{truncated}.subspan(detail::kHeaderBytes));
  rewriteHeader(truncated, shortened);
  CXET_CHECK(entropy::decode(truncated, callback) == Status::CorruptData);
  bytes.back() ^= 0x80;
  CXET_CHECK(entropy::decode(bytes, callback) == Status::CorruptData);
  // The entropy CRCs are correct; the native batch itself declares more levels
  // than its consumed columns contain. It must refuse before any batch reserve.
  auto base = fixture.encoded("depth.jsonl", kDepth, "hftmac.depth_ladder_offset_v3");
  const auto batchStart = 176u + static_cast<std::size_t>(readLe(base, 76, 4));
  CXET_CHECK(base.at(batchStart) == 0u && base.at(batchStart + 1u) == 2u);
  const auto batchBytes = readLe(base, 80, 4);
  base.erase(base.begin() + batchStart + 1u);
  std::array<std::uint8_t, 10> impossibleCount{};
  impossibleCount.fill(0xffu); impossibleCount.back() = 1u;
  base.insert(base.begin() + batchStart + 1u, impossibleCount.begin(), impossibleCount.end());
  writeLe(base, 80, 4, batchBytes + 9u);
  writeLe(base, 16, 8, base.size());
  const auto malformed = wrapDepth(base);
  CXET_CHECK(entropy::decode(malformed, callback) == Status::CorruptData);
  CXET_CHECK(codecs::depth_ladder_offset::decode(base, callback) == Status::CorruptData);
  CXET_CHECK(callbacks == 0);
}

void decodedCrcAndTerminationPrecedePublication() {
  Fixture fixture;
  auto bytes = fixture.encoded("trades.jsonl", kTrades, "hftmac.trades_grouped_delta_qtydict_ac16_ctx0_v1");
  detail::Header header{};
  CXET_CHECK(detail::parseHeader(bytes.data(), bytes.size(), header));
  header.decodedCrc32c ^= 1;
  rewriteHeader(bytes, header);
  std::size_t callbacks = 0;
  CXET_CHECK(entropy::decode(bytes, [&](auto) { ++callbacks; return true; }) == Status::CorruptData);
  CXET_CHECK(callbacks == 0);
  header.decodedCrc32c ^= 1;
  ++header.baseBytes;
  rewriteHeader(bytes, header);
  CXET_CHECK(entropy::decode(bytes, [&](auto) { ++callbacks; return true; }) == Status::CorruptData);
  CXET_CHECK(callbacks == 0);
  // An extra CRC-valid zero byte is not a complete canonical stream.
  bytes = fixture.encoded("trades.jsonl", kTrades, "hftmac.trades_grouped_delta_qtydict_ac16_ctx0_v1");
  CXET_CHECK(detail::parseHeader(bytes.data(), bytes.size(), header));
  bytes.push_back(0);
  ++header.payloadBytes; ++header.outputBytes;
  header.payloadCrc32c = format::crc32c(std::span{bytes}.subspan(detail::kHeaderBytes));
  rewriteHeader(bytes, header);
  CXET_CHECK(entropy::decode(bytes, [&](auto) { ++callbacks; return true; }) == Status::CorruptData);
  CXET_CHECK(callbacks == 0);
}

void callbackStopAndThrowReturnStatus() {
  Fixture fixture;
  const auto bytes = fixture.encoded("trades.jsonl", kTrades, "hftmac.trades_grouped_delta_qtydict_ac16_ctx0_v1");
  std::size_t callbacks = 0;
  CXET_CHECK(entropy::decode(bytes, [&](auto) { ++callbacks; return false; }) == Status::CallbackStopped);
  CXET_CHECK(callbacks == 1);
  CXET_CHECK(entropy::decode(bytes, [](auto) -> bool { throw std::runtime_error("callback failure"); }) == Status::DecodeError);
  CXET_CHECK(entropy::decode(bytes, [](auto) -> bool { throw std::bad_alloc{}; }) == Status::DecodeError);
  const auto path = fixture.root / "changing.ehf";
  { std::ofstream out(path, std::ios::binary); out.write(reinterpret_cast<const char*>(bytes.data()), bytes.size()); CXET_CHECK(out.good()); }
  bool changed = false;
  CXET_CHECK(entropy::decodeFile(path, [&](auto) {
    if (!changed) {
      std::fstream out(path, std::ios::binary | std::ios::in | std::ios::out);
      out.seekp(static_cast<std::streamoff>(bytes.size() - 1u));
      out.put(static_cast<char>(bytes.back() ^ 1u)); out.close();
      CXET_CHECK(!out.fail()); changed = true;
    }
    return true;
  }) == Status::CorruptData);
  CXET_CHECK(changed);
  const auto mutateAndStop = [](const std::filesystem::path& path, const std::vector<std::uint8_t>& input) {
    std::fstream out(path, std::ios::binary | std::ios::in | std::ios::out);
    out.seekp(static_cast<std::streamoff>(input.size() - 1u));
    out.put(static_cast<char>(input.back() ^ 1u)); out.close();
    CXET_CHECK(!out.fail());
    return false;
  };
  { std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out.write(reinterpret_cast<const char*>(bytes.data()), bytes.size()); CXET_CHECK(out.good()); }
  CXET_CHECK(entropy::decodeFile(path, [&](auto) { return mutateAndStop(path, bytes); }) == Status::CorruptData);
  struct Native { const char* file; const char* rows; const char* pipeline;
    Status (*decode)(const std::filesystem::path&, const DecodedBlockCallback&) noexcept; };
  const Native natives[]{
    {"trades.jsonl",kTrades,"hftmac.trades_grouped_delta_qtydict_math_v3",codecs::trades_grouped_delta_qtydict::decodeFile},
    {"bookticker.jsonl",kBbo,"hftmac.bookticker_delta_mask_v2",codecs::bookticker_delta_mask::decodeFile},
    {"depth.jsonl",kDepth,"hftmac.depth_ladder_offset_v3",codecs::depth_ladder_offset::decodeFile},
  };
  for (const auto& native : natives) {
    const auto input = fixture.encoded(native.file,native.rows,native.pipeline);
    { std::ofstream out(path, std::ios::binary | std::ios::trunc);
      out.write(reinterpret_cast<const char*>(input.data()), input.size()); CXET_CHECK(out.good()); }
    CXET_CHECK(native.decode(path,[&](auto) { return mutateAndStop(path,input); }) == Status::CorruptData);
  }
}

void largeTradesCrossBlocksWithoutWholeFileOutput() {
  Fixture fixture;
  std::string input;
  for (std::uint64_t i = 0; i < 33000; ++i)
    input += "[" + std::to_string(100000001 + i) + ",200000003,0," + std::to_string(1000000000 + i) + "]\n";
  const auto bytes = fixture.encoded("trades.jsonl", input, "hftmac.trades_grouped_delta_qtydict_ac16_ctx8_v1");
  std::string decoded;
  std::size_t callbacks = 0;
  CXET_CHECK(entropy::decode(bytes, [&](auto block) {
    CXET_CHECK(!block.empty() && block.size() <= 65536);
    decoded.append(reinterpret_cast<const char*>(block.data()), block.size());
    ++callbacks;
    return true;
  }) == Status::Ok);
  CXET_CHECK(callbacks > 2 && decoded == input);
}

void currentEntropyVariantsPreserveAllBaseCodecs() {
  Fixture fixture;
  constexpr const char* suffixes[]{"ac16_ctx0", "ac16_ctx8", "ac16_ctx12", "ac32_ctx8", "range_byte_ctx8", "rans_byte_static"};
  struct Base { const char* file; const char* rows; const char* prefix; };
  constexpr Base bases[]{
    {"trades.jsonl", kTrades, "hftmac.trades_grouped_delta_qtydict_"},
    {"bookticker.jsonl", kBbo, "hftmac.bookticker_delta_mask_"},
    {"depth.jsonl", kDepth, "hftmac.depth_ladder_offset_"},
  };
  for (const auto& base : bases) for (const char* suffix : suffixes) {
    const auto bytes = fixture.encoded(base.file, base.rows, std::string(base.prefix) + suffix + "_v1");
    std::string decoded;
    CXET_CHECK(entropy::decode(bytes, [&](auto block) {
      decoded.append(reinterpret_cast<const char*>(block.data()), block.size()); return true;
    }) == Status::Ok);
    CXET_CHECK(decoded == base.rows);
  }
}
void actualDecoderAndDepthBookAllocationFailuresReturnStatus() {
  Fixture fixture;
  const auto bboPath = fixture.root / "BOOKTICKER.JSONL";
  { allocation_probe::Scope counting(0);
    const auto type = inferStreamTypeFromPath(bboPath);
    const auto allocations = allocation_probe::ordinal;
    CXET_CHECK(type == StreamType::BookTicker && allocations == 0u);
  }
  // Fail a real allocation inside each backend entrypoint, rather than at
  // the public API's preceding staging work or inside a throwing callback.
  struct Encoder {
    const char* file; const char* rows; const char* pipeline;
    CompressionResult (*run)(const CompressionRequest&, const PipelineDescriptor&) noexcept;
  };
  const Encoder encoders[]{
    {"trades.jsonl", kTrades, "hftmac.trades_grouped_delta_qtydict_math_v3", &codecs::trades_grouped_delta_qtydict::compress},
    {"bookticker.jsonl", kBbo, "hftmac.bookticker_delta_mask_v2", &codecs::bookticker_delta_mask::compress},
    {"depth.jsonl", kDepth, "hftmac.depth_ladder_offset_v3", &codecs::depth_ladder_offset::compress},
    {"depth.jsonl", kDepth, "hftmac.depth_ladder_offset_ac16_ctx0_v1", &entropy::compress},
  };
  for (const auto& encoder : encoders) {
    CompressionRequest request{};
    request.inputPath = fixture.root / encoder.file;
    request.outputPathOverride = fixture.root / "failed-allocation.hfc";
    { std::ofstream out(request.inputPath, std::ios::binary); out << encoder.rows; CXET_CHECK(out.good()); }
    const auto* pipeline = findPipeline(encoder.pipeline);
    CXET_CHECK(pipeline);
    CompressionResult result{};
    { allocation_probe::Scope failure(1); result = encoder.run(request, *pipeline); }
    CXET_CHECK(result.status == Status::DecodeError && !result.roundtripOk);
    CXET_CHECK(!std::filesystem::exists(request.outputPathOverride));
  }
  const auto encoded = fixture.encoded("depth.jsonl", kDepth, "hftmac.depth_ladder_offset_ac16_ctx0_v1");
  std::size_t callbacks = 0;
  const DecodedBlockCallback accept = [&](auto) { ++callbacks; return true; };
  Status status{};
  { allocation_probe::Scope failure(1); status = entropy::decode(encoded, accept); }
  CXET_CHECK(status == Status::DecodeError && callbacks == 0);
  const auto base = fixture.encoded("depth.jsonl", kDepth, "hftmac.depth_ladder_offset_v3");
  std::size_t allocations{};
  { allocation_probe::Scope counting(0);
    status = codecs::depth_ladder_offset::decode(base, accept);
    allocations = allocation_probe::ordinal;
  }
  CXET_CHECK(status == Status::Ok && callbacks == 1 && allocations > 0);
  // The baseline executes both real BookState maps with actual bid/ask levels.
  // Sweeping every observed allocation also fails their nodes and bucket arrays,
  // as well as decoder workspaces, independent of standard-library layouts.
  for (std::size_t failure = 1; failure <= allocations; ++failure) {
    callbacks = 0;
    { allocation_probe::Scope injection(failure);
      status = codecs::depth_ladder_offset::decode(base, accept);
    }
    CXET_CHECK(status == Status::DecodeError && callbacks == 0);
  }
}
}

int main(int argc, char** argv) {
  const cxet::testing::Case cases[]{
    cxet::testing::Case{"entropy.enormous_declared_size_refuses_without_allocation", enormousDeclaredBaseSizeCannotAllocate},
    cxet::testing::Case{"entropy.overflowing_header_size_is_corrupt", overflowingContainerSizesRefuseHeader},
    cxet::testing::Case{"entropy.truncated_or_bad_payload_crc_never_publishes", truncationAndCorruptPayloadNeverPublish},
    cxet::testing::Case{"entropy.decoded_crc_and_termination_precede_publication", decodedCrcAndTerminationPrecedePublication},
    cxet::testing::Case{"entropy.callback_stop_and_exception_return_status", callbackStopAndThrowReturnStatus},
    cxet::testing::Case{"entropy.large_trades_cross_bounded_output_blocks", largeTradesCrossBlocksWithoutWholeFileOutput},
    cxet::testing::Case{"entropy.current_variants_preserve_three_base_codecs", currentEntropyVariantsPreserveAllBaseCodecs},
    cxet::testing::Case{"entropy.actual_decoder_and_depth_book_allocation_failures_return_status", actualDecoderAndDepthBookAllocationFailuresReturnStatus},
  };
  return cxet::testing::runCases(argc, argv, cases);
}

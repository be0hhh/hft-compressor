#include <cstdio>
#include <filesystem>
#include <span>
#include <string_view>

#include "codecs/bookticker_delta_mask/BookTickerDeltaMask.hpp"
#include "codecs/depth_ladder_offset/DepthLadderOffset.hpp"
#include "codecs/depth_ladder_offset/DepthLadderOffsetV2.hpp"
#include "codecs/entropy_hftmac/EntropyHftMac.hpp"
#include "codecs/trades_grouped_delta_qtydict/TradesGroupedDeltaQtyDict.hpp"
#include "hft_compressor/compressor.hpp"

namespace {

void printBlock(std::span<const std::uint8_t> block) {
    if (!block.empty()) std::fwrite(block.data(), 1u, block.size(), stdout);
}

bool argEquals(char* arg, std::string_view value) noexcept {
    return arg != nullptr && std::string_view{arg} == value;
}

}  // namespace

int main(int argc, char** argv) {
    if (argc >= 2 && std::string_view{argv[1]} == "--list-pipelines") {
        for (const auto& pipeline : hft_compressor::listPipelines()) {
            std::printf("%s\t%s\t%s\t%s\t%s\n",
                        pipeline.id.data(),
                        hft_compressor::pipelineAvailabilityToString(pipeline.availability).data(),
                        pipeline.streamScope.data(),
                        pipeline.representation.data(),
                        pipeline.entropy.data());
        }
        return 0;
    }
    if (argc >= 6 && std::string_view{argv[1]} == "inspect") {
        std::filesystem::path input;
        std::string_view view;
        for (int i = 2; i + 1 < argc; i += 2) {
            if (argEquals(argv[i], "--input")) input = std::filesystem::path{argv[i + 1]};
            else if (argEquals(argv[i], "--view")) view = std::string_view{argv[i + 1]};
        }
        hft_compressor::Status status = hft_compressor::Status::InvalidArgument;
        auto callback = [](std::span<const std::uint8_t> block) noexcept -> bool {
            printBlock(block);
            return true;
        };
        const auto tryEntropy = [&]() noexcept {
            if (view == "canonical-json" || view == "canonical-jsonl") return hft_compressor::codecs::entropy_hftmac::decodeFile(input, callback);
            if (view == "encoded-json") return hft_compressor::codecs::entropy_hftmac::inspectEncodedJsonFile(input, callback);
            if (view == "encoded-binary") return hft_compressor::codecs::entropy_hftmac::inspectEncodedBinaryFile(input, callback);
            if (view == "stats") return hft_compressor::codecs::entropy_hftmac::inspectStatsJsonFile(input, callback);
            return hft_compressor::Status::InvalidArgument;
        };
        const auto tryTrade = [&]() noexcept {
            if (view == "canonical-json" || view == "canonical-jsonl") return hft_compressor::codecs::trades_grouped_delta_qtydict::decodeFile(input, callback);
            if (view == "encoded-json") return hft_compressor::codecs::trades_grouped_delta_qtydict::inspectEncodedJsonFile(input, callback);
            if (view == "encoded-binary") return hft_compressor::codecs::trades_grouped_delta_qtydict::inspectEncodedBinaryFile(input, callback);
            if (view == "stats") return hft_compressor::codecs::trades_grouped_delta_qtydict::inspectStatsJsonFile(input, callback);
            return hft_compressor::Status::InvalidArgument;
        };
        const auto tryBookTicker = [&]() noexcept {
            if (view == "canonical-json" || view == "canonical-jsonl") return hft_compressor::codecs::bookticker_delta_mask::decodeFile(input, callback);
            if (view == "encoded-json") return hft_compressor::codecs::bookticker_delta_mask::inspectEncodedJsonFile(input, callback);
            if (view == "encoded-binary") return hft_compressor::codecs::bookticker_delta_mask::inspectEncodedBinaryFile(input, callback);
            if (view == "stats") return hft_compressor::codecs::bookticker_delta_mask::inspectStatsJsonFile(input, callback);
            return hft_compressor::Status::InvalidArgument;
        };
        const auto tryDepthV1 = [&]() noexcept {
            if (view == "canonical-json" || view == "canonical-jsonl") return hft_compressor::codecs::depth_ladder_offset::decodeFile(input, callback);
            if (view == "encoded-json") return hft_compressor::codecs::depth_ladder_offset::inspectEncodedJsonFile(input, callback);
            if (view == "encoded-binary") return hft_compressor::codecs::depth_ladder_offset::inspectEncodedBinaryFile(input, callback);
            if (view == "stats") return hft_compressor::codecs::depth_ladder_offset::inspectStatsJsonFile(input, callback);
            return hft_compressor::Status::InvalidArgument;
        };
        const auto tryDepthV2 = [&]() noexcept {
            if (view == "canonical-json" || view == "canonical-jsonl") return hft_compressor::codecs::depth_ladder_offset_v2::decodeFile(input, callback);
            if (view == "encoded-json") return hft_compressor::codecs::depth_ladder_offset_v2::inspectEncodedJsonFile(input, callback);
            if (view == "encoded-binary") return hft_compressor::codecs::depth_ladder_offset_v2::inspectEncodedBinaryFile(input, callback);
            if (view == "stats") return hft_compressor::codecs::depth_ladder_offset_v2::inspectStatsJsonFile(input, callback);
            return hft_compressor::Status::InvalidArgument;
        };
        const auto tryDepth = [&]() noexcept {
            auto st = tryDepthV1();
            if (!hft_compressor::isOk(st)) st = tryDepthV2();
            return st;
        };
        status = tryEntropy();
        if (!hft_compressor::isOk(status)) status = tryTrade();
        if (!hft_compressor::isOk(status)) status = tryBookTicker();
        if (!hft_compressor::isOk(status)) status = tryDepth();
        if (!hft_compressor::isOk(status)) {
            std::fprintf(stderr, "status=%s\n", hft_compressor::statusToString(status).data());
            return 1;
        }
        return 0;
    }
    if (argc < 4 || std::string_view{argv[2]} != "--pipeline") {
        std::puts("Usage: hft-compressor --list-pipelines");
        std::puts("Usage: hft-compressor <trades.jsonl|bookticker.jsonl|depth.jsonl> --pipeline <pipeline_id> [output_root]");
        std::puts("Usage: hft-compressor inspect --input <artifact> --view <canonical-json|encoded-json|encoded-binary|stats>");
        return 0;
    }
    hft_compressor::CompressionRequest request{};
    request.inputPath = std::filesystem::path{argv[1]};
    request.pipelineId = argv[3];
    if (argc >= 5) request.outputRoot = std::filesystem::path{argv[4]};
    const auto result = hft_compressor::compress(request);
    std::printf("status=%s\n", hft_compressor::statusToString(result.status).data());
    std::printf("pipeline=%s\n", result.pipelineId.c_str());
    std::printf("output=%s\n", result.outputPath.string().c_str());
    std::printf("ratio=%.4f encode_mb_s=%.2f decode_mb_s=%.2f\n",
                hft_compressor::ratio(result),
                hft_compressor::encodeMbPerSec(result),
                hft_compressor::decodeMbPerSec(result));
    if (!hft_compressor::isOk(result.status)) {
        std::printf("error=%s\n", result.error.c_str());
        return 1;
    }
    return 0;
}

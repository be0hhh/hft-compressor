#include <cstdio>
#include <filesystem>
#include <span>
#include <string_view>

#include "../Runtime/src/Codecs/BooktickerDeltaMask/BookTickerDeltaMask.hpp"
#include "../Runtime/src/Codecs/DepthLadderOffset/DepthLadderOffset.hpp"
#include "../Runtime/src/Codecs/EntropyHftmac/EntropyHftMac.hpp"
#include "../Runtime/src/Codecs/TradesGroupedDeltaQtydict/TradesGroupedDeltaQtyDict.hpp"
#include "hft_compressor/Compressor.hpp"

namespace {

bool printBlock(std::span<const std::uint8_t> block) noexcept {
    return (block.empty() || std::fwrite(block.data(), 1u, block.size(), stdout) == block.size())
        && std::ferror(stdout) == 0;
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
            return printBlock(block);
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
        const auto tryCurrentDepth = [&]() noexcept {
            if (view == "canonical-json" || view == "canonical-jsonl") return hft_compressor::codecs::depth_ladder_offset::decodeFile(input, callback);
            if (view == "encoded-json") return hft_compressor::codecs::depth_ladder_offset::inspectEncodedJsonFile(input, callback);
            if (view == "encoded-binary") return hft_compressor::codecs::depth_ladder_offset::inspectEncodedBinaryFile(input, callback);
            if (view == "stats") return hft_compressor::codecs::depth_ladder_offset::inspectStatsJsonFile(input, callback);
            return hft_compressor::Status::InvalidArgument;
        };
        // Probe metadata without output, then invoke one exact format decoder.
        // A later error must not append another codec's output to a failed prefix.
        const auto matches = [&](std::string_view pipelineId, auto inspect) {
            const auto* pipeline = hft_compressor::findPipeline(pipelineId);
            return pipeline && hft_compressor::isOk(inspect(input, *pipeline).status);
        };
        if (matches("hftmac.trades_grouped_delta_qtydict_ac16_ctx0_v1", hft_compressor::codecs::entropy_hftmac::inspectArtifact))
            status = tryEntropy();
        else if (matches("hftmac.trades_grouped_delta_qtydict_math_v3", hft_compressor::codecs::trades_grouped_delta_qtydict::inspectArtifact))
            status = tryTrade();
        else if (matches("hftmac.bookticker_delta_mask_v2", hft_compressor::codecs::bookticker_delta_mask::inspectArtifact))
            status = tryBookTicker();
        else if (matches("hftmac.depth_ladder_offset_v3", hft_compressor::codecs::depth_ladder_offset::inspectArtifact))
            status = tryCurrentDepth();
        else status = hft_compressor::Status::CorruptData;
        if (hft_compressor::isOk(status) && std::fflush(stdout) != 0)
            status = hft_compressor::Status::IoError;
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
    std::printf("output=%s\n", hft_compressor::isOk(result.status) ? result.outputPath.string().c_str() : "");
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

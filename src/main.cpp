#include <cstdio>
#include <filesystem>
#include <string_view>

#include "hft_compressor/compressor.hpp"

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
    if (argc < 4 || std::string_view{argv[2]} != "--pipeline") {
        std::puts("Usage: hft-compressor --list-pipelines");
        std::puts("Usage: hft-compressor <trades.jsonl|bookticker.jsonl|depth.jsonl> --pipeline <pipeline_id> [output_root]");
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

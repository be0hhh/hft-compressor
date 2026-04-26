#include <cstdio>
#include <filesystem>

#include "hft_compressor/compressor.hpp"

int main(int argc, char** argv) {
    if (argc < 2) {
        std::puts("Usage: hft-compressor <trades.jsonl|bookticker.jsonl|depth.jsonl> [output_root]");
        return 0;
    }
    hft_compressor::CompressionRequest request{};
    request.inputPath = std::filesystem::path{argv[1]};
    if (argc >= 3) request.outputRoot = std::filesystem::path{argv[2]};
    const auto result = hft_compressor::compressFile(request);
    std::printf("status=%s\n", hft_compressor::statusToString(result.status).data());
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


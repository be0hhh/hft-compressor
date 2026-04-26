#include <cassert>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <vector>

#include "hft_compressor/compressor.hpp"

namespace fs = std::filesystem;

namespace {

void writeFile(const fs::path& path, const std::string& text) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out << text;
}

}  // namespace

int main() {
    using hft_compressor::StreamType;
    assert(hft_compressor::inferStreamTypeFromPath("trades.jsonl") == StreamType::Trades);
    assert(hft_compressor::inferStreamTypeFromPath("bookticker.jsonl") == StreamType::BookTicker);
    assert(hft_compressor::inferStreamTypeFromPath("depth.jsonl") == StreamType::Depth);

    const auto dir = fs::temp_directory_path() / "hft_compressor_tests";
    fs::create_directories(dir);
    const auto input = dir / "trades.jsonl";
    writeFile(input, "[1,2,1,100]\n[2,3,0,200]\n");
    hft_compressor::CompressionRequest request{};
    request.inputPath = input;
    request.outputRoot = dir / "compressedData" / "zstd" / "sessions";
    request.blockBytes = 16;
    const auto result = hft_compressor::compressFile(request);
#if HFT_COMPRESSOR_WITH_ZSTD
    assert(hft_compressor::isOk(result.status));
    assert(result.lineCount == 2u);
    assert(result.blockCount >= 1u);
    assert(fs::exists(result.outputPath));
    std::ifstream in(result.outputPath, std::ios::binary);
    std::vector<unsigned char> data((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
    std::string decoded;
    const auto status = hft_compressor::decodeBuffer(data, [&decoded](std::span<const std::uint8_t> block) {
        decoded.append(reinterpret_cast<const char*>(block.data()), block.size());
        return true;
    });
    assert(hft_compressor::isOk(status));
    assert(decoded == "[1,2,1,100]\n[2,3,0,200]\n");
#else
    assert(result.status == hft_compressor::Status::DependencyUnavailable);
#endif
    return 0;
}


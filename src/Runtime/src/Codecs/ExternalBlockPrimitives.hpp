#pragma once

#include <cstddef>
#include <cstdint>
#include <span>

#include "hft_compressor/Status.hpp"

// Internal scratch-reusing block primitives provided by the five reviewed
// external-library translation units (Lz4, Zstd, Brotli, Xz, Gzip).
namespace hft_compressor::codecs::external_primitives {

bool lz4Available() noexcept;
std::size_t lz4StateBytes() noexcept;
std::size_t lz4CompressBound(std::size_t inputBytes) noexcept;
Status lz4CompressFast(
    std::span<std::uint8_t> state,
    std::span<const std::uint8_t> src,
    std::span<std::uint8_t> dst,
    std::size_t& written) noexcept;
Status lz4DecompressBlock(
    std::span<const std::uint8_t> src,
    std::span<std::uint8_t> dst) noexcept;

bool zstdAvailable() noexcept;
void* createZstdCCtx() noexcept;
void freeZstdCCtx(void* ctx) noexcept;
void* createZstdDCtx() noexcept;
void freeZstdDCtx(void* ctx) noexcept;
std::size_t zstdCompressBound(std::size_t inputBytes) noexcept;
Status zstdCompressWithCtx(
    void* cctx,
    int level,
    std::span<const std::uint8_t> src,
    std::span<std::uint8_t> dst,
    std::size_t& written) noexcept;
Status zstdDecompressWithCtx(
    void* dctx,
    std::span<const std::uint8_t> src,
    std::span<std::uint8_t> dst) noexcept;

bool brotliAvailable() noexcept;
std::size_t brotliCompressBound(std::size_t inputBytes) noexcept;
Status brotliCompressBlock(
    int quality,
    std::span<const std::uint8_t> src,
    std::span<std::uint8_t> dst,
    std::size_t& written) noexcept;
Status brotliDecompressBlock(
    std::span<const std::uint8_t> src,
    std::span<std::uint8_t> dst) noexcept;

bool xzLzmaAvailable() noexcept;
std::size_t xzLzmaCompressBound(std::size_t inputBytes) noexcept;
Status xzLzmaCompressBlock(
    int preset,
    std::span<const std::uint8_t> src,
    std::span<std::uint8_t> dst,
    std::size_t& written) noexcept;
Status xzLzmaDecompressBlock(
    std::span<const std::uint8_t> src,
    std::span<std::uint8_t> dst) noexcept;

} // namespace hft_compressor::codecs::external_primitives

#pragma once

#include <span>
#include <string>
#include <vector>

#include "../Common/DecodeSource.hpp"
#include "hft_compressor/CorpusLab.hpp"

// Internal composition boundary: no new public compressor contract.
namespace hft_compressor::codecs::trades_grouped_delta_qtydict {
Status encodeSliceBytes(
    const ByteBlockLayout& layout,
    std::span<const std::uint8_t> records,
    std::vector<std::uint8_t>& outEncoded,
    std::uint64_t& outRecordCount,
    std::uint64_t& outEncodeCoreNs,
    std::uint64_t& outEncodeCycles,
    std::string& outError);
Status decodeSource(const internal::DecodeSource& source, const DecodedBlockCallback& onBlock);
} // namespace hft_compressor::codecs::trades_grouped_delta_qtydict

namespace hft_compressor::codecs::bookticker_delta_mask {
Status encodeSliceBytes(
    const ByteBlockLayout& layout,
    std::span<const std::uint8_t> records,
    std::vector<std::uint8_t>& outEncoded,
    std::uint64_t& outRecordCount,
    std::uint64_t& outEncodeCoreNs,
    std::uint64_t& outEncodeCycles,
    std::string& outError);
Status decodeSource(const internal::DecodeSource& source, const DecodedBlockCallback& onBlock);
} // namespace hft_compressor::codecs::bookticker_delta_mask

namespace hft_compressor::codecs::depth_ladder_offset {
Status encodeSliceBytes(
    const ByteBlockLayout& layout,
    std::span<const std::uint8_t> records,
    std::vector<std::uint8_t>& outEncoded,
    std::uint64_t& outRecordCount,
    std::uint64_t& outEncodeCoreNs,
    std::uint64_t& outEncodeCycles,
    std::string& outError);
Status decodeSource(const internal::DecodeSource& source, const DecodedBlockCallback& onBlock);
} // namespace hft_compressor::codecs::depth_ladder_offset

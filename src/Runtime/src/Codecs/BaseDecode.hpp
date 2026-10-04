#pragma once

#include "../Common/DecodeSource.hpp"

// Internal composition boundary: no new public compressor contract.
namespace hft_compressor::codecs::trades_grouped_delta_qtydict {
Status decodeSource(const internal::DecodeSource& source, const DecodedBlockCallback& onBlock);
}
namespace hft_compressor::codecs::bookticker_delta_mask {
Status decodeSource(const internal::DecodeSource& source, const DecodedBlockCallback& onBlock);
}
namespace hft_compressor::codecs::depth_ladder_offset {
Status decodeSource(const internal::DecodeSource& source, const DecodedBlockCallback& onBlock);
}

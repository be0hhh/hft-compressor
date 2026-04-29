#pragma once

#include <span>
#include <string_view>

#include "hft_compressor/api.hpp"

namespace hft_compressor {

enum class PipelineAvailability {
    Available,
    DependencyUnavailable,
    NotImplemented,
};

struct PipelineDescriptor {
    std::string_view id{};
    std::string_view label{};
    std::string_view streamScope{};
    std::string_view representation{};
    std::string_view transform{};
    std::string_view entropy{};
    std::string_view profile{};
    std::string_view implementationKind{};
    PipelineAvailability availability{PipelineAvailability::NotImplemented};
    std::string_view availabilityReason{};
    std::string_view outputSlug{};
    std::string_view fileExtension{".hfc"};
    std::string_view capabilities{};
};

HFT_COMPRESSOR_API std::span<const PipelineDescriptor> listPipelines() noexcept;
HFT_COMPRESSOR_API const PipelineDescriptor* findPipeline(std::string_view id) noexcept;
HFT_COMPRESSOR_API std::string_view pipelineAvailabilityToString(PipelineAvailability availability) noexcept;

}  // namespace hft_compressor

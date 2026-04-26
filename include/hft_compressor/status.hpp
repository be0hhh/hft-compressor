#pragma once

#include <string_view>

namespace hft_compressor {

enum class Status {
    Ok,
    InvalidArgument,
    IoError,
    UnsupportedStream,
    DependencyUnavailable,
    CorruptData,
    DecodeError,
};

constexpr bool isOk(Status status) noexcept { return status == Status::Ok; }

constexpr std::string_view statusToString(Status status) noexcept {
    switch (status) {
        case Status::Ok: return "ok";
        case Status::InvalidArgument: return "invalid_argument";
        case Status::IoError: return "io_error";
        case Status::UnsupportedStream: return "unsupported_stream";
        case Status::DependencyUnavailable: return "dependency_unavailable";
        case Status::CorruptData: return "corrupt_data";
        case Status::DecodeError: return "decode_error";
    }
    return "unknown";
}

}  // namespace hft_compressor

#pragma once

#include <cstdint>

#include "hft_compressor/api.hpp"

namespace hft_compressor {

class HFT_COMPRESSOR_API MetricsServer {
  public:
    MetricsServer() = default;
    MetricsServer(const MetricsServer&) = delete;
    MetricsServer& operator=(const MetricsServer&) = delete;
    ~MetricsServer();

    void startFromEnvironment() noexcept;
    void start(std::uint16_t port) noexcept;
    void stop() noexcept;

  private:
    struct Impl;
    Impl* impl_{nullptr};
};

}  // namespace hft_compressor


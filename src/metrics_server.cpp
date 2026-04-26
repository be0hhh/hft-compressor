#include "hft_compressor/metrics_server.hpp"

#include "hft_compressor/metrics.hpp"

#include <atomic>
#include <cstdlib>
#include <cstring>
#include <string>
#include <thread>

#if !defined(_WIN32)
#include <arpa/inet.h>
#include <cerrno>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#endif

namespace hft_compressor {
namespace {

std::uint16_t portFromEnvironment() noexcept {
    const char* raw = std::getenv("HFT_COMPRESSOR_METRICS_PORT");
    if (raw == nullptr || raw[0] == '\0') return 8081u;
    const unsigned long parsed = std::strtoul(raw, nullptr, 10);
    if (parsed == 0u || parsed > 65535u) return 8081u;
    return static_cast<std::uint16_t>(parsed);
}

bool metricsOff() noexcept {
    const char* raw = std::getenv("HFT_COMPRESSOR_METRICS_MODE");
    return raw != nullptr && std::strcmp(raw, "off") == 0;
}

}  // namespace

struct MetricsServer::Impl {
#if !defined(_WIN32)
    explicit Impl(std::uint16_t value) noexcept : port(value) {}
    void start() noexcept {
        stopRequested.store(false, std::memory_order_release);
        try { thread = std::thread([this]() { run(); }); } catch (...) {}
    }
    void stop() noexcept {
        stopRequested.store(true, std::memory_order_release);
        const int fd = listenFd.exchange(-1, std::memory_order_acq_rel);
        if (fd >= 0) { ::shutdown(fd, SHUT_RDWR); ::close(fd); }
        if (thread.joinable()) thread.join();
    }
    void run() noexcept;
    static void handleClient(int client) noexcept;
    std::uint16_t port{8081};
    std::atomic<bool> stopRequested{false};
    std::atomic<int> listenFd{-1};
    std::thread thread{};
#else
    explicit Impl(std::uint16_t) noexcept {}
    void start() noexcept {}
    void stop() noexcept {}
#endif
};

#if !defined(_WIN32)
void MetricsServer::Impl::run() noexcept {
    const int fd = ::socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return;
    listenFd.store(fd, std::memory_order_release);
    int yes = 1;
    (void)::setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (::bind(fd, reinterpret_cast<sockaddr*>(&addr), sizeof(addr)) != 0 || ::listen(fd, 16) != 0) {
        ::close(fd);
        listenFd.store(-1, std::memory_order_release);
        return;
    }
    while (!stopRequested.load(std::memory_order_acquire)) {
        const int client = ::accept(fd, nullptr, nullptr);
        if (client < 0) {
            if (errno == EINTR) continue;
            if (stopRequested.load(std::memory_order_acquire)) break;
            continue;
        }
        handleClient(client);
        ::close(client);
    }
}

void MetricsServer::Impl::handleClient(int client) noexcept {
    char buffer[1024];
    const ssize_t n = ::recv(client, buffer, sizeof(buffer) - 1u, 0);
    if (n <= 0) return;
    buffer[n] = '\0';
    const std::string_view request{buffer, static_cast<std::size_t>(n)};
    std::string body;
    std::string status = "200 OK";
    if (request.rfind("GET /metrics ", 0) == 0 || request.rfind("GET /metrics?", 0) == 0) {
        metrics::renderPrometheus(body);
    } else if (request.rfind("GET /-/ready ", 0) == 0) {
        body = "hft-compressor metrics ready\n";
    } else {
        status = "404 Not Found";
        body = "not found\n";
    }
    const std::string header = "HTTP/1.1 " + status + "\r\nContent-Type: text/plain; charset=utf-8\r\nContent-Length: "
        + std::to_string(body.size()) + "\r\nConnection: close\r\n\r\n";
    (void)::send(client, header.data(), header.size(), MSG_NOSIGNAL);
    if (!body.empty()) (void)::send(client, body.data(), body.size(), MSG_NOSIGNAL);
}
#endif

MetricsServer::~MetricsServer() { stop(); }

void MetricsServer::startFromEnvironment() noexcept {
    if (metricsOff()) return;
    start(portFromEnvironment());
}

void MetricsServer::start(std::uint16_t port) noexcept {
    stop();
    impl_ = new Impl(port);
    impl_->start();
}

void MetricsServer::stop() noexcept {
    if (impl_ != nullptr) {
        impl_->stop();
        delete impl_;
        impl_ = nullptr;
    }
}

}  // namespace hft_compressor

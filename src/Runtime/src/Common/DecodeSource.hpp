#pragma once

#include <algorithm>
#include <array>
#include <cstdint>
#include <filesystem>
#include <cerrno>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <limits>
#include <memory>
#include <span>
#include <string_view>

#include "hft_compressor/Compressor.hpp"

namespace hft_compressor::internal {

// Cold codec input. Each column owns an independent forward cursor, so a
// transformed source can replay without retaining a whole decoded artifact.
class DecodeCursor {
public:
    virtual ~DecodeCursor() = default;
    virtual bool byte(std::uint8_t& out) = 0;
    virtual std::uint64_t remaining() const noexcept = 0;
    bool read(std::span<std::uint8_t> out) {
        for (auto& value : out) if (!byte(value)) return false;
        return true;
    }
    bool skip(std::uint64_t count) {
        std::uint8_t ignored{};
        while (count != 0u) { if (!byte(ignored)) return false; --count; }
        return true;
    }
    bool varint(std::uint64_t& out) {
        out = 0;
        for (unsigned shift = 0; shift <= 63u; shift += 7u) {
            std::uint8_t value{};
            if (!byte(value) || (shift == 63u && (value & 0xfeu) != 0u)) return false;
            out |= static_cast<std::uint64_t>(value & 0x7fu) << shift;
            if ((value & 0x80u) == 0u) return shift == 0u || value != 0u;
        }
        return false;
    }
};

class DecodeSource {
public:
    virtual ~DecodeSource() = default;
    virtual std::uint64_t size() const noexcept = 0;
    virtual std::unique_ptr<DecodeCursor> cursor(std::uint64_t offset, std::uint64_t count) const = 0;
    virtual bool unchanged() const noexcept { return true; }
    bool read(std::uint64_t offset, std::span<std::uint8_t> out) const {
        auto input = cursor(offset, out.size());
        return input && input->read(out);
    }
    bool contains(std::uint64_t offset, std::uint64_t count) const noexcept {
        return offset <= size() && count <= size() - offset;
    }
};

class SpanDecodeSource final : public DecodeSource {
    std::span<const std::uint8_t> bytes_;
    class Cursor final : public DecodeCursor {
        std::span<const std::uint8_t> bytes_;
    public:
        explicit Cursor(std::span<const std::uint8_t> bytes) : bytes_(bytes) {}
        bool byte(std::uint8_t& out) override {
            if (bytes_.empty()) return false;
            out = bytes_.front(); bytes_ = bytes_.subspan(1); return true;
        }
        std::uint64_t remaining() const noexcept override { return bytes_.size(); }
    };
public:
    explicit SpanDecodeSource(std::span<const std::uint8_t> bytes) : bytes_(bytes) {}
    std::uint64_t size() const noexcept override { return bytes_.size(); }
    std::unique_ptr<DecodeCursor> cursor(std::uint64_t offset, std::uint64_t count) const override {
        if (!contains(offset, count)) return {};
        return std::make_unique<Cursor>(bytes_.subspan(static_cast<std::size_t>(offset), static_cast<std::size_t>(count)));
    }
};

class FileDecodeSource final : public DecodeSource {
    int fd_{-1};
    std::uint64_t size_{};
    bool valid_{};
    struct stat identity_{};
    class Cursor final : public DecodeCursor {
        const FileDecodeSource& owner_;
        std::array<std::uint8_t, 16384> buffer_{};
        std::size_t pos_{}, end_{};
        std::uint64_t position_{}, remaining_{};
    public:
        Cursor(const FileDecodeSource& owner, std::uint64_t offset, std::uint64_t count)
            : owner_(owner), position_(offset), remaining_(count) {}
        bool byte(std::uint8_t& out) override {
            if (remaining_ == 0u) return false;
            if (pos_ == end_) {
                const auto count = static_cast<std::size_t>(std::min<std::uint64_t>(remaining_, buffer_.size()));
                std::size_t obtained{};
                while (obtained != count) {
                    const auto result = ::pread(owner_.fd_, buffer_.data() + obtained, count - obtained,
                        static_cast<off_t>(position_ + obtained));
                    if (result < 0 && errno == EINTR) continue;
                    if (result <= 0) return false;
                    obtained += static_cast<std::size_t>(result);
                }
                pos_ = 0; end_ = count;
            }
            out = buffer_[pos_++]; ++position_; --remaining_; return true;
        }
        std::uint64_t remaining() const noexcept override { return remaining_; }
    };
public:
    // All passes stay on one opened inode. Replacing a path cannot substitute a
    // different artifact between its integrity pass and column replay.
    explicit FileDecodeSource(const std::filesystem::path& path) {
        fd_ = ::open(path.c_str(), O_RDONLY | O_CLOEXEC | O_NONBLOCK);
        valid_ = fd_ >= 0 && ::fstat(fd_, &identity_) == 0 && S_ISREG(identity_.st_mode) && identity_.st_size >= 0;
        if (valid_) size_ = static_cast<std::uint64_t>(identity_.st_size);
    }
    ~FileDecodeSource() override { if (fd_ >= 0) ::close(fd_); }
    FileDecodeSource(const FileDecodeSource&) = delete;
    FileDecodeSource& operator=(const FileDecodeSource&) = delete;
    bool valid() const noexcept { return valid_; }
    bool unchanged() const noexcept override {
        struct stat now{};
        return valid_ && ::fstat(fd_, &now) == 0 && now.st_dev == identity_.st_dev && now.st_ino == identity_.st_ino
            && now.st_size == identity_.st_size && now.st_mtim.tv_sec == identity_.st_mtim.tv_sec
            && now.st_mtim.tv_nsec == identity_.st_mtim.tv_nsec && now.st_ctim.tv_sec == identity_.st_ctim.tv_sec
            && now.st_ctim.tv_nsec == identity_.st_ctim.tv_nsec;
    }
    std::uint64_t size() const noexcept override { return size_; }
    std::unique_ptr<DecodeCursor> cursor(std::uint64_t offset, std::uint64_t count) const override {
        if (!valid_ || !contains(offset, count)) return {};
        return std::make_unique<Cursor>(*this, offset, count);
    }
};

struct DecodeBits {
    DecodeCursor& input;
    std::uint8_t current{};
    unsigned used{8};
    bool bits(unsigned count, std::uint64_t& out) {
        if (count > 64u) return false;
        out = 0;
        for (unsigned i = 0; i < count; ++i) {
            if (used == 8u) { if (!input.byte(current)) return false; used = 0; }
            out |= static_cast<std::uint64_t>((current >> used++) & 1u) << i;
        }
        return true;
    }
    bool finished() const noexcept { return input.remaining() == 0u && (used == 8u || (current >> used) == 0u); }
};

// A block may end inside a JSONL line; consumers already accept byte blocks.
class DecodeOutput {
    const DecodedBlockCallback& callback_;
    std::unique_ptr<std::array<std::uint8_t, 65536>> bytes_;
    std::size_t used_{};
public:
    std::uint64_t produced{};
    Status status{Status::Ok};
    explicit DecodeOutput(const DecodedBlockCallback& callback)
        : callback_(callback), bytes_(std::make_unique<std::array<std::uint8_t, 65536>>()) {}
    bool flush() {
        if (used_ == 0u) return true;
        if (!callback_({bytes_->data(), used_})) { status = Status::CallbackStopped; return false; }
        used_ = 0; return true;
    }
    bool append(std::string_view text) {
        if (text.size() > std::numeric_limits<std::uint64_t>::max() - produced) { status = Status::CorruptData; return false; }
        produced += text.size();
        while (!text.empty()) {
            const auto count = std::min(text.size(), bytes_->size() - used_);
            std::copy_n(reinterpret_cast<const std::uint8_t*>(text.data()), count, bytes_->data() + used_);
            used_ += count; text.remove_prefix(count);
            if (used_ == bytes_->size() && !flush()) return false;
        }
        return true;
    }
};

inline bool addI64(std::int64_t lhs, std::int64_t rhs, std::int64_t& out) noexcept {
    return !__builtin_add_overflow(lhs, rhs, &out);
}
inline bool multiplyI64(std::int64_t lhs, std::int64_t rhs, std::int64_t& out) noexcept {
    return !__builtin_mul_overflow(lhs, rhs, &out);
}
inline bool subtractI64(std::int64_t lhs, std::int64_t rhs, std::int64_t& out) noexcept {
    return !__builtin_sub_overflow(lhs, rhs, &out);
}
inline std::int64_t decodeZigzag(std::uint64_t value) noexcept {
    return static_cast<std::int64_t>(value >> 1u) ^ -static_cast<std::int64_t>(value & 1u);
}

} // namespace hft_compressor::internal

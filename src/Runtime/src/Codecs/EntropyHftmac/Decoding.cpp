#include "EntropyHftMacInternal.hpp"
#include "../BaseDecode.hpp"

namespace hft_compressor::codecs::entropy_hftmac::detail {

Status readHeader(const internal::DecodeSource& file, Header& header) {
    std::array<std::uint8_t, kHeaderBytes> bytes{};
    if (file.size() < kHeaderBytes || !file.read(0u, bytes)
        || !parseHeader(bytes.data(), bytes.size(), header) || !validHeader(header)
        || file.size() != header.outputBytes) return Status::CorruptData;
    return Status::Ok;
}

namespace {

class BaseSource final : public internal::DecodeSource {
    const internal::DecodeSource& encoded_;
    const Header& header_;
    struct State {
        std::unique_ptr<internal::DecodeCursor> decoded;
        std::uint64_t position{};
    };
    // Native codecs have at most ten simultaneous column/table/CRC cursors.
    // Reuse forward entropy positions across Trades chunks; never retain base
    // bytes or create one cached state per chunk/file offset.
    mutable std::array<State, 16> idle_{};
    void recycle(State state) const noexcept {
        auto* slot = &idle_.front();
        for (auto& candidate : idle_) {
            if (!candidate.decoded) { slot = &candidate; break; }
            if (candidate.position < slot->position) slot = &candidate;
        }
        *slot = std::move(state);
    }
    class Cursor final : public internal::DecodeCursor {
        const BaseSource& owner_;
        State state_;
        std::uint64_t remaining_;
    public:
        Cursor(const BaseSource& owner, State state, std::uint64_t count)
            : owner_(owner), state_(std::move(state)), remaining_(count) {}
        ~Cursor() override { owner_.recycle(std::move(state_)); }
        std::uint64_t remaining() const noexcept override { return remaining_; }
        bool byte(std::uint8_t& out) override {
            if (!remaining_ || !state_.decoded->byte(out)) return false;
            ++state_.position; --remaining_; return true;
        }
    };
public:
    BaseSource(const internal::DecodeSource& encoded, const Header& header) : encoded_(encoded), header_(header) {}
    void reset() noexcept { for (auto& state : idle_) state = {}; }
    std::uint64_t size() const noexcept override { return header_.baseBytes; }
    std::unique_ptr<internal::DecodeCursor> cursor(std::uint64_t offset, std::uint64_t count) const override {
        if (!contains(offset, count)) return {};
        State* best{};
        for (auto& candidate : idle_) {
            if (candidate.decoded && candidate.position <= offset
                && (!best || candidate.position > best->position)) best = &candidate;
        }
        State state = best ? std::move(*best) : State{arithmeticCursor(encoded_, header_), 0u};
        if (!state.decoded || !state.decoded->skip(offset - state.position)) return {};
        state.position = offset;
        return std::make_unique<Cursor>(*this, std::move(state), count);
    }
};

Status decodeBase(BaseKind base, const internal::DecodeSource& source, const DecodedBlockCallback& onBlock) {
    switch (base) {
        case BaseKind::Trades: return trades_grouped_delta_qtydict::decodeSource(source, onBlock);
        case BaseKind::BookTicker: return bookticker_delta_mask::decodeSource(source, onBlock);
        case BaseKind::Depth: return depth_ladder_offset::decodeSource(source, onBlock);
    }
    return Status::CorruptData;
}

} // namespace

Status decodeSource(const internal::DecodeSource& file, const DecodedBlockCallback& onBlock) {
    if (!onBlock) return Status::InvalidArgument;
    Header header{};
    auto status = verifyPayload(file, header);
    if (!isOk(status)) return status;
    BaseSource base(file, header);
    std::uint64_t produced{}, lines{};
    bool invalidOutput = false;
    const DecodedBlockCallback validate = [&](std::span<const std::uint8_t> bytes) {
        if (produced > header.inputBytes || bytes.size() > header.inputBytes - produced) {
            invalidOutput = true; return false;
        }
        produced += bytes.size();
        for (auto byte : bytes) if (byte == '\n') {
            if (lines == header.lineCount) { invalidOutput = true; return false; }
            ++lines;
        }
        return true;
    };
    // Validate both container and complete native column/chunk layout before
    // publishing a single byte. Replay is deliberate: no decoded spool exists.
    status = decodeBase(static_cast<BaseKind>(header.base), base, validate);
    if (invalidOutput || (!isOk(status) && status == Status::CallbackStopped)) return Status::CorruptData;
    if (!isOk(status)) return status;
    if (produced != header.inputBytes || lines != header.lineCount || !file.unchanged()) return Status::CorruptData;
    base.reset();
    status = decodeBase(static_cast<BaseKind>(header.base), base, onBlock);
    return !file.unchanged() ? Status::CorruptData : status;
}

} // namespace hft_compressor::codecs::entropy_hftmac::detail

namespace hft_compressor::codecs::entropy_hftmac {

Status decode(std::span<const std::uint8_t> file, const DecodedBlockCallback& onBlock) noexcept {
    try {
        internal::SpanDecodeSource source(file);
        return detail::decodeSource(source, onBlock);
    } catch (...) { return Status::DecodeError; }
}

Status decodeFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept {
    if (path.empty() || !onBlock) return Status::InvalidArgument;
    try {
        internal::FileDecodeSource source(path);
        if (!source.valid()) return Status::IoError;
        return detail::decodeSource(source, onBlock);
    } catch (...) { return Status::DecodeError; }
}

} // namespace hft_compressor::codecs::entropy_hftmac

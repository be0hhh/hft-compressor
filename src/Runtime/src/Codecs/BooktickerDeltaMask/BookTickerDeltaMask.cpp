#include "BookTickerDeltaMask.hpp"
#include "../BaseDecode.hpp"

#include <algorithm>
#include <charconv>
#include <fstream>
#include <numeric>
#include <sstream>
#include <string_view>
#include <system_error>
#include <vector>

#include "../../Common/CompressionInternals.hpp"
#include "../../Common/Timing.hpp"
#include "../../Container/Hfc/Format.hpp"

namespace hft_compressor::codecs::bookticker_delta_mask {
namespace {

constexpr std::uint32_t kMagic = 0x4b544243u; // B T C K little-endian wire
constexpr std::uint16_t kCurrentArtifactVersion = 2u;
constexpr std::size_t kHeaderBytes = 160u;

struct Row { std::int64_t bid{0}, bidQty{0}, ask{0}, askQty{0}, ts{0}; };
struct Header {
    std::uint32_t magic{kMagic};
    std::uint16_t version{kCurrentArtifactVersion};
    std::uint16_t reserved{0};
    std::uint64_t inputBytes{0};
    std::uint64_t outputBytes{0};
    std::uint64_t recordCount{0};
    std::int64_t timeScale{1}, priceScale{1}, qtyScale{1};
    std::int64_t baseTsUnit{0}, baseBidTick{0}, baseSpreadTick{0}, baseBidQtyLot{0}, baseAskQtyLot{0};
    std::uint32_t timeBytes{0}, maskBytes{0}, bidBytes{0}, spreadBytes{0}, bidQtyBytes{0}, askQtyBytes{0};
    std::uint32_t maskNonZero{0}, bidChanged{0}, spreadChanged{0}, bidQtyChanged{0}, askQtyChanged{0};
};

struct Cursor {
    std::string_view text{}; std::size_t pos{0};
    void ws() noexcept { while (pos < text.size() && (text[pos] == ' ' || text[pos] == '\t' || text[pos] == '\r')) ++pos; }
    bool ch(char c) noexcept { ws(); if (pos >= text.size() || text[pos] != c) return false; ++pos; return true; }
    bool i64(std::int64_t& out) noexcept {
        ws();
        if (pos >= text.size()) return false;
        const std::size_t beginPos = pos;
        if (text[pos] == '-') { ++pos; if (pos >= text.size()) return false; }
        if (text[pos] == '0') {
            ++pos;
            if (pos < text.size() && text[pos] >= '0' && text[pos] <= '9') return false;
        } else {
            if (text[pos] < '1' || text[pos] > '9') return false;
            do { ++pos; } while (pos < text.size() && text[pos] >= '0' && text[pos] <= '9');
        }
        const char* b = text.data() + beginPos;
        const char* e = text.data() + pos;
        const auto [p, ec] = std::from_chars(b, e, out);
        return ec == std::errc{} && p == e;
    }
    bool end() noexcept { ws(); return pos == text.size(); }
};

bool parseLine(std::string_view line, Row& out) noexcept {
    Cursor p{line};
    return p.ch('[') && p.i64(out.bid) && p.ch(',') && p.i64(out.bidQty) && p.ch(',') && p.i64(out.ask) && p.ch(',') && p.i64(out.askQty) && p.ch(',') && p.i64(out.ts) && p.ch(']') && p.end();
}

bool parseRows(std::span<const std::uint8_t> input, std::vector<Row>& rows) {
    if (input.empty()) return false;
    std::int64_t prevTs = 0;
    bool have = false;
    std::size_t lineStart = 0;
    while (lineStart < input.size()) {
        std::size_t lineEnd = lineStart;
        while (lineEnd < input.size() && input[lineEnd] != static_cast<std::uint8_t>('\n')) ++lineEnd;
        std::string_view line{reinterpret_cast<const char*>(input.data() + lineStart), lineEnd - lineStart};
        if (!line.empty() && line.back() == '\r') line.remove_suffix(1);
        if (line.empty()) return false;
        Row row{};
        if (!parseLine(line, row)) return false;
        if (have && row.ts < prevTs) return false;
        prevTs = row.ts;
        have = true;
        rows.push_back(row);
        lineStart = lineEnd + (lineEnd < input.size() ? 1u : 0u);
    }
    return !rows.empty();
}

std::int64_t gcdAbs(std::int64_t a, std::int64_t b) noexcept { a = a < 0 ? -a : a; b = b < 0 ? -b : b; return std::gcd(a, b); }
std::int64_t safeScale(std::int64_t v) noexcept { return v == 0 ? 1 : (v < 0 ? -v : v); }
std::uint64_t zz(std::int64_t v) noexcept { return v < 0 ? (static_cast<std::uint64_t>(-v) * 2u - 1u) : static_cast<std::uint64_t>(v) * 2u; }

void varint(std::vector<std::uint8_t>& out, std::uint64_t v) { while (v >= 0x80u) { out.push_back(static_cast<std::uint8_t>(v | 0x80u)); v >>= 7u; } out.push_back(static_cast<std::uint8_t>(v)); }

template <class T> void le(std::vector<std::uint8_t>& out, T v) { using U = std::make_unsigned_t<T>; U u = static_cast<U>(v); for (std::size_t i = 0; i < sizeof(T); ++i) out.push_back(static_cast<std::uint8_t>((u >> (i * 8u)) & 0xffu)); }
template <class T> bool rd(const std::uint8_t*& p, const std::uint8_t* e, T& out) noexcept { if (static_cast<std::size_t>(e - p) < sizeof(T)) return false; using U = std::make_unsigned_t<T>; U u = 0; for (std::size_t i = 0; i < sizeof(T); ++i) u |= static_cast<U>(*p++) << (i * 8u); out = static_cast<T>(u); return true; }

std::vector<std::uint8_t> headerBytes(const Header& h) {
    std::vector<std::uint8_t> out; out.reserve(kHeaderBytes);
    le(out,h.magic); le(out,h.version); le(out,h.reserved); le(out,h.inputBytes); le(out,h.outputBytes); le(out,h.recordCount);
    le(out,h.timeScale); le(out,h.priceScale); le(out,h.qtyScale); le(out,h.baseTsUnit); le(out,h.baseBidTick); le(out,h.baseSpreadTick); le(out,h.baseBidQtyLot); le(out,h.baseAskQtyLot);
    le(out,h.timeBytes); le(out,h.maskBytes); le(out,h.bidBytes); le(out,h.spreadBytes); le(out,h.bidQtyBytes); le(out,h.askQtyBytes);
    le(out,h.maskNonZero); le(out,h.bidChanged); le(out,h.spreadChanged); le(out,h.bidQtyChanged); le(out,h.askQtyChanged);
    out.resize(kHeaderBytes, 0); return out;
}

bool readHeader(std::span<const std::uint8_t> data, Header& h) noexcept {
    if (data.size() < kHeaderBytes) return false; const auto* p = data.data(); const auto* e = data.data() + kHeaderBytes;
    return rd(p,e,h.magic) && rd(p,e,h.version) && rd(p,e,h.reserved) && rd(p,e,h.inputBytes) && rd(p,e,h.outputBytes) && rd(p,e,h.recordCount)
        && rd(p,e,h.timeScale) && rd(p,e,h.priceScale) && rd(p,e,h.qtyScale) && rd(p,e,h.baseTsUnit) && rd(p,e,h.baseBidTick) && rd(p,e,h.baseSpreadTick) && rd(p,e,h.baseBidQtyLot) && rd(p,e,h.baseAskQtyLot)
        && rd(p,e,h.timeBytes) && rd(p,e,h.maskBytes) && rd(p,e,h.bidBytes) && rd(p,e,h.spreadBytes) && rd(p,e,h.bidQtyBytes) && rd(p,e,h.askQtyBytes)
        && rd(p,e,h.maskNonZero) && rd(p,e,h.bidChanged) && rd(p,e,h.spreadChanged) && rd(p,e,h.bidQtyChanged) && rd(p,e,h.askQtyChanged)
        && h.magic == kMagic && h.version == kCurrentArtifactVersion && h.timeScale != 0 && h.priceScale != 0 && h.qtyScale != 0
        && std::all_of(p, e, [](auto byte) { return byte == 0u; });
}

void pushPackedMask(std::vector<std::uint8_t>& out, std::uint8_t mask, bool& highNibble) {
    mask &= 0x0fu;
    if (!highNibble) {
        out.push_back(mask);
        highNibble = true;
    } else {
        out.back() = static_cast<std::uint8_t>(out.back() | static_cast<std::uint8_t>(mask << 4u));
        highNibble = false;
    }
}

Status decodeColumns(const internal::DecodeSource& source, const DecodedBlockCallback* jsonl, std::ostream* encoded) {
    std::array<std::uint8_t, kHeaderBytes> bytes{};
    Header h{};
    if (!source.read(0u, bytes) || !readHeader(bytes, h) || h.outputBytes != source.size()
        || h.reserved != 0u || h.timeScale <= 0 || h.priceScale <= 0 || h.qtyScale <= 0 || h.recordCount == 0u) return Status::CorruptData;
    const auto updates = h.recordCount - 1u;
    if (updates > h.timeBytes || h.maskBytes != updates / 2u + updates % 2u) return Status::CorruptData;
    std::uint64_t offset = kHeaderBytes;
    auto next = [&](std::uint32_t count) {
        auto result = source.cursor(offset, count);
        if (source.contains(offset, count)) offset += count;
        return result;
    };
    auto time = next(h.timeBytes), mask = next(h.maskBytes), bidInput = next(h.bidBytes), spreadInput = next(h.spreadBytes);
    auto bidQty = next(h.bidQtyBytes), askQty = next(h.askQtyBytes);
    if (!time || !mask || !bidInput || !spreadInput || !bidQty || !askQty || offset != source.size()) return Status::CorruptData;
    const DecodedBlockCallback ignore = [](auto) { return true; };
    internal::DecodeOutput output(jsonl ? *jsonl : ignore);
    std::int64_t ts=h.baseTsUnit, bid=h.baseBidTick, spread=h.baseSpreadTick, bq=h.baseBidQtyLot, aq=h.baseAskQtyLot;
    std::uint8_t packedMask{};
    bool highNibble = false;
    std::uint64_t masksNonzero{}, bidsChanged{}, spreadsChanged{}, bidsQtyChanged{}, asksQtyChanged{};
    std::int64_t baseTime{}, baseBid{}, baseSpread{}, baseBq{}, baseAq{};
    if (!internal::multiplyI64(ts, h.timeScale, baseTime) || !internal::multiplyI64(bid, h.priceScale, baseBid)
        || !internal::multiplyI64(spread, h.priceScale, baseSpread) || !internal::multiplyI64(bq, h.qtyScale, baseBq)
        || !internal::multiplyI64(aq, h.qtyScale, baseAq)) return Status::CorruptData;
    if (encoded) *encoded << "{\n  \"pipeline_id\": \"hftmac.bookticker_delta_mask_v2\",\n  \"record_count\": " << h.recordCount << ",\n  \"base_state\": {\"ts\": " << baseTime << ", \"bid\": " << baseBid << ", \"spread\": " << baseSpread << ", \"bid_qty\": " << baseBq << ", \"ask_qty\": " << baseAq << "},\n  \"updates\": [\n";
    for (std::uint64_t i=0; i<h.recordCount; ++i) {
        std::uint8_t m{};
        std::uint64_t dt{};
        std::int64_t dbid{}, dspread{}, dbq{}, daq{};
        if (i != 0u) {
            if (!time->varint(dt) || dt > static_cast<std::uint64_t>(std::numeric_limits<std::int64_t>::max())
                || !internal::addI64(ts, static_cast<std::int64_t>(dt), ts)) return Status::CorruptData;
            if (!highNibble && !mask->byte(packedMask)) return Status::CorruptData;
            m = highNibble ? packedMask >> 4u : packedMask & 15u;
            highNibble = !highNibble;
            if (m) ++masksNonzero;
            auto delta = [](internal::DecodeCursor& input, std::int64_t& change, std::int64_t& state) {
                std::uint64_t raw{};
                if (!input.varint(raw)) return false;
                change = internal::decodeZigzag(raw);
                return internal::addI64(state, change, state);
            };
            if (m & 1u) { if (!delta(*bidInput, dbid, bid)) return Status::CorruptData; ++bidsChanged; }
            if (m & 2u) { if (!delta(*spreadInput, dspread, spread)) return Status::CorruptData; ++spreadsChanged; }
            if (m & 4u) { if (!delta(*bidQty, dbq, bq)) return Status::CorruptData; ++bidsQtyChanged; }
            if (m & 8u) { if (!delta(*askQty, daq, aq)) return Status::CorruptData; ++asksQtyChanged; }
        }
        std::int64_t bidValue{}, askTick{}, askValue{}, bqValue{}, aqValue{}, timeValue{};
        if (!internal::addI64(bid, spread, askTick) || !internal::multiplyI64(bid, h.priceScale, bidValue)
            || !internal::multiplyI64(askTick, h.priceScale, askValue) || !internal::multiplyI64(bq, h.qtyScale, bqValue)
            || !internal::multiplyI64(aq, h.qtyScale, aqValue) || !internal::multiplyI64(ts, h.timeScale, timeValue)) return Status::CorruptData;
        if (jsonl) {
            const auto row = "[" + std::to_string(bidValue) + "," + std::to_string(bqValue) + "," + std::to_string(askValue)
                + "," + std::to_string(aqValue) + "," + std::to_string(timeValue) + "]\n";
            if (!output.append(row)) return output.status;
        }
        if (encoded) {
            *encoded << (i ? ",\n" : "") << "    {\"dt\": " << dt << ", \"mask\": " << static_cast<unsigned>(m)
                << ", \"d\": {\"bid\": " << dbid << ", \"spread\": " << dspread << ", \"bid_qty\": " << dbq << ", \"ask_qty\": " << daq << "}"
                << ", \"state\": [" << bidValue << "," << bqValue << "," << askValue << "," << aqValue << "," << timeValue << "]}";
        }
    }
    if (time->remaining() || mask->remaining() || (highNibble && (packedMask >> 4u) != 0u)
        || bidInput->remaining() || spreadInput->remaining() || bidQty->remaining() || askQty->remaining()
        || masksNonzero != h.maskNonZero || bidsChanged != h.bidChanged || spreadsChanged != h.spreadChanged
        || bidsQtyChanged != h.bidQtyChanged || asksQtyChanged != h.askQtyChanged
        || (jsonl && output.produced != h.inputBytes)) return Status::CorruptData;
    if (jsonl && !output.flush()) return output.status;
    if (encoded) *encoded << "\n  ]\n}\n";
    return Status::Ok;
}

Status decodeBytes(std::span<const std::uint8_t> data, std::string* jsonl, std::ostream* encoded) noexcept {
    try {
        internal::SpanDecodeSource source(data);
        const DecodedBlockCallback append = [&](auto bytes) { jsonl->append(reinterpret_cast<const char*>(bytes.data()), bytes.size()); return true; };
        return decodeColumns(source, jsonl ? &append : nullptr, encoded);
    } catch (...) { return Status::DecodeError; }
}

std::string statsJson(const Header& h) {
    std::ostringstream o; o << "{\n  \"pipeline_id\": \"hftmac.bookticker_delta_mask_v2\",\n  \"version\": " << h.version << ",\n  \"record_count\": " << h.recordCount << ",\n  \"raw_runtime_bytes\": " << (h.recordCount*40u) << ",\n  \"encoded_bytes\": " << h.outputBytes << ",\n  \"bytes_per_record\": " << (h.recordCount? static_cast<double>(h.outputBytes)/static_cast<double>(h.recordCount):0.0) << ",\n  \"mask_nonzero_count\": " << h.maskNonZero << ",\n  \"bid_changed_count\": " << h.bidChanged << ",\n  \"spread_changed_count\": " << h.spreadChanged << ",\n  \"bid_qty_changed_count\": " << h.bidQtyChanged << ",\n  \"ask_qty_changed_count\": " << h.askQtyChanged << ",\n  \"time_stream_bytes\": " << h.timeBytes << ",\n  \"mask_stream_bytes\": " << h.maskBytes << ",\n  \"bid_delta_stream_bytes\": " << h.bidBytes << ",\n  \"spread_delta_stream_bytes\": " << h.spreadBytes << ",\n  \"bid_qty_delta_stream_bytes\": " << h.bidQtyBytes << ",\n  \"ask_qty_delta_stream_bytes\": " << h.askQtyBytes << "\n}\n"; return o.str();
}

bool readFile(const std::filesystem::path& p, std::vector<std::uint8_t>& out) { return internal::readFileBytes(p,out); }
Status emitText(const std::string& s, const DecodedBlockCallback& cb) { return cb(std::span<const std::uint8_t>{reinterpret_cast<const std::uint8_t*>(s.data()), s.size()}) ? Status::Ok : Status::CallbackStopped; }

} // namespace

CompressionResult compress(const CompressionRequest& request, const PipelineDescriptor& pipeline) noexcept try {
    if (request.inputPath.empty()) { auto r=internal::fail(Status::InvalidArgument,request,&pipeline,"input path is empty");  return r; }
    if (inferStreamTypeFromPath(request.inputPath) != StreamType::BookTicker) { auto r=internal::fail(Status::UnsupportedStream,request,&pipeline,"expected bookticker.jsonl");  return r; }
    CompressionResult r{}; internal::applyPipeline(r,&pipeline); r.streamType=StreamType::BookTicker; r.inputPath=request.inputPath; const auto total=timing::nowNs();
    std::vector<std::uint8_t> input; const auto rs=timing::nowNs(); if (!internal::readFileBytes(request.inputPath,input)) { auto f=internal::fail(Status::IoError,request,&pipeline,"failed to read input file");  return f; } r.readNs=timing::nowNs()-rs; r.inputBytes=input.size();
    std::vector<Row> rows; const auto ps=timing::nowNs(); rows.reserve(std::count(input.begin(),input.end(),static_cast<std::uint8_t>('\n'))+1u); if (!parseRows(input,rows)) { auto f=internal::fail(Status::CorruptData,request,&pipeline,"input is not canonical bookticker jsonl");  return f; } r.parseNs=timing::nowNs()-ps;
    Header h{}; h.version = kCurrentArtifactVersion; h.inputBytes=r.inputBytes; h.recordCount=rows.size(); std::int64_t timeG=rows.front().ts, priceG=0, qtyG=0; for (const auto& x: rows) { timeG=gcdAbs(timeG,x.ts-rows.front().ts); priceG=gcdAbs(priceG,x.bid); priceG=gcdAbs(priceG,x.ask); qtyG=gcdAbs(qtyG,x.bidQty); qtyG=gcdAbs(qtyG,x.askQty); } h.timeScale=safeScale(timeG); h.priceScale=safeScale(priceG); h.qtyScale=safeScale(qtyG);
    h.baseTsUnit=rows.front().ts/h.timeScale; h.baseBidTick=rows.front().bid/h.priceScale; h.baseSpreadTick=(rows.front().ask-rows.front().bid)/h.priceScale; h.baseBidQtyLot=rows.front().bidQty/h.qtyScale; h.baseAskQtyLot=rows.front().askQty/h.qtyScale;
    std::vector<std::uint8_t> timeS, maskS, bidS, spreadS, bidQtyS, askQtyS; const auto es=timing::nowNs(); const auto ec0=timing::readCycles();
    std::int64_t pts=h.baseTsUnit, pb=h.baseBidTick, pspr=h.baseSpreadTick, pbq=h.baseBidQtyLot, paq=h.baseAskQtyLot;
    bool maskHighNibble = false;
    for (std::size_t i=1;i<rows.size();++i) { const auto ts=rows[i].ts/h.timeScale, b=rows[i].bid/h.priceScale, spr=(rows[i].ask-rows[i].bid)/h.priceScale, bq=rows[i].bidQty/h.qtyScale, aq=rows[i].askQty/h.qtyScale; varint(timeS, static_cast<std::uint64_t>(ts-pts)); std::uint8_t m=0; if (b!=pb) m|=1u; if (spr!=pspr) m|=2u; if (bq!=pbq) m|=4u; if (aq!=paq) m|=8u; pushPackedMask(maskS,m,maskHighNibble); if (m) ++h.maskNonZero; if (m&1u){varint(bidS,zz(b-pb));++h.bidChanged;} if(m&2u){varint(spreadS,zz(spr-pspr));++h.spreadChanged;} if(m&4u){varint(bidQtyS,zz(bq-pbq));++h.bidQtyChanged;} if(m&8u){varint(askQtyS,zz(aq-paq));++h.askQtyChanged;} pts=ts; pb=b; pspr=spr; pbq=bq; paq=aq; }
    r.encodeCycles=timing::readCycles()-ec0; r.encodeCoreNs=timing::nowNs()-es; h.timeBytes=timeS.size(); h.maskBytes=maskS.size(); h.bidBytes=bidS.size(); h.spreadBytes=spreadS.size(); h.bidQtyBytes=bidQtyS.size(); h.askQtyBytes=askQtyS.size();
    const auto outPath=internal::outputPathFor(request,pipeline,StreamType::BookTicker); std::error_code dirEc; std::filesystem::create_directories(outPath.parent_path(),dirEc); if(dirEc){ auto f=internal::fail(Status::IoError,request,&pipeline,"failed to create output directory");  return f; }
    h.outputBytes=kHeaderBytes+h.timeBytes+h.maskBytes+h.bidBytes+h.spreadBytes+h.bidQtyBytes+h.askQtyBytes; r.outputPath=outPath; r.metricsPath=outPath.parent_path()/(outPath.stem().string()+".metrics.json"); const auto ws=timing::nowNs(); std::ofstream out(outPath,std::ios::binary|std::ios::trunc); auto hb=headerBytes(h); out.write(reinterpret_cast<const char*>(hb.data()),hb.size()); for(auto* s:{&timeS,&maskS,&bidS,&spreadS,&bidQtyS,&askQtyS}) out.write(reinterpret_cast<const char*>(s->data()),static_cast<std::streamsize>(s->size())); out.close(); r.writeNs=timing::nowNs()-ws; r.encodeNs=timing::nowNs()-total; r.outputBytes=h.outputBytes; r.lineCount=h.recordCount; r.blockCount=1;
    std::string decoded; const auto ds=timing::nowNs(); const auto dc=timing::readCycles(); std::vector<std::uint8_t> file; readFile(outPath,file); const auto st=decodeBytes(file,&decoded,nullptr); r.decodeCycles=timing::readCycles()-dc; r.decodeNs=timing::nowNs()-ds; r.decodeCoreNs=r.decodeNs; r.roundtripOk=isOk(st)&&decoded.size()==input.size()&&std::equal(decoded.begin(),decoded.end(),reinterpret_cast<const char*>(input.data())); r.status=r.roundtripOk?Status::Ok:Status::DecodeError; if(!r.roundtripOk) r.error="roundtrip check failed"; (void)internal::writeTextFile(r.metricsPath,toMetricsJson(r));  return r;
} catch (...) { CompressionResult failed{}; failed.status = Status::DecodeError; return failed; }

ReplayArtifactInfo inspectArtifact(const std::filesystem::path& path, const PipelineDescriptor& pipeline) noexcept try { internal::FileDecodeSource source(path); std::array<std::uint8_t, kHeaderBytes> data{}; ReplayArtifactInfo i{}; i.path=path; if(!source.valid() || !source.read(0u, data)){i.status=Status::IoError;i.error="failed to read artifact";return i;} Header h{}; if(!readHeader(data,h) || h.outputBytes != source.size() || !source.unchanged()){i.status=Status::CorruptData;i.error="invalid bookticker artifact";return i;} i.status=Status::Ok; i.found=true; i.formatId="hftmac.bookticker_delta_mask.v2"; i.pipelineId=std::string{pipeline.id}; i.transform=std::string{pipeline.transform}; i.entropy=std::string{pipeline.entropy}; i.streamType=StreamType::BookTicker; i.version=h.version; i.inputBytes=h.inputBytes; i.outputBytes=h.outputBytes; i.lineCount=h.recordCount; i.blockCount=1; return i; } catch (...) { ReplayArtifactInfo failed{}; failed.status = Status::DecodeError; return failed; }
Status decodeSource(const internal::DecodeSource& source, const DecodedBlockCallback& onBlock) {
    if (!onBlock) return Status::InvalidArgument;
    return decodeColumns(source, &onBlock, nullptr);
}
Status decode(std::span<const std::uint8_t> bytes, const DecodedBlockCallback& onBlock) noexcept {
    if (!onBlock) return Status::InvalidArgument;
    try {
        internal::SpanDecodeSource source(bytes);
        const DecodedBlockCallback validate = [](auto) { return true; };
        auto status = decodeSource(source, validate);
        if (!isOk(status)) return status;
        if (!source.unchanged()) return Status::CorruptData;
        status = decodeSource(source, onBlock);
        return !source.unchanged() ? Status::CorruptData : status;
    } catch (...) { return Status::DecodeError; }
}
Status decodeFile(const std::filesystem::path& path, const DecodedBlockCallback& onBlock) noexcept {
    if (path.empty() || !onBlock) return Status::InvalidArgument;
    try {
        internal::FileDecodeSource source(path);
        if (!source.valid()) return Status::IoError;
        const DecodedBlockCallback validate = [](auto) { return true; };
        auto status = decodeSource(source, validate);
        if (!isOk(status)) return status;
        if (!source.unchanged()) return Status::CorruptData;
        status = decodeSource(source, onBlock);
        return !source.unchanged() ? Status::CorruptData : status;
    } catch (...) { return Status::DecodeError; }
}
Status inspectEncodedJsonFile(const std::filesystem::path& path, const DecodedBlockCallback& cb) noexcept try { std::vector<std::uint8_t> data; if(!readFile(path,data)) return Status::IoError; std::ostringstream out; const auto st=decodeBytes(data,nullptr,&out); if(!isOk(st)) return st; return emitText(out.str(),cb); } catch (...) { return Status::DecodeError; }
Status inspectEncodedBinaryFile(const std::filesystem::path& path, const DecodedBlockCallback& cb) noexcept try { internal::FileDecodeSource source(path); std::array<std::uint8_t, kHeaderBytes> data{}; if(!source.valid() || !source.read(0u, data)) return Status::IoError; std::ostringstream o; Header h{}; if(!readHeader(data,h) || h.outputBytes != source.size() || !source.unchanged()) return Status::CorruptData; o << "bookticker_delta_mask_v2" << " bytes=" << source.size() << " header=" << kHeaderBytes << " time=" << h.timeBytes << " mask=" << h.maskBytes << " bid=" << h.bidBytes << " spread=" << h.spreadBytes << " bid_qty=" << h.bidQtyBytes << " ask_qty=" << h.askQtyBytes << "\n"; return emitText(o.str(),cb); } catch (...) { return Status::DecodeError; }
Status inspectStatsJsonFile(const std::filesystem::path& path, const DecodedBlockCallback& cb) noexcept try { internal::FileDecodeSource source(path); std::array<std::uint8_t, kHeaderBytes> data{}; if(!source.valid() || !source.read(0u, data)) return Status::IoError; Header h{}; if(!readHeader(data,h) || h.outputBytes != source.size() || !source.unchanged()) return Status::CorruptData; return emitText(statsJson(h),cb); } catch (...) { return Status::DecodeError; }

}  // namespace hft_compressor::codecs::bookticker_delta_mask

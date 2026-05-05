#include "codecs/bookticker_delta_mask/BookTickerDeltaMask.hpp"

#include <algorithm>
#include <charconv>
#include <fstream>
#include <numeric>
#include <sstream>
#include <string_view>
#include <system_error>
#include <vector>

#include <simdjson.h>

#include "common/CompressionInternals.hpp"
#include "common/timing.hpp"
#include "container/hfc/format.hpp"
#include "hft_compressor/metrics.hpp"

namespace hft_compressor::codecs::bookticker_delta_mask {
namespace {

constexpr std::uint32_t kMagic = 0x4b544243u; // B T C K little-endian wire
constexpr std::uint16_t kVersionV1 = 1u;
constexpr std::uint16_t kVersionV2 = 2u;
constexpr std::size_t kHeaderBytes = 160u;

bool isSimdjsonEmpty(simdjson::error_code error) noexcept {
    return error == simdjson::EMPTY || static_cast<int>(error) == 12;
}

struct Row { std::int64_t bid{0}, bidQty{0}, ask{0}, askQty{0}, ts{0}; };
struct Header {
    std::uint32_t magic{kMagic};
    std::uint16_t version{kVersionV1};
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
    bool i64(std::int64_t& out) noexcept { ws(); const char* b = text.data() + pos; const char* e = text.data() + text.size(); const auto [p, ec] = std::from_chars(b, e, out); if (ec != std::errc{} || p == b) return false; pos = static_cast<std::size_t>(p - text.data()); return true; }
    bool end() noexcept { ws(); return pos == text.size(); }
};

bool parseLine(std::string_view line, Row& out) noexcept {
    Cursor p{line};
    return p.ch('[') && p.i64(out.bid) && p.ch(',') && p.i64(out.bidQty) && p.ch(',') && p.i64(out.ask) && p.ch(',') && p.i64(out.askQty) && p.ch(',') && p.i64(out.ts) && p.ch(']') && p.end();
}

bool parseRows(std::span<const std::uint8_t> input, std::vector<Row>& rows) {
    simdjson::dom::parser parser;
    simdjson::padded_string padded{reinterpret_cast<const char*>(input.data()), input.size()};
    auto docs = parser.parse_many(padded.data(), padded.size(), padded.size());
    std::int64_t prevTs = 0;
    bool have = false;
    for (auto docResult : docs) {
        simdjson::dom::element doc;
        const auto docError = docResult.get(doc);
        if (isSimdjsonEmpty(docError)) continue;
        if (docError != simdjson::SUCCESS || !doc.is_array()) return false;
        simdjson::dom::array values;
        if (doc.get_array().get(values) != simdjson::SUCCESS || values.size() != 5u) return false;
        Row row{};
        std::size_t index = 0;
        for (auto value : values) {
            std::int64_t parsed = 0;
            if (value.get_int64().get(parsed) != simdjson::SUCCESS) return false;
            if (index == 0u) row.bid = parsed;
            else if (index == 1u) row.bidQty = parsed;
            else if (index == 2u) row.ask = parsed;
            else if (index == 3u) row.askQty = parsed;
            else row.ts = parsed;
            ++index;
        }
        if (have && row.ts < prevTs) return false;
        prevTs = row.ts;
        have = true;
        rows.push_back(row);
    }
    return !rows.empty();
}

std::int64_t gcdAbs(std::int64_t a, std::int64_t b) noexcept { a = a < 0 ? -a : a; b = b < 0 ? -b : b; return std::gcd(a, b); }
std::int64_t safeScale(std::int64_t v) noexcept { return v == 0 ? 1 : (v < 0 ? -v : v); }
std::uint64_t zz(std::int64_t v) noexcept { return v < 0 ? (static_cast<std::uint64_t>(-v) * 2u - 1u) : static_cast<std::uint64_t>(v) * 2u; }
std::int64_t unzz(std::uint64_t v) noexcept { return (v & 1u) ? -static_cast<std::int64_t>((v + 1u) / 2u) : static_cast<std::int64_t>(v / 2u); }

void varint(std::vector<std::uint8_t>& out, std::uint64_t v) { while (v >= 0x80u) { out.push_back(static_cast<std::uint8_t>(v | 0x80u)); v >>= 7u; } out.push_back(static_cast<std::uint8_t>(v)); }
bool readVar(const std::uint8_t*& p, const std::uint8_t* e, std::uint64_t& out) noexcept { out = 0; unsigned s = 0; while (p < e && s <= 63u) { const auto b = *p++; out |= static_cast<std::uint64_t>(b & 0x7fu) << s; if ((b & 0x80u) == 0u) return true; s += 7u; } return false; }

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
        && h.magic == kMagic && (h.version == kVersionV1 || h.version == kVersionV2) && h.timeScale != 0 && h.priceScale != 0 && h.qtyScale != 0;
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

struct MaskReader {
    const std::uint8_t* p{};
    const std::uint8_t* e{};
    bool highNibble{false};

    bool read(std::uint8_t& out) noexcept {
        if (p >= e) return false;
        if (!highNibble) {
            out = static_cast<std::uint8_t>(*p & 0x0fu);
            highNibble = true;
            return true;
        }
        out = static_cast<std::uint8_t>((*p >> 4u) & 0x0fu);
        ++p;
        highNibble = false;
        return true;
    }

    bool finished() const noexcept {
        return highNibble ? (p + 1 == e) : (p == e);
    }
};

bool slices(std::span<const std::uint8_t> data, const Header& h, std::span<const std::uint8_t>& time, std::span<const std::uint8_t>& mask, std::span<const std::uint8_t>& bid, std::span<const std::uint8_t>& spread, std::span<const std::uint8_t>& bidQty, std::span<const std::uint8_t>& askQty) noexcept {
    std::size_t off = kHeaderBytes; const auto take = [&](std::uint32_t n, std::span<const std::uint8_t>& s) { if (off + n > data.size()) return false; s = data.subspan(off, n); off += n; return true; };
    return take(h.timeBytes,time) && take(h.maskBytes,mask) && take(h.bidBytes,bid) && take(h.spreadBytes,spread) && take(h.bidQtyBytes,bidQty) && take(h.askQtyBytes,askQty) && off == data.size();
}

Status decodeBytes(std::span<const std::uint8_t> data, std::string* jsonl, std::ostream* encoded) noexcept {
    Header h{}; if (!readHeader(data, h)) return Status::CorruptData;
    std::span<const std::uint8_t> tsS, maskS, bidS, spreadS, bidQtyS, askQtyS; if (!slices(data,h,tsS,maskS,bidS,spreadS,bidQtyS,askQtyS)) return Status::CorruptData;
    const auto* tp=tsS.data(); const auto* te=tsS.data()+tsS.size(); const auto* mp=maskS.data(); const auto* me=maskS.data()+maskS.size(); const auto* bp=bidS.data(); const auto* be=bidS.data()+bidS.size(); const auto* sp=spreadS.data(); const auto* se=spreadS.data()+spreadS.size(); const auto* bqp=bidQtyS.data(); const auto* bqe=bidQtyS.data()+bidQtyS.size(); const auto* aqp=askQtyS.data(); const auto* aqe=askQtyS.data()+askQtyS.size();
    MaskReader masks{mp, me};
    std::int64_t ts=h.baseTsUnit, bid=h.baseBidTick, spread=h.baseSpreadTick, bq=h.baseBidQtyLot, aq=h.baseAskQtyLot;
    if (encoded) *encoded << "{\n  \"pipeline_id\": \"" << (h.version == kVersionV2 ? "hftmac.bookticker_delta_mask_v2" : "hftmac.bookticker_delta_mask_v1") << "\",\n  \"record_count\": " << h.recordCount << ",\n  \"base_state\": {\"ts\": " << ts*h.timeScale << ", \"bid\": " << bid*h.priceScale << ", \"spread\": " << spread*h.priceScale << ", \"bid_qty\": " << bq*h.qtyScale << ", \"ask_qty\": " << aq*h.qtyScale << "},\n  \"updates\": [\n";
    for (std::uint64_t i=0; i<h.recordCount; ++i) {
        std::uint8_t m = 0;
        std::uint64_t dt = 0;
        std::int64_t dbid = 0, dspread = 0, dbq = 0, daq = 0;
        if (i != 0) {
            if (!readVar(tp,te,dt)) return Status::CorruptData;
            if (h.version == kVersionV2) { if (!masks.read(m)) return Status::CorruptData; }
            else { if (mp >= me) return Status::CorruptData; m = *mp++; }
            ts += static_cast<std::int64_t>(dt);
            std::uint64_t v=0;
            if (m & 1u) { if (!readVar(bp,be,v)) return Status::CorruptData; dbid = unzz(v); bid += dbid; }
            if (m & 2u) { if (!readVar(sp,se,v)) return Status::CorruptData; dspread = unzz(v); spread += dspread; }
            if (m & 4u) { if (!readVar(bqp,bqe,v)) return Status::CorruptData; dbq = unzz(v); bq += dbq; }
            if (m & 8u) { if (!readVar(aqp,aqe,v)) return Status::CorruptData; daq = unzz(v); aq += daq; }
        }
        if (jsonl) *jsonl += "[" + std::to_string(bid*h.priceScale) + "," + std::to_string(bq*h.qtyScale) + "," + std::to_string((bid+spread)*h.priceScale) + "," + std::to_string(aq*h.qtyScale) + "," + std::to_string(ts*h.timeScale) + "]\n";
        if (encoded) {
            *encoded << (i ? ",\n" : "") << "    {\"dt\": " << dt << ", \"mask\": " << static_cast<unsigned>(m)
                     << ", \"d\": {\"bid\": " << dbid << ", \"spread\": " << dspread << ", \"bid_qty\": " << dbq << ", \"ask_qty\": " << daq << "}"
                     << ", \"state\": [" << bid*h.priceScale << "," << bq*h.qtyScale << "," << (bid+spread)*h.priceScale << "," << aq*h.qtyScale << "," << ts*h.timeScale << "]}";
        }
    }
    if (tp!=te || (h.version == kVersionV2 ? !masks.finished() : mp!=me) || bp!=be || sp!=se || bqp!=bqe || aqp!=aqe) return Status::CorruptData;
    if (encoded) *encoded << "\n  ]\n}\n";
    return Status::Ok;
}

std::string statsJson(const Header& h) {
    std::ostringstream o; o << "{\n  \"pipeline_id\": \"hftmac.bookticker_delta_mask_v1\",\n  \"version\": " << h.version << ",\n  \"record_count\": " << h.recordCount << ",\n  \"raw_runtime_bytes\": " << (h.recordCount*40u) << ",\n  \"encoded_bytes\": " << h.outputBytes << ",\n  \"bytes_per_record\": " << (h.recordCount? static_cast<double>(h.outputBytes)/static_cast<double>(h.recordCount):0.0) << ",\n  \"mask_nonzero_count\": " << h.maskNonZero << ",\n  \"bid_changed_count\": " << h.bidChanged << ",\n  \"spread_changed_count\": " << h.spreadChanged << ",\n  \"bid_qty_changed_count\": " << h.bidQtyChanged << ",\n  \"ask_qty_changed_count\": " << h.askQtyChanged << ",\n  \"time_stream_bytes\": " << h.timeBytes << ",\n  \"mask_stream_bytes\": " << h.maskBytes << ",\n  \"bid_delta_stream_bytes\": " << h.bidBytes << ",\n  \"spread_delta_stream_bytes\": " << h.spreadBytes << ",\n  \"bid_qty_delta_stream_bytes\": " << h.bidQtyBytes << ",\n  \"ask_qty_delta_stream_bytes\": " << h.askQtyBytes << "\n}\n"; return o.str();
}

bool readFile(const std::filesystem::path& p, std::vector<std::uint8_t>& out) noexcept { return internal::readFileBytes(p,out); }
Status emitText(const std::string& s, const DecodedBlockCallback& cb) noexcept { return cb(std::span<const std::uint8_t>{reinterpret_cast<const std::uint8_t*>(s.data()), s.size()}) ? Status::Ok : Status::CallbackStopped; }

} // namespace

CompressionResult compress(const CompressionRequest& request, const PipelineDescriptor& pipeline) noexcept {
    if (request.inputPath.empty()) { auto r=internal::fail(Status::InvalidArgument,request,&pipeline,"input path is empty"); metrics::recordRun(r); return r; }
    if (inferStreamTypeFromPath(request.inputPath) != StreamType::BookTicker) { auto r=internal::fail(Status::UnsupportedStream,request,&pipeline,"expected bookticker.jsonl"); metrics::recordRun(r); return r; }
    CompressionResult r{}; internal::applyPipeline(r,&pipeline); r.streamType=StreamType::BookTicker; r.inputPath=request.inputPath; const auto total=timing::nowNs();
    std::vector<std::uint8_t> input; const auto rs=timing::nowNs(); if (!internal::readFileBytes(request.inputPath,input)) { auto f=internal::fail(Status::IoError,request,&pipeline,"failed to read input file"); metrics::recordRun(f); return f; } r.readNs=timing::nowNs()-rs; r.inputBytes=input.size();
    std::vector<Row> rows; const auto ps=timing::nowNs(); rows.reserve(std::count(input.begin(),input.end(),static_cast<std::uint8_t>('\n'))+1u); if (!parseRows(input,rows)) { auto f=internal::fail(Status::CorruptData,request,&pipeline,"input is not canonical bookticker jsonl"); metrics::recordRun(f); return f; } r.parseNs=timing::nowNs()-ps;
    Header h{}; h.version = pipeline.id == std::string_view{"hftmac.bookticker_delta_mask_v2"} ? kVersionV2 : kVersionV1; h.inputBytes=r.inputBytes; h.recordCount=rows.size(); std::int64_t timeG=rows.front().ts, priceG=0, qtyG=0; for (const auto& x: rows) { timeG=gcdAbs(timeG,x.ts-rows.front().ts); priceG=gcdAbs(priceG,x.bid); priceG=gcdAbs(priceG,x.ask); qtyG=gcdAbs(qtyG,x.bidQty); qtyG=gcdAbs(qtyG,x.askQty); } h.timeScale=safeScale(timeG); h.priceScale=safeScale(priceG); h.qtyScale=safeScale(qtyG);
    h.baseTsUnit=rows.front().ts/h.timeScale; h.baseBidTick=rows.front().bid/h.priceScale; h.baseSpreadTick=(rows.front().ask-rows.front().bid)/h.priceScale; h.baseBidQtyLot=rows.front().bidQty/h.qtyScale; h.baseAskQtyLot=rows.front().askQty/h.qtyScale;
    std::vector<std::uint8_t> timeS, maskS, bidS, spreadS, bidQtyS, askQtyS; const auto es=timing::nowNs(); const auto ec0=timing::readCycles();
    std::int64_t pts=h.baseTsUnit, pb=h.baseBidTick, pspr=h.baseSpreadTick, pbq=h.baseBidQtyLot, paq=h.baseAskQtyLot;
    bool maskHighNibble = false;
    for (std::size_t i=1;i<rows.size();++i) { const auto ts=rows[i].ts/h.timeScale, b=rows[i].bid/h.priceScale, spr=(rows[i].ask-rows[i].bid)/h.priceScale, bq=rows[i].bidQty/h.qtyScale, aq=rows[i].askQty/h.qtyScale; varint(timeS, static_cast<std::uint64_t>(ts-pts)); std::uint8_t m=0; if (b!=pb) m|=1u; if (spr!=pspr) m|=2u; if (bq!=pbq) m|=4u; if (aq!=paq) m|=8u; if (h.version == kVersionV2) pushPackedMask(maskS,m,maskHighNibble); else maskS.push_back(m); if (m) ++h.maskNonZero; if (m&1u){varint(bidS,zz(b-pb));++h.bidChanged;} if(m&2u){varint(spreadS,zz(spr-pspr));++h.spreadChanged;} if(m&4u){varint(bidQtyS,zz(bq-pbq));++h.bidQtyChanged;} if(m&8u){varint(askQtyS,zz(aq-paq));++h.askQtyChanged;} pts=ts; pb=b; pspr=spr; pbq=bq; paq=aq; }
    r.encodeCycles=timing::readCycles()-ec0; r.encodeCoreNs=timing::nowNs()-es; h.timeBytes=timeS.size(); h.maskBytes=maskS.size(); h.bidBytes=bidS.size(); h.spreadBytes=spreadS.size(); h.bidQtyBytes=bidQtyS.size(); h.askQtyBytes=askQtyS.size();
    const auto outPath=internal::outputPathFor(request,pipeline,StreamType::BookTicker); std::error_code dirEc; std::filesystem::create_directories(outPath.parent_path(),dirEc); if(dirEc){ auto f=internal::fail(Status::IoError,request,&pipeline,"failed to create output directory"); metrics::recordRun(f); return f; }
    h.outputBytes=kHeaderBytes+h.timeBytes+h.maskBytes+h.bidBytes+h.spreadBytes+h.bidQtyBytes+h.askQtyBytes; r.outputPath=outPath; r.metricsPath=outPath.parent_path()/(outPath.stem().string()+".metrics.json"); const auto ws=timing::nowNs(); std::ofstream out(outPath,std::ios::binary|std::ios::trunc); auto hb=headerBytes(h); out.write(reinterpret_cast<const char*>(hb.data()),hb.size()); for(auto* s:{&timeS,&maskS,&bidS,&spreadS,&bidQtyS,&askQtyS}) out.write(reinterpret_cast<const char*>(s->data()),static_cast<std::streamsize>(s->size())); out.close(); r.writeNs=timing::nowNs()-ws; r.encodeNs=timing::nowNs()-total; r.outputBytes=h.outputBytes; r.lineCount=h.recordCount; r.blockCount=1;
    std::string decoded; const auto ds=timing::nowNs(); const auto dc=timing::readCycles(); std::vector<std::uint8_t> file; readFile(outPath,file); const auto st=decodeBytes(file,&decoded,nullptr); r.decodeCycles=timing::readCycles()-dc; r.decodeNs=timing::nowNs()-ds; r.decodeCoreNs=r.decodeNs; r.roundtripOk=isOk(st)&&decoded.size()==input.size()&&std::equal(decoded.begin(),decoded.end(),reinterpret_cast<const char*>(input.data())); r.status=r.roundtripOk?Status::Ok:Status::DecodeError; if(!r.roundtripOk) r.error="roundtrip check failed"; (void)internal::writeTextFile(r.metricsPath,toMetricsJson(r)); metrics::recordRun(r); return r;
}

ReplayArtifactInfo inspectArtifact(const std::filesystem::path& path, const PipelineDescriptor& pipeline) noexcept { std::vector<std::uint8_t> data; ReplayArtifactInfo i{}; i.path=path; if(!readFile(path,data)){i.status=Status::IoError;i.error="failed to read artifact";return i;} Header h{}; if(!readHeader(data,h)){i.status=Status::CorruptData;i.error="invalid bookticker artifact";return i;} i.status=Status::Ok; i.found=true; i.formatId=h.version == kVersionV2 ? "hftmac.bookticker_delta_mask.v2" : "hftmac.bookticker_delta_mask.v1"; i.pipelineId=std::string{pipeline.id}; i.transform=std::string{pipeline.transform}; i.entropy=std::string{pipeline.entropy}; i.streamType=StreamType::BookTicker; i.version=h.version; i.inputBytes=h.inputBytes; i.outputBytes=h.outputBytes; i.lineCount=h.recordCount; i.blockCount=1; return i; }
Status decode(std::span<const std::uint8_t> bytes, const DecodedBlockCallback& cb) noexcept { std::string out; const auto st=decodeBytes(bytes,&out,nullptr); if(!isOk(st)) return st; return emitText(out,cb); }
Status decodeFile(const std::filesystem::path& path, const DecodedBlockCallback& cb) noexcept { std::vector<std::uint8_t> data; if(!readFile(path,data)) return Status::IoError; return decode(data,cb); }
Status inspectEncodedJsonFile(const std::filesystem::path& path, const DecodedBlockCallback& cb) noexcept { std::vector<std::uint8_t> data; if(!readFile(path,data)) return Status::IoError; std::ostringstream out; const auto st=decodeBytes(data,nullptr,&out); if(!isOk(st)) return st; return emitText(out.str(),cb); }
Status inspectEncodedBinaryFile(const std::filesystem::path& path, const DecodedBlockCallback& cb) noexcept { std::vector<std::uint8_t> data; if(!readFile(path,data)) return Status::IoError; std::ostringstream o; Header h{}; if(!readHeader(data,h)) return Status::CorruptData; o << (h.version == kVersionV2 ? "bookticker_delta_mask_v2" : "bookticker_delta_mask_v1") << " bytes=" << data.size() << " header=" << kHeaderBytes << " time=" << h.timeBytes << " mask=" << h.maskBytes << " bid=" << h.bidBytes << " spread=" << h.spreadBytes << " bid_qty=" << h.bidQtyBytes << " ask_qty=" << h.askQtyBytes << "\n"; return emitText(o.str(),cb); }
Status inspectStatsJsonFile(const std::filesystem::path& path, const DecodedBlockCallback& cb) noexcept { std::vector<std::uint8_t> data; if(!readFile(path,data)) return Status::IoError; Header h{}; if(!readHeader(data,h)) return Status::CorruptData; return emitText(statsJson(h),cb); }

}  // namespace hft_compressor::codecs::bookticker_delta_mask

#include "OfflineCase.hpp"
#include "hft_compressor/Compressor.hpp"
#include "hft_compressor/ReplayDecode.hpp"
#include <array>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <new>
#include <stdexcept>
#include <string>
#include <unistd.h>

// Exercise the actual CLI consumer without a subprocess or a test-only API.
#define main compressorCliMain
#include "../../src/Process/Main.cpp"
#undef main

namespace {
using namespace hft_compressor;
constexpr const char* trades="[100000001,200000003,0,1000000000]\n[100000002,300000004,1,1000000001]\n[100000002,200000003,0,1000000002]\n";
struct Fixture {
  std::filesystem::path root=std::filesystem::current_path()/
      ("compressor-fixture-"+std::to_string(getpid())+"-"+
       std::to_string(std::chrono::steady_clock::now().time_since_epoch().count()));
  std::filesystem::path input;
  CompressionResult result;
  Fixture(const char* file,const std::string& rows,const char* pipeline) {
    CXET_CHECK(std::filesystem::create_directories(root)); input=root/file;
    {std::ofstream out(input,std::ios::binary); out<<rows; CXET_CHECK(out.good());}
    CompressionRequest request{}; request.inputPath=input; request.outputRoot=root/"output";
    request.pipelineId=pipeline; request.blockBytes=1024;
    result=compress(request); CXET_CHECK(result.status==Status::Ok && !result.outputPath.empty());
  }
  ~Fixture(){std::error_code ec;std::filesystem::remove_all(root,ec);}
  ReplayArtifactInfo artifact() const {
    ReplayArtifactRequest request{};request.compressedRoot=root/"output";
    request.sessionId=root.filename().string();request.streamType=result.streamType;
    request.preferredPipelineId=result.pipelineId;
    auto artifact=discoverReplayArtifact(request);
    CXET_CHECK(artifact.status==Status::Ok && artifact.found && artifact.path==result.outputPath);
    CXET_CHECK(!artifact.formatId.empty() && artifact.pipelineId==result.pipelineId);
    return artifact;
  }
  void verifyRecords(std::uint64_t count) const {
    DecodeVerifyRequest request{};request.compressedPath=result.outputPath;request.canonicalPath=input;
    request.pipelineId=result.pipelineId;request.verifyMode=VerifyMode::RecordExact;
    auto verified=decodeAndVerify(request);
    CXET_CHECK(verified.status==Status::Ok && verified.verified && verified.recordExact);
    CXET_CHECK(verified.canonicalRecordCount==count && verified.decodedRecordCount==count);
  }
};
void zstdBytes() {
  Fixture f("trades.jsonl",trades,"std.zstd_jsonl_blocks_v1");
  DecodeVerifyRequest request{}; request.compressedPath=f.result.outputPath;request.canonicalPath=f.input;
  request.pipelineId=f.result.pipelineId;request.verifyMode=VerifyMode::Both;
  auto v=decodeAndVerify(request);
  CXET_CHECK(v.status==Status::Ok && v.verified && v.byteExact && v.recordExact && v.mismatchBytes==0);
}
void groupedTrades() {
  Fixture f("trades.jsonl",trades,"hftmac.trades_grouped_delta_qtydict_math_v3");f.verifyRecords(3);
  std::uint64_t count=0;
  CXET_CHECK(decodeReplayArtifactRecordBatches(f.artifact(),1,[&](const ReplayRecordBatch& b){
    CXET_CHECK(b.trades.size()==1 && b.trades[0].tsNs==1000000000+static_cast<std::int64_t>(count));
    CXET_CHECK(b.trades[0].qtyE8==(count==1?300000004:200000003));++count;return true;
  })==Status::Ok && count==3);
}
void bboMask() {
  Fixture f("bookticker.jsonl","[100000001,200000003,100000011,400000005,1000000000]\n[100000002,200000003,100000012,400000006,1000000001]\n",
      "hftmac.bookticker_delta_mask_v2");f.verifyRecords(2);
}
void depthLadder() {
  Fixture f("depth.jsonl","[[100000001,200000003,0],[100000011,400000005,1],1000000000]\n[[100000001,0,0],[100000012,400000006,1],1000000001]\n",
      "hftmac.depth_ladder_offset_v3");f.verifyRecords(2);
}
std::vector<std::uint8_t> bytes(const std::filesystem::path& path) {
  std::ifstream in(path,std::ios::binary);CXET_CHECK(in.good());
  return {std::istreambuf_iterator<char>(in),std::istreambuf_iterator<char>()};
}
void corruptCrc() {
  Fixture f("trades.jsonl",trades,"std.zstd_jsonl_blocks_v1");auto data=bytes(f.result.outputPath);
  std::size_t originalBytes=0;
  CXET_CHECK(decodeHfcBuffer(data,[&](auto block){originalBytes+=block.size();return true;})==Status::Ok);
  CXET_CHECK(originalBytes==std::string(trades).size());
  CXET_CHECK(data.size()>32);data.back()^=0x40;std::size_t delivered=0;
  CXET_CHECK(decodeHfcBuffer(data,[&](auto block){delivered+=block.size();return true;})!=Status::Ok);
  CXET_CHECK(delivered==0);
}
void truncatedBlock() {
  Fixture f("trades.jsonl",trades,"std.zstd_jsonl_blocks_v1");auto data=bytes(f.result.outputPath);
  CXET_CHECK(data.size()>32);data.resize(data.size()-7);
  CXET_CHECK(decodeHfcBuffer(data,[](auto){return true;})!=Status::Ok);
}
void unsupportedPipeline() {
  Fixture f("trades.jsonl",trades,"std.raw_jsonl_blocks_v1");
  CompressionRequest r{};r.inputPath=f.input;r.outputRoot=f.root/"refused";r.pipelineId="unknown-pipeline";
  CXET_CHECK(compress(r).status==Status::UnsupportedPipeline);
  r.pipelineId="hftmac.bookticker_delta_mask_v2";
  CXET_CHECK(compress(r).status==Status::UnsupportedStream);
}
void callbackBoundaries() {
  Fixture f("trades.jsonl",trades,"std.raw_jsonl_blocks_v1");std::uint64_t count=0;
  CXET_CHECK(decodeReplayArtifactRecordBatches(f.artifact(),2,[&](const ReplayRecordBatch& b){
    CXET_CHECK(b.recordCount()<=2 && b.firstLineNumber==count+1);count+=b.recordCount();return true;
  })==Status::Ok && count==3);
  std::size_t callbacks=0;
  CXET_CHECK(decodeReplayArtifactRecordBatches(f.artifact(),1,[&](const ReplayRecordBatch&){++callbacks;return false;})==Status::CallbackStopped);
  CXET_CHECK(callbacks==1);
}
void callbackExceptionsRefuseRecordAdmission() {
  Fixture f("trades.jsonl",trades,"hftmac.trades_grouped_delta_qtydict_math_v3");
  CXET_CHECK(decodeReplayArtifactRecordBatches(f.artifact(),1,[](const auto&) -> bool {
    throw std::runtime_error("consumer rejected row");
  })==Status::DecodeError);
  CXET_CHECK(decodeReplayArtifactRecordBatches(f.artifact(),1,[](const auto&) -> bool {
    throw std::bad_alloc{};
  })==Status::DecodeError);
  const auto artifact=f.artifact();
  const auto input=bytes(artifact.path);
  CXET_CHECK(decodeReplayArtifactRecordBatches(artifact,1,[&](const auto&) {
    std::fstream out(artifact.path,std::ios::binary|std::ios::in|std::ios::out);
    out.seekp(static_cast<std::streamoff>(input.size()-1u));
    out.put(static_cast<char>(input.back()^1u)); out.close(); CXET_CHECK(!out.fail());
    return false;
  })==Status::CorruptData);
}
void failedRoundtripCannotPublishOrOverwriteArtifact() {
  Fixture f("trades.jsonl",trades,"hftmac.trades_grouped_delta_qtydict_math_v3");
  // Valid native input grammar, deliberately not byte-canonical: the real
  // encoder writes output, then its roundtrip consumer refuses admission.
  { std::ofstream out(f.input,std::ios::binary|std::ios::trunc);
    out << "[100000001, 200000003,0,1000000000]\n"; CXET_CHECK(out.good()); }
  CompressionRequest request{}; request.inputPath=f.input;
  request.pipelineId=f.result.pipelineId; request.outputPathOverride=f.root/"failure.hfc";
  CXET_CHECK(!isOk(compress(request).status));
  CXET_CHECK(!std::filesystem::exists(request.outputPathOverride));
  { std::ofstream out(request.outputPathOverride,std::ios::binary); out << "existing artifact"; CXET_CHECK(out.good()); }
  CXET_CHECK(!isOk(compress(request).status));
  std::ifstream in(request.outputPathOverride,std::ios::binary);
  const std::string retained{std::istreambuf_iterator<char>(in),std::istreambuf_iterator<char>()};
  CXET_CHECK(retained=="existing artifact");
  for (const auto& entry:std::filesystem::directory_iterator(f.root))
    CXET_CHECK(entry.path().filename().string().find(".partial.")==std::string::npos);
}
void cliWriteFailureCannotReportInspectSuccess() {
  Fixture f("trades.jsonl",trades,"hftmac.trades_grouped_delta_qtydict_math_v3");
  struct StdoutGuard {
    std::FILE* previous{stdout};
    std::FILE* replacement{};
    explicit StdoutGuard(const std::filesystem::path& path) {
      replacement=std::fopen(path.c_str(),"rb"); CXET_CHECK(replacement);
      stdout=replacement; // Any fwrite to this owned read-only stream fails.
    }
    ~StdoutGuard(){stdout=previous;std::fclose(replacement);}
  } guard(f.result.outputPath);
  std::array<std::string,6> arguments{"hft-compressor","inspect","--input",f.result.outputPath.string(),"--view","canonical-json"};
  std::array<char*,6> argv{};
  for (std::size_t i=0;i<argv.size();++i) argv[i]=arguments[i].data();
  CXET_CHECK(compressorCliMain(static_cast<int>(argv.size()),argv.data())==1);
}
void nativeDiagnosticCallbackExceptionsReturnStatus() {
  struct Input { const char* file; const char* rows; const char* pipeline; };
  const Input inputs[]{
    {"trades.jsonl",trades,"hftmac.trades_grouped_delta_qtydict_math_v3"},
    {"bookticker.jsonl","[100000001,200000003,100000011,400000005,1000000000]\n","hftmac.bookticker_delta_mask_v2"},
    {"depth.jsonl","[[100000001,200000003,0],[100000011,400000005,1],1000000000]\n","hftmac.depth_ladder_offset_v3"},
  };
  for (const auto& input:inputs) {
    Fixture f(input.file,input.rows,input.pipeline);
    for (const auto* view:{"encoded-json","encoded-binary","stats"})
      CXET_CHECK(inspectCompressedArtifact(f.result.outputPath,input.pipeline,view,[](auto) -> bool {
        throw std::runtime_error("diagnostic consumer failed");
      })==Status::DecodeError);
  }
}
}
int main(int argc,char** argv) {
  const cxet::testing::Case cases[]{
    cxet::testing::Case{"compressor.zstd_roundtrip_is_byte_and_record_exact",zstdBytes},
    cxet::testing::Case{"compressor.grouped_trade_batches_preserve_integer_rows",groupedTrades},
    cxet::testing::Case{"compressor.bbo_delta_mask_preserves_integer_values",bboMask},
    cxet::testing::Case{"compressor.depth_ladder_preserves_mutation_order_and_deletion",depthLadder},
    cxet::testing::Case{"compressor.corrupt_block_crc_cannot_publish_payload",corruptCrc},
    cxet::testing::Case{"compressor.truncated_block_refuses_decode",truncatedBlock},
    cxet::testing::Case{"compressor.unknown_pipeline_and_wrong_stream_refuse",unsupportedPipeline},
    cxet::testing::Case{"compressor.record_batch_boundary_and_callback_stop_are_exact",callbackBoundaries},
    cxet::testing::Case{"compressor.consumer_callback_exceptions_refuse_record_admission",callbackExceptionsRefuseRecordAdmission},
    cxet::testing::Case{"compressor.failed_roundtrip_cannot_publish_or_overwrite_artifact",failedRoundtripCannotPublishOrOverwriteArtifact},
    cxet::testing::Case{"compressor.cli_write_failure_cannot_report_success",cliWriteFailureCannotReportInspectSuccess},
    cxet::testing::Case{"compressor.native_diagnostic_callback_exceptions_return_status",nativeDiagnosticCallbackExceptionsReturnStatus},
  };return cxet::testing::runCases(argc,argv,cases);
}

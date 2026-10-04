#include "OfflineCase.hpp"
#include "hft_compressor/Compressor.hpp"
#include "hft_compressor/ReplayDecode.hpp"
#include <chrono>
#include <filesystem>
#include <fstream>
#include <iterator>
#include <string>
#include <unistd.h>

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
  };return cxet::testing::runCases(argc,argv,cases);
}

# hft-compressor

Standalone compression core for canonical `apps/hft-recorder` market-data
corpora. It is built separately and consumed as a prebuilt shared library.

## Current scope

Input is one canonical JSONL channel file:

- `trades.jsonl`
- `bookticker.jsonl`
- `depth.jsonl`

The core is organized around compression pipelines, not a single codec enum.
A pipeline describes:

- stream scope: all streams or a specific family
- representation: JSONL blocks, delta rows, columnar rows, orderbook keyframes
- transform: raw JSONL, delta, delta+varint, columnar delta
- entropy stage: zstd, lz4, brotli, xz, arithmetic coding, range coding, rANS
- profile: live, archive, replay, or research

Only `std.zstd_jsonl_blocks_v1` is implemented today. The other descriptors are
intentional placeholders for the coursework benchmark matrix and return
`not_implemented` until their implementation is added.

Current baseline output:

```text
compressedData/zstd/sessions/<session>/<channel>.hfc
compressedData/zstd/sessions/<session>/<channel>.metrics.json
```

This path and extension are implementation details of the current baseline.
Replay consumers should ask the public API to discover an artifact for a root,
session, stream, and preference; they should not construct `.hfc` paths.
Decoding is block streaming: one compressed block is decompressed into scratch
memory, emitted to the caller, and then discarded.

## Source layout

New implementations should be added in separate folders, then registered in the
pipeline registry:

- `src/codecs/<codec_or_method>/` - codec/pipeline bodies such as zstd JSONL blocks, future lz4, rANS, PFor, delta+varint, or orderbook-specific methods.
- `src/container/hfc/` - current `.hfc` JSONL-block baseline container.
- `src/pipelines/` - pipeline descriptors and availability metadata.
- `src/common/` - small shared helpers used by implementations.

The public API stays in `include/hft_compressor/`; consumers should not include
private files from `src/`.

## Public API

Use `listPipelines()` to discover available and planned pipelines, then call
`compress()` with an explicit `pipelineId`.

```cpp
hft_compressor::CompressionRequest request{};
request.inputPath = "trades.jsonl";
request.outputRoot = "compressedData";
request.pipelineId = "std.zstd_jsonl_blocks_v1";
const auto result = hft_compressor::compress(request);
```

Replay-facing callers should use `discoverReplayArtifact()` and then
`decodeReplayArtifactJsonl()`. The request names the compressed root, session,
stream, and preference; compressor owns the path and format policy.
`decodeReplayRecords()` is reserved for future binary winners that emit
compressor-owned neutral records instead of JSONL-compatible chunks.
`decodeHfcFile()` and `decodeHfcBuffer()` remain baseline-specific helpers for
callers that intentionally work with the current container. None of these APIs
materializes the full decoded corpus.

CLI examples:

```bash
hft-compressor --list-pipelines
hft-compressor trades.jsonl --pipeline std.zstd_jsonl_blocks_v1 compressedData
```

## Build

`hft-compressor` is a standalone module. Build it separately and pass the
resulting library to `hft-recorder`; do not add this directory as a recorder
subdirectory.

Windows/MSVC:

```powershell
cmake -S apps/hft-compressor -B apps/hft-compressor/build
cmake --build apps/hft-compressor/build --config Release --target hft_compressor_core
```

Linux/WSL:

```bash
./compile.sh
```

The stable Linux artifact is:

```text
apps/hft-compressor/build/libhft_compressor_core.so
```

`hft-recorder` consumes the module through:

- `HFT_COMPRESSOR_PUBLIC_INCLUDE_DIR`
- `HFT_COMPRESSOR_SHARED_LIB`
- `HFT_COMPRESSOR_RUNTIME_LIB` on Windows when the runtime DLL is separate from the import lib

## Coursework direction

The registry already names standard baselines, Python research baselines,
domain transforms, hybrid pipelines, and custom entropy coders. This keeps the
next work focused on implementing and benchmarking pipeline bodies rather than
reshaping the public API each time a new idea is tested.

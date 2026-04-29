# Codec verification and replay contract

This document is the gate for adding production/ranked codecs to `hft-compressor`.
Lab-only codecs may exist outside this contract, but they must not be shown as ranked
or used by replay until they pass the same checks.

## Production codec gate

A production codec must expose the backend functions registered in `PipelineBackend`:

- `inspectArtifact(path, pipeline)` validates the artifact header, codec id, stream id,
  block table, decoded byte offsets, stored byte totals, trailing bytes, and checksums.
  File extension is not the contract.
- `decodeJsonl(path, onBlock)` streams decoded JSONL blocks to the callback. It must not
  materialize a full decoded corpus on disk or in one large decoded buffer.
- `decodeBuffer(bytes, onBlock)` is required when the codec supports buffer-backed decode.
- `compress(request, pipeline)` must perform an internal byte-exact roundtrip before
  returning `Status::Ok`.

A codec may be listed as available only when its dependency is found at configure time.
If the dependency is missing, the pipeline stays visible as `DependencyUnavailable` with
an explicit reason.

## Correctness checks

Before a codec is treated as ranked, it must pass the shared tests for:

- strict canonical input validation for trades, bookticker, and depth JSONL;
- byte-exact decode against the canonical JSONL;
- record-exact decode through `decodeAndVerify(..., VerifyMode::Both)`;
- semantic-only `RecordExact` verification without requiring byte-exact formatting;
- replay batch parsing through `decodeReplayRecordBatches`;
- corrupted artifacts: bad header, bad block header, truncated payload, wrong checksum,
  wrong stream, and trailing bytes.

Python lab codecs must either call the same verifier or mark their metrics as unranked.
A Python result that only compares file sizes or compression ratios is research-only.

## CXETCPP replay direction

`hft-compressor` remains independent from CXETCPP internals. Future replay integration
should consume the public streaming APIs only:

1. Discover artifact by `pipeline_id`, `format_id`, and stream metadata.
2. Decode through `decodeReplayRecordBatches` or `decodeReplayRecords`.
3. Feed batches into the recorder/replay timeline merger and replay clock.
4. Publish normalized replay events to the local CXETCPP adapter/event bus.

Do not implement replay by expanding a large compressed session into a full decoded JSONL
file first. Decode errors must fail fast on unsupported codec, schema/header mismatch,
wrong stream, checksum mismatch, or truncated artifact.

## Python lab codecs

Python codec implementations live under `src/codecs/python/`, next to the C++ codec
folders. Each codec has its own file, for example `gzip_codec.py`, `lzma_codec.py`,
`zstandard_codec.py`, and `lz4_frame_codec.py`.

The `python/` directory is only a thin user-facing wrapper/CLI over `src/codecs/python`.
These Python codecs are lab-only and unranked by default. They must report byte-exact
and record-exact verification before their metrics are used in comparisons.

Run examples from `apps/hft-compressor`:

```bash
PYTHONPATH=python python3 -m hft_compressor list
PYTHONPATH=python python3 -m hft_compressor compress ./session/trades.jsonl --codec py.gzip_v1 --output-root ./compressedData-python
PYTHONPATH=python python3 -m hft_compressor verify ./compressedData-python/py-gzip/sessions/session/trades.pylab.gz ./session/trades.jsonl --codec py.gzip_v1
```

# Agent rules - hft-compressor

`hft-compressor` is the app-layer compression core and codec/pipeline library used by recorder/lab workflows.

## Boundaries

- Keep compression codecs, file formats, decode/verify logic, pipeline registry, and compression metrics inside this app.
- Do not move compression experiments into CXETCPP core `src/src`.
- Do not make recorder depend on compressor private internals; expose stable public headers in `include/hft_compressor/`.

## Research contract

- Baseline comparisons are mandatory before claiming a custom codec is better: `zstd`, `lz4`, `brotli`, `xz/lzma`.
- Verification must be lossless unless the user explicitly asks for lossy research.
- Rankings must be per stream family, not global.
- Track ratio, encode speed, decode speed, block size, stream family, and corpus identity.
- Prefer simple, reproducible codecs before adding complex speculative transforms.

## Build/test restraint

- Do not run builds/tests/benchmarks unless the current user message explicitly asks for the exact verification.
- If verification is needed, state the exact command for the user to run manually.

## Subagents

Use subagents only for disjoint areas: one codec, pipeline registry, container format, benchmark/reporting, or docs.

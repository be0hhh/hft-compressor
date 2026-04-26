# Agent rules - hft-compressor

This directory is a standalone compression application/library under `CXETCPP/apps`.
It may later be published as a separate public GitHub repository, but it must stay
usable from the local monorepo layout first.

## Hard boundary

- `hft-compressor` is not part of the core `CXETCPP` normalizer library.
- It consumes corpus files and exposes a small public compression API.
- Do not compile, vendor, or depend on `CXETCPP/src/src` internals.
- Do not use `add_subdirectory()` from consumers such as `hft-recorder` or future `CXETCPP` integration paths.
- Consumers must link it as a prebuilt shared library and include only headers from `include/`.
- Keep this module lightweight enough that `CXETCPP` can optionally consume it without pulling GUI, recorder, benchmark dashboards, or app state into the core library.

## Build contract

- Canonical Linux/WSL build command: `./compile.sh` from `apps/hft-compressor`.
- Canonical Linux artifact: `build/libhft_compressor_core.so`.
- Windows builds may produce `.dll`/`.lib` for local GUI work, but the stable cross-app artifact for `hft-recorder` and future `CXETCPP` use is the Linux `.so` path above.
- `hft-recorder` must consume the library through `HFT_COMPRESSOR_PUBLIC_INCLUDE_DIR` and `HFT_COMPRESSOR_SHARED_LIB`, not by building this directory as a child project.
- Do not run broad rebuilds after every edit. Use targeted `cmake --build ... --target hft_compressor_core`, `./compile.sh`, or targeted tests only when they validate the current change.

## Product responsibility

- This module owns compression research and compression runtime for canonical market-data corpora.
- Input for the first workflow is one canonical JSONL stream file named exactly:
  - `trades.jsonl`
  - `bookticker.jsonl`
  - `depth.jsonl`
- Stream family must be inferred from the filename and kept explicit in output paths, metrics, and benchmark labels.
- Do not assume `trades == aggTrade`; keep stream semantics explicit.
- Output files belong under `compressedData/<codec>/sessions/<session>/<channel>.hfc` unless the caller supplies another output root.

## Compression rules

- First shipped codec: `zstd_jsonl_blocks_v1`.
- Decoders must support streaming decoded blocks/rows without materializing the full decoded file.
- Loading the compressed file into memory is allowed for the MVP; materializing the full decoded corpus is not allowed in decoder-facing paths.
- Baseline comparisons are mandatory before claiming a custom codec is better:
  - `zstd`
  - `lz4`
  - `brotli`
  - `xz/lzma`
- Custom codecs must be additive and benchmarked per stream family, not globally.
- Future custom ideas should stay domain-specific: trade-specific, L1/bookticker-specific, and orderbook/depth-specific.

## Metrics and observability

- Keep compression ratio, encode/decode speed, bytes in/out, block count, line count, timing ns, and RDTSCP/cycle counters visible to callers.
- Metrics emitted by this module should be easy for `hft-recorder`, Prometheus, and Grafana to consume.
- Do not hide failed dependency states. If `zstd` or another codec dependency is unavailable, return an explicit status.

## Git and publication

- Do not run Git or GitHub commands unless the user explicitly asks for that exact action in the current message.
- Public GitHub publication must be a separate, user-approved step with an explicit repository name and account/owner.
- Do not push, create remotes, rewrite history, or publish artifacts automatically from normal build/test work.
- If this directory becomes a standalone public repository, keep the public API, `compile.sh`, README, and license boundary clear before publishing.

## Karpathy-style behavioral rules

Source: https://github.com/forrestchang/andrej-karpathy-skills

These rules are secondary to the project-specific rules above. When there is a conflict, follow the stricter local rule.

### 1. Think before coding

- Do not assume silently. State important assumptions before implementation.
- If multiple interpretations exist, surface them instead of picking one invisibly.
- If a simpler approach fits the request better, say so and explain the tradeoff.
- If the task is unclear enough that a reasonable implementation would be risky, stop and ask.

### 2. Simplicity first

- Write the minimum code that solves the requested problem.
- Do not add speculative features, one-off abstractions, or configurability that was not requested.
- Do not add error handling for impossible scenarios just to make code look defensive.
- If a solution is much larger than the problem requires, simplify it before presenting it.

### 3. Surgical changes

- Touch only files and lines that trace directly to the user's request.
- Do not improve adjacent code, comments, formatting, or structure as a drive-by change.
- Match existing style even when a different style would be personally preferred.
- Remove only unused code created by the current change. Mention unrelated dead code instead of deleting it.

### 4. Goal-driven execution

- Convert non-trivial tasks into verifiable success criteria.
- For bug fixes, prefer a reproducing test or targeted check before and after the fix.
- For refactors, preserve behavior and verify with the narrowest meaningful check.
- For multi-step work, keep a short plan with each step tied to a concrete verification.
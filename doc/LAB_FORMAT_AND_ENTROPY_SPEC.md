# hft-compressor lab format and entropy spec

## Boundary

`hft-compressor` owns file layouts, transforms, entropy coders, block headers,
checksums, manifests, and artifact discovery. Consumers such as `src/replay`
pass a compressed root, session id or directory, stream/channel, and preference
into the public API. They must not construct paths such as `<channel>.hfc` or
depend on `.hfc`, zstd, range coder, rANS, arithmetic coding, block CRCs, or
container internals.

The compressor is also the format adapter boundary for backtests and exchange
simulation. It may accept multiple artifact families over time, including raw
JSONL, `.hfc`, `cxcef`, or future binary/container formats. Regardless of input
format, replay-facing decode must expose a stable stream contract to consumers.
For the current baseline that contract is canonical JSONL bytes. Future binary
winners may additionally expose compressor-owned neutral records through
`decodeReplayRecords()`, but those records are still an interchange format, not
CXETCPP internal primitives.

Compressed input may be memory-resident for the MVP and for fast random access
plans. Decoded replay data must be streamed to the parser and must not be held as
a fully materialized decoded file. The parser/replay layer owns line splitting,
normalization into CXETCPP rows or primitives, event ordering, timing policy, and
dispatch to recorder, GUI, callbacks, queues, or algorithms.

`.hfc` is the current HFT Compressor Container for
`std.zstd_jsonl_blocks_v1`. It is supported as the present JSONL-block zstd
baseline, not selected as the final production replay format.

## Lab Artifact Identity

File extensions are not a lab contract. Artifact identity is declared by the
manifest or header:

- `format_id`: container/layout family, for example `hfc.zstd_jsonl_blocks_v1`.
- `pipeline_id`: registry id, for example `std.zstd_jsonl_blocks_v1`.
- `transform`: raw JSONL, delta VarInt, columnar delta, depth keyframe delta.
- `entropy`: zstd, lz4, brotli, xz, arithmetic coder, range coder, rANS.
- `stream`: trades, bookticker, depth.
- `version`: layout/coder version.

The current discovery API may resolve today's `.hfc` files, but that policy
lives inside the compressor library so future lab winners can change paths or
extensions without replay code changes.

## Replay Integration Plan

Backtest and exchange-simulation callers should use the compressor as the single
artifact reader and decoder:

```text
artifact path/root + session + stream + preference
  -> hft-compressor discover/decode
  -> stable decoded stream
  -> CXETCPP/recorder parser
  -> normalized replay events
  -> algorithm, recorder, GUI, or any other consumer
```

The parser should not care whether the source artifact was JSONL, `.hfc`,
`cxcef`, or a later custom compressed format. It should only care about the
stable decoded data contract emitted by the compressor. For JSONL-block outputs,
the integration layer keeps a small carry buffer for partial lines between
decoded blocks and submits complete lines to the existing stream-specific
parsers: trades, bookticker, and depth.

To simulate a live exchange, replay code should inject parsed events into the
same callback, queue, or adapter path used by live capture. Compression and file
layout decisions stay below that line.

## Candidate Matrix

Each stream family is ranked separately. The lab must report per-stream results,
not one global winner.

- Current baseline: JSONL blocks plus zstd (`std.zstd_jsonl_blocks_v1`).
- Binary row blocks: delta plus ZigZag plus VarInt.
- Columnar binary blocks: per-field arrays with delta or delta-delta coding.
- Arithmetic-coded bitstreams: `ac_bin16_ctx0`, `ac_bin16_ctx8`,
  `ac_bin16_ctx12`, `ac_bin32_ctx8`.
- Range coder: `range_ctx8`.
- rANS: `rans_ctx8`, later depth keyframe plus delta plus rANS.

## Entropy Target

For a symbol sequence `x_i` with model probability `p_i`, the target bit cost is:

```text
bits ~= sum(-log2(p_i))
```

Implementations use integer or fixed-point arithmetic only. Floating-point
operations are excluded from codec hot paths.

## Probability Models

Probability grids:

- 16-bit total frequency for compact arithmetic/range experiments.
- 32-bit total frequency for wider-range arithmetic experiments.

Contexts:

- `ctx0`: single global model.
- `ctx8`: previous byte or low 8-bit state context.
- `ctx12`: 12-bit mixed context for stronger local adaptation.

Adaptive counters use Laplace smoothing: initialize all active symbols with a
small nonzero count, update after each symbol, and rescale before totals exceed
the chosen grid. Coder comments should keep formulas next to the update logic:

```text
count[s] += 1
total += 1
p(s) = count[s] / total
```

## Arithmetic / Range / rANS Notes

Arithmetic and range coders maintain an integer interval or range and emit bits
or bytes during renormalization. Formula comments should sit next to interval
update, carry handling, and renormalization code:

```text
range = high - low + 1
high = low + floor(range * cum_high / total) - 1
low  = low + floor(range * cum_low  / total)
```

rANS maintains an integer state, encodes by pushing low bits during
renormalization, and decodes by mapping `state % total` through cumulative
frequencies. Formula comments should sit next to state update and renormalize:

```text
state = floor(state / freq) * total + (state % freq) + cum
```

## Ranking Requirements

A candidate can be ranked only after byte-exact or record-exact lossless
verification passes. Reports must include:

- compressed bytes and ratio;
- encode speed;
- decode speed;
- blocks and records/lines;
- stream family;
- format id, pipeline id, transform, entropy, version.

Baselines for comparison remain mandatory: zstd, lz4, brotli, and xz/lzma.

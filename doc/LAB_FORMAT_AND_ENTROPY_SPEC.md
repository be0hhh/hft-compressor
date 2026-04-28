# hft-compressor lab format and entropy spec

## Boundary

`hft-compressor` owns file layouts, transforms, entropy coders, block headers,
checksums, manifests, and artifact discovery. Consumers such as `src/replay`
pass a compressed root, session id or directory, stream/channel, and preference
into the public API. They must not construct paths such as `<channel>.hfc` or
depend on `.hfc`, zstd, range coder, rANS, arithmetic coding, block CRCs, or
container internals.

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

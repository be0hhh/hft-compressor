# HFT Compressor

[![cpp](https://github.com/be0hhh/hft-compressor/actions/workflows/cpp.yml/badge.svg?branch=main)](https://github.com/be0hhh/hft-compressor/actions/workflows/cpp.yml?query=branch%3Amain)

HFT Compressor is the app-layer compression library and command-line tool for
Recorder and research workflows. It provides lossless container, codec,
decode/verify, pipeline-registry, replay-decode and compression-metrics
capabilities. Available system libraries enable the zstd, LZ4, Brotli, XZ/LZMA
and gzip baselines alongside project-owned stream codecs.

## Build

From this directory in the canonical CXET checkout:

```bash
./compile.sh p
./compile.sh --force portable p
./compile.sh all p
./compile.sh all portable p
```

Builds use pinned Clang, GNU Make, Release `-O3`, native CPU targeting and
LTO OFF. `portable` disables CPU targeting; `--force` selects FULL LTO without
cleaning. Needed missing or stale dependencies rebuild automatically, including
without a terminal. `--accept-dependency-updates` remains accepted but redundant.

Ordinary products share `<CXET>/build/product` across owners. CPU, hot, exchange
and LTO changes reconfigure that cache; omitted options select defaults.
`CXET_BUILD_DIR` is the exact product-directory override. Fundamentally
incompatible identities reset only a verified selected product leaf before a
fresh build; unsafe or foreign paths refuse reset. Other build namespaces and
ordinary compilation failures do not trigger cleanup.

`all` builds and runs only this owner's registered tests and needed dependencies.
It uses `<CXET>/build/tests/<owner>` without configuring or activating a product.
`--force all` builds the product first and publishes its selection only after
all selected stages succeed. Other owners' tests are not selected; an empty
registry does not establish passing test proof.

Source-built launchers require a successful product marker and retain the
checkout-wide shared build lock through their owned runtime descendants.
A build or launch facing a busy lock refuses immediately; status/stop remain
available. Reset or code-generation-profile changes invalidate markers pointing
at the tree; a failed rebuild leaves those products unavailable. Old trees are preserved
until separately managed. See the root [build entrypoints](../../README.md#build-entrypoints)
and [launch exclusion](../../README.md#launch-and-build-exclusion) for the full
reset, activation and failure rules. `p` selects memory-aware parallelism;
positive `-j N` overrides it. See `--help` for supported options.


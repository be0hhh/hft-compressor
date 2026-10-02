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
./compile.sh all p
./compile.sh all portable p
```

The default incrementally builds this repository's production targets and their
full required dependency closure, including missing or stale foreign providers.
Ready targets are reused. `all` builds and runs only this repository's registered
tests and their required production dependencies; it does not select unrelated
production daemons, benchmarks or another repository's tests. If no
local tests are registered, the command reports that and succeeds.

Libraries are static `.a` archives. Release uses `-O3`, host-native CPU targeting
and full LTO; `portable` selects portable CPU targeting. Use `p` or `-j N` for
parallelism and `--help` for the supported options.


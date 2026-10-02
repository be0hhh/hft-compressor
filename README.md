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
./compile.sh --force p
./compile.sh all p
./compile.sh --force all portable p
```

The default builds only this repository's production targets and requires ready
prerequisites. `--force` cleans and rebuilds the exact required production
closure. `all` builds and runs only this repository's registered tests and their
support libraries; it does not build production daemons or benchmarks. With
`--force all`, production is rebuilt first, followed by the local tests. If no
local tests are registered, the command reports that and succeeds.

Libraries are static `.a` archives. Release uses `-O3`, host-native CPU targeting
and full LTO; `portable` selects portable CPU targeting. Use `p` or `-j N` for
parallelism and `--help` for the supported options.


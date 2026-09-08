# HFT Compressor

[![cpp](https://github.com/be0hhh/hft-compressor/actions/workflows/cpp.yml/badge.svg?branch=main)](https://github.com/be0hhh/hft-compressor/actions/workflows/cpp.yml?query=branch%3Amain)

HFT Compressor is the app-layer compression library and command-line tool for
Recorder and research workflows. It provides lossless container, codec,
decode/verify, pipeline-registry, replay-decode and compression-metrics
capabilities. Available system libraries enable the zstd, LZ4, Brotli, XZ/LZMA
and gzip baselines alongside project-owned stream codecs.

## Build

The local `compile.sh` builds the shared library on Linux/WSL. Keep this checkout
under `CXETCPP/apps/hft-compressor` and prepare the shared Clang 24 toolchain
from the CXET root with `python3 tools/Toolchain/BootstrapLlvm.py`.
Both entrypoints use the canonical `cmake/CxetToolchain.cmake` selector.
A complete standalone portable build with the CLI and offline tests can be
configured from this directory with:

```bash
/usr/bin/cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DHFT_COMPRESSOR_PORTABLE_BUILD=ON \
  -DHFT_COMPRESSOR_BUILD_CLI=ON \
  -DHFT_COMPRESSOR_BUILD_TESTS=ON
/usr/bin/cmake --build build --parallel "$(nproc)"
/usr/bin/ctest --test-dir build --output-on-failure --no-tests=error
```

The `cpp` workflow runs this pinned Clang configuration with all five system
codec libraries installed. It obtains the shared selector/bootstrap from the
private CXET root using `CI_CXETCPP_SSH_KEY`. It does not run compression
benchmarks.

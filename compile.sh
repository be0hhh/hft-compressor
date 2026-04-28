#!/usr/bin/env bash
# Build only the hft-compressor shared library for Linux/WSL.
# Result: apps/hft-compressor/build/libhft_compressor_core.so
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

if [[ "$(uname -s)" != Linux* ]]; then
  echo "hft-compressor compile.sh is Linux/WSL-only. Current: $(uname -s)" >&2
  exit 1
fi

if [[ "$(uname -m)" != x86_64 ]]; then
  echo "hft-compressor compile.sh expects x86_64. Current: $(uname -m)" >&2
  exit 1
fi

if ! command -v cmake >/dev/null 2>&1; then
  echo "cmake was not found in PATH" >&2
  exit 1
fi

if [[ -f build/CMakeCache.txt ]]; then
  CACHED_SOURCE="$(grep -E '^CMAKE_HOME_DIRECTORY:INTERNAL=' build/CMakeCache.txt 2>/dev/null | cut -d= -f2- || true)"
  if [[ -n "$CACHED_SOURCE" && "$CACHED_SOURCE" != "$SCRIPT_DIR" ]]; then
    BUILD_DIR="$(cd build && pwd)"
    if [[ "$BUILD_DIR" != "$SCRIPT_DIR/build" ]]; then
      echo "Refusing to remove unexpected build directory: $BUILD_DIR" >&2
      exit 1
    fi
    echo "Build was configured from a different path. Removing hft-compressor/build/ to reconfigure."
    rm -rf build
  fi
fi

cmake -S . -B build \
  -DCMAKE_BUILD_TYPE=Release \
  -DHFT_COMPRESSOR_BUILD_CLI=OFF \
  -DHFT_COMPRESSOR_BUILD_TESTS=OFF

cmake --build build --target hft_compressor_core --config Release

STABLE_SO="$SCRIPT_DIR/build/libhft_compressor_core.so"
BUILT_SO=""
for candidate in \
  "$SCRIPT_DIR/build/libhft_compressor_core.so" \
  "$SCRIPT_DIR/build/lib/libhft_compressor_core.so" \
  "$SCRIPT_DIR/build/Release/libhft_compressor_core.so"
do
  if [[ -f "$candidate" ]]; then
    BUILT_SO="$candidate"
    break
  fi
done

if [[ -z "$BUILT_SO" ]]; then
  BUILT_SO="$(find "$SCRIPT_DIR/build" -type f -name 'libhft_compressor_core.so' -print -quit)"
fi

if [[ -z "$BUILT_SO" || ! -f "$BUILT_SO" ]]; then
  echo "Build finished, but libhft_compressor_core.so was not found under hft-compressor/build/" >&2
  exit 1
fi

if [[ "$BUILT_SO" != "$STABLE_SO" ]]; then
  cp -f "$BUILT_SO" "$STABLE_SO"
fi

if [[ ! -s "$STABLE_SO" ]]; then
  echo "Stable library output is missing or empty: $STABLE_SO" >&2
  exit 1
fi

echo "Library build OK. libhft_compressor_core.so in apps/hft-compressor/build/"
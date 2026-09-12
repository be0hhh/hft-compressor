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

CXET_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
if ! selected_paths="$(cmake -P "$CXET_ROOT/cmake/CxetToolchain.cmake")"; then
  echo "Prepare the canonical CXET toolchain before using the family build wrapper." >&2
  exit 1
fi
mapfile -t selected_compilers <<< "$selected_paths"
if [[ ${#selected_compilers[@]} -ne 2 ]]; then
  echo "The canonical CXET compiler selector returned an invalid pair." >&2
  exit 1
fi
CLANG_CXX="${selected_compilers[1]}"

reset_build_state_preserving_dependency_cache() {
  local build_dir="$SCRIPT_DIR/build"
  if [[ ! -d "$build_dir" ]]; then
    return 0
  fi
  if [[ -L "$build_dir" ]]; then
    echo "Refusing to clean a symlinked build directory: $build_dir" >&2
    exit 1
  fi

  local resolved
  resolved="$(cd "$build_dir" && pwd -P)"
  if [[ "$resolved" != "$build_dir" ]]; then
    echo "Refusing to clean unexpected build directory: $resolved" >&2
    exit 1
  fi

  local deps_root="$build_dir/_deps"
  local dependency_dir populate_prefix stamp_dir
  if [[ -L "$deps_root" ]]; then
    echo "Refusing to clean a symlinked dependency root: $deps_root" >&2
    exit 1
  fi

  if [[ -d "$deps_root" ]]; then
    shopt -s nullglob
    for dependency_dir in "$deps_root"/*-build "$deps_root"/*-subbuild; do
      if [[ -L "$dependency_dir" ]]; then
        echo "Refusing to clean a symlinked dependency build directory: $dependency_dir" >&2
        exit 1
      fi
    done
    for dependency_dir in "$deps_root"/*-subbuild; do
      for populate_prefix in "$dependency_dir"/*-populate-prefix; do
        if [[ -L "$populate_prefix" ]]; then
          echo "Refusing to clean a symlinked dependency populate prefix: $populate_prefix" >&2
          exit 1
        fi
        if [[ -L "$populate_prefix/src" ]]; then
          echo "Refusing to clean a symlinked dependency populate source directory: $populate_prefix/src" >&2
          exit 1
        fi
      done
    done
    shopt -u nullglob
  fi

  find "$build_dir" -mindepth 1 -maxdepth 1 ! -name _deps -exec rm -rf -- {} +
  if [[ ! -d "$deps_root" ]]; then
    return 0
  fi

  shopt -s nullglob
  for dependency_dir in "$deps_root"/*-build; do
    rm -rf -- "$dependency_dir"
  done
  for dependency_dir in "$deps_root"/*-subbuild; do
    rm -rf -- \
      "$dependency_dir/CMakeCache.txt" \
      "$dependency_dir/CMakeFiles" \
      "$dependency_dir/CMakeLists.txt" \
      "$dependency_dir/Makefile" \
      "$dependency_dir/build.ninja" \
      "$dependency_dir/cmake_install.cmake" \
      "$dependency_dir/.ninja_deps" \
      "$dependency_dir/.ninja_log"
    for stamp_dir in "$dependency_dir"/*-populate-prefix/src/*-populate-stamp; do
      rm -rf -- "$stamp_dir"
    done
    rm -rf -- "$dependency_dir"/*-populate-prefix/tmp
  done
  shopt -u nullglob
}

if [[ -f build/CMakeCache.txt ]]; then
  CACHED_SOURCE="$(grep -E '^CMAKE_HOME_DIRECTORY:INTERNAL=' build/CMakeCache.txt 2>/dev/null | cut -d= -f2- || true)"
  CACHED_BUILD="$(grep -E '^CMAKE_CACHEFILE_DIR:INTERNAL=' build/CMakeCache.txt 2>/dev/null | cut -d= -f2- || true)"
  CACHED_CXX="$(grep -E '^CMAKE_CXX_COMPILER:FILEPATH=' build/CMakeCache.txt 2>/dev/null | cut -d= -f2- || true)"
  if [[ (-n "$CACHED_SOURCE" && "$CACHED_SOURCE" != "$SCRIPT_DIR") ||
        (-n "$CACHED_BUILD" && "$CACHED_BUILD" != "$SCRIPT_DIR/build") ]]; then
    echo "Build was configured from a different path. Resetting generated state while preserving dependency sources and downloads."
    reset_build_state_preserving_dependency_cache
  elif [[ -n "$CACHED_CXX" && "$CACHED_CXX" != "$CLANG_CXX" ]]; then
    echo "Build was configured with a non-Clang or different compiler. Resetting generated state while preserving dependency sources and downloads."
    reset_build_state_preserving_dependency_cache
  fi
fi

cmake -S . -B build \
  -DCMAKE_CXX_COMPILER="$CLANG_CXX" \
  -DCMAKE_BUILD_TYPE=Release \
  -DHFT_COMPRESSOR_PORTABLE_BUILD=OFF \
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

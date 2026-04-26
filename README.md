# hft-compressor

Sibling compression application/library for `apps/hft-recorder`.

MVP scope:
- input: one `trades.jsonl`, `bookticker.jsonl`, or `depth.jsonl` file
- codec: `zstd_jsonl_blocks_v1`
- output: `compressedData/zstd/sessions/<session>/<channel>.hfc`
- report: `compressedData/zstd/sessions/<session>/<channel>.metrics.json`

The compressed file may be loaded into memory by consumers. Decoding is block
streaming: one compressed block is decompressed into scratch memory, emitted to
the caller, and then discarded.


## Prebuilt library usage

`hft-compressor` is a standalone module. Build it separately and pass the
resulting library to `hft-recorder`; do not add this directory as a recorder
subdirectory.

Windows/MSVC:

```powershell
cmake -S apps/hft-compressor -B apps/hft-compressor/build
cmake --build apps/hft-compressor/build --config Release --target hft_compressor_core
```

This produces:

- `apps/hft-compressor/build/Release/hft_compressor_core.dll`
- `apps/hft-compressor/build/Release/hft_compressor_core.lib`

Linux/WSL:

```bash
cmake -S apps/hft-compressor -B apps/hft-compressor/build
cmake --build apps/hft-compressor/build --target hft_compressor_core
```

This produces `apps/hft-compressor/build/libhft_compressor_core.so` or the
platform generator's equivalent shared object path.

`hft-recorder` consumes the module through:

- `HFT_COMPRESSOR_PUBLIC_INCLUDE_DIR`
- `HFT_COMPRESSOR_SHARED_LIB`
- `HFT_COMPRESSOR_RUNTIME_LIB` on Windows when the runtime DLL is separate from the import lib
# Python codecs

Python codec implementations live next to the C++ codecs:

```text
src/codecs/python/
  gzip_codec.py
  bz2_codec.py
  lzma_codec.py
  zlib_codec.py
  brotli_codec.py
  zstandard_codec.py
  lz4_frame_codec.py
  common.py
  registry.py
  cli.py
```

The `python/hft_compressor` package is only a thin terminal/API wrapper over that
folder. It does not touch the C++ hot path.

Run from `apps/hft-compressor`:

```bash
PYTHONPATH=python python3 -m hft_compressor list
PYTHONPATH=python python3 -m hft_compressor compress ./session/trades.jsonl --codec py.gzip_v1 --output-root ./compressedData-python
PYTHONPATH=python python3 -m hft_compressor verify ./compressedData-python/py-gzip/sessions/session/trades.pylab.gz ./session/trades.jsonl --codec py.gzip_v1
```

You can also run the codec folder directly:

```bash
python3 src/codecs/python/cli.py list
```

Available without extra packages: `py.gzip_v1`, `py.bz2_v1`, `py.lzma_v1`, `py.zlib_v1`.
Optional: `py.brotli_v1`, `py.zstandard_v1`, `py.lz4_frame_v1`.


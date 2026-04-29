from __future__ import annotations

import pathlib
import sys
import tempfile

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "python"))

import hft_compressor

with tempfile.TemporaryDirectory() as tmp:
    root = pathlib.Path(tmp)
    session = root / "session_py"
    session.mkdir()
    input_path = session / "trades.jsonl"
    input_path.write_text("[1,2,1,100]\n[2,3,0,200]\n", encoding="utf-8")

    codecs = {codec["id"]: codec for codec in hft_compressor.list_codecs()}
    assert "py.gzip_v1" in codecs
    assert "py.bz2_v1" in codecs
    assert "py.lzma_v1" in codecs
    assert "py.zlib_v1" in codecs

    result = hft_compressor.compress_file(input_path, "py.gzip_v1", root / "compressed", level=6)
    assert result["ok"], result
    assert result["byte_exact"]
    assert result["record_exact"]

    verify = hft_compressor.verify_file(result["output_path"], input_path, "py.gzip_v1")
    assert verify["ok"], verify
    assert verify["byte_exact"]
    assert verify["record_exact"]

    decoded = hft_compressor.decode_file(result["output_path"], "py.gzip_v1")
    assert decoded == input_path.read_bytes()

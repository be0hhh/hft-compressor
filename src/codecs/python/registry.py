from __future__ import annotations

import importlib
from dataclasses import asdict
from typing import Any

from common import PythonCodecError, PythonCodecModule, VerifyMode, compress_file as _compress_file, decode_file as _decode_file, verify_file as _verify_file

_CODEC_MODULES = [
    "gzip_codec",
    "bz2_codec",
    "lzma_codec",
    "zlib_codec",
    "brotli_codec",
    "zstandard_codec",
    "lz4_frame_codec",
]


def _load_modules() -> list[PythonCodecModule]:
    codecs: list[PythonCodecModule] = []
    for name in _CODEC_MODULES:
        module = importlib.import_module(name)
        codecs.append(module.codec())
    return codecs


def list_codecs() -> list[dict[str, Any]]:
    return [asdict(codec.descriptor) for codec in _load_modules()]


def find_codec(codec_id: str, *, require_available: bool = True) -> PythonCodecModule:
    for codec in _load_modules():
        if codec.descriptor.id != codec_id:
            continue
        if require_available and not codec.descriptor.available:
            raise PythonCodecError(f"{codec_id} is unavailable: {codec.descriptor.availability_reason}")
        return codec
    raise PythonCodecError(f"unknown Python codec: {codec_id}")


def compress_file(input_path: str, codec_id: str, output_root: str, *, level: int = 6, verify_mode: VerifyMode = "both") -> dict[str, Any]:
    return _compress_file(find_codec(codec_id), input_path, output_root, level=level, verify_mode=verify_mode)


def decode_file(artifact_path: str, codec_id: str) -> bytes:
    return _decode_file(find_codec(codec_id), artifact_path)


def verify_file(artifact_path: str, canonical_path: str, codec_id: str, *, verify_mode: VerifyMode = "both") -> dict[str, Any]:
    return _verify_file(find_codec(codec_id), artifact_path, canonical_path, verify_mode=verify_mode)

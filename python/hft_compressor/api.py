from __future__ import annotations

import pathlib
import sys
from typing import Any


def _codecs_dir() -> pathlib.Path:
    return pathlib.Path(__file__).resolve().parents[2] / "src" / "codecs" / "python"


def _load_registry():
    codecs_dir = str(_codecs_dir())
    if codecs_dir not in sys.path:
        sys.path.insert(0, codecs_dir)
    import registry
    return registry


def list_codecs() -> list[dict[str, Any]]:
    return _load_registry().list_codecs()


def compress_file(input_path: str | pathlib.Path,
                  codec_id: str,
                  output_root: str | pathlib.Path,
                  *,
                  level: int = 6,
                  verify_mode: str = "both") -> dict[str, Any]:
    return _load_registry().compress_file(str(input_path), codec_id, str(output_root), level=level, verify_mode=verify_mode)


def verify_file(artifact_path: str | pathlib.Path,
                canonical_path: str | pathlib.Path,
                codec_id: str,
                *,
                verify_mode: str = "both") -> dict[str, Any]:
    return _load_registry().verify_file(str(artifact_path), str(canonical_path), codec_id, verify_mode=verify_mode)


def decode_file(artifact_path: str | pathlib.Path, codec_id: str) -> bytes:
    return _load_registry().decode_file(str(artifact_path), codec_id)

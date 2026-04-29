from __future__ import annotations

import json
import pathlib
import time
from dataclasses import dataclass
from typing import Any, Callable, Literal

VerifyMode = Literal["byte_exact", "record_exact", "both"]


class PythonCodecError(RuntimeError):
    pass


@dataclass(frozen=True)
class PythonCodecDescriptor:
    id: str
    label: str
    slug: str
    extension: str
    module: str
    available: bool
    availability_reason: str = ""
    ranked: bool = False
    lab_only: bool = True


@dataclass(frozen=True)
class PythonCodecModule:
    descriptor: PythonCodecDescriptor
    compress_bytes: Callable[[bytes, int], bytes]
    decompress_bytes: Callable[[bytes], bytes]


def stream_name(path: pathlib.Path) -> str:
    if path.name == "trades.jsonl":
        return "trades"
    if path.name == "bookticker.jsonl":
        return "bookticker"
    if path.name == "depth.jsonl":
        return "depth"
    return "unknown"


def session_id(path: pathlib.Path) -> str:
    return path.parent.name or f"manual_{path.stem}"


def output_path_for(input_path: pathlib.Path, output_root: pathlib.Path, codec: PythonCodecDescriptor) -> pathlib.Path:
    stream = stream_name(input_path)
    if stream == "unknown":
        stream = input_path.stem
    return output_root / codec.slug / "sessions" / session_id(input_path) / f"{stream}.pylab{codec.extension}"


def write_json(path: pathlib.Path, data: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")


def record_values(data: bytes, stream: str) -> list[Any]:
    records: list[Any] = []
    for line_no, raw_line in enumerate(data.splitlines(), start=1):
        line = raw_line.strip()
        if not line:
            continue
        try:
            value = json.loads(line)
        except json.JSONDecodeError as exc:
            raise PythonCodecError(f"json parse failed at line {line_no}: {exc}") from exc
        if stream == "trades":
            if not isinstance(value, list) or len(value) != 4 or value[2] not in (0, 1):
                raise PythonCodecError(f"invalid trades record at line {line_no}")
        elif stream == "bookticker":
            if not isinstance(value, list) or len(value) != 5:
                raise PythonCodecError(f"invalid bookticker record at line {line_no}")
        elif stream == "depth":
            if not isinstance(value, list) or len(value) < 2 or not isinstance(value[-1], int):
                raise PythonCodecError(f"invalid depth record at line {line_no}")
            for level in value[:-1]:
                if not isinstance(level, list) or len(level) < 3 or level[2] not in (0, 1):
                    raise PythonCodecError(f"invalid depth level at line {line_no}")
        else:
            raise PythonCodecError("record verification needs trades.jsonl, bookticker.jsonl, or depth.jsonl")
        records.append(value)
    return records


def verify_bytes(decoded: bytes, canonical: bytes, stream: str, verify_mode: VerifyMode) -> dict[str, Any]:
    byte_checked = verify_mode in ("byte_exact", "both")
    record_checked = verify_mode in ("record_exact", "both")
    byte_exact = byte_checked and decoded == canonical
    record_exact = False
    decoded_record_count = 0
    canonical_record_count = 0
    first_mismatch_field = ""

    if record_checked:
        decoded_records = record_values(decoded, stream)
        canonical_records = record_values(canonical, stream)
        decoded_record_count = len(decoded_records)
        canonical_record_count = len(canonical_records)
        record_exact = decoded_records == canonical_records
        if not record_exact:
            first_mismatch_field = "record"

    if byte_checked and not byte_exact and not first_mismatch_field:
        first_mismatch_field = "byte"

    if verify_mode == "byte_exact":
        ok = byte_exact
    elif verify_mode == "record_exact":
        ok = record_exact
    elif verify_mode == "both":
        ok = byte_exact and record_exact
    else:
        raise PythonCodecError("verify_mode must be byte_exact, record_exact, or both")

    return {
        "ok": ok,
        "verified": True,
        "byte_exact": byte_exact,
        "record_exact": record_exact,
        "decoded_record_count": decoded_record_count,
        "canonical_record_count": canonical_record_count,
        "mismatch_bytes": 0 if decoded == canonical else abs(len(decoded) - len(canonical)) or 1,
        "first_mismatch_field": first_mismatch_field,
    }


def compress_file(module: PythonCodecModule,
                  input_path: str | pathlib.Path,
                  output_root: str | pathlib.Path,
                  *,
                  level: int = 6,
                  verify_mode: VerifyMode = "both") -> dict[str, Any]:
    source = pathlib.Path(input_path)
    root = pathlib.Path(output_root)
    plain = source.read_bytes()
    stream = stream_name(source)
    target = output_path_for(source, root, module.descriptor)
    target.parent.mkdir(parents=True, exist_ok=True)

    encode_start = time.perf_counter_ns()
    compressed = module.compress_bytes(plain, level)
    encode_ns = time.perf_counter_ns() - encode_start
    target.write_bytes(compressed)

    decode_start = time.perf_counter_ns()
    decoded = module.decompress_bytes(compressed)
    decode_ns = time.perf_counter_ns() - decode_start
    verify = verify_bytes(decoded, plain, stream, verify_mode)

    metrics_path = target.with_suffix(target.suffix + ".metrics.json")
    result = {
        "status": "ok" if verify["ok"] else "verification_failed",
        "ok": verify["ok"],
        "lab_only": True,
        "ranked": False,
        "codec_id": module.descriptor.id,
        "label": module.descriptor.label,
        "module": module.descriptor.module,
        "stream": stream,
        "input_path": str(source),
        "output_path": str(target),
        "metrics_path": str(metrics_path),
        "input_bytes": len(plain),
        "output_bytes": len(compressed),
        "ratio": (len(plain) / len(compressed)) if compressed else 0.0,
        "encode_ns": encode_ns,
        "decode_ns": decode_ns,
        "verify_mode": verify_mode,
        **verify,
    }
    write_json(metrics_path, result)
    return result


def decode_file(module: PythonCodecModule, artifact_path: str | pathlib.Path) -> bytes:
    return module.decompress_bytes(pathlib.Path(artifact_path).read_bytes())


def verify_file(module: PythonCodecModule,
                artifact_path: str | pathlib.Path,
                canonical_path: str | pathlib.Path,
                *,
                verify_mode: VerifyMode = "both") -> dict[str, Any]:
    artifact = pathlib.Path(artifact_path)
    canonical = pathlib.Path(canonical_path)
    decode_start = time.perf_counter_ns()
    decoded = decode_file(module, artifact)
    decode_ns = time.perf_counter_ns() - decode_start
    canonical_bytes = canonical.read_bytes()
    verify = verify_bytes(decoded, canonical_bytes, stream_name(canonical), verify_mode)
    return {
        "status": "ok" if verify["ok"] else "verification_failed",
        "ok": verify["ok"],
        "lab_only": True,
        "ranked": False,
        "codec_id": module.descriptor.id,
        "artifact_path": str(artifact),
        "canonical_path": str(canonical),
        "decoded_bytes": len(decoded),
        "canonical_bytes": len(canonical_bytes),
        "decode_ns": decode_ns,
        "verify_mode": verify_mode,
        **verify,
    }

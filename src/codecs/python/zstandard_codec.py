from __future__ import annotations

from common import PythonCodecDescriptor, PythonCodecModule

try:
    import zstandard
except ModuleNotFoundError:
    zstandard = None

DESCRIPTOR = PythonCodecDescriptor(
    id="py.zstandard_v1",
    label="Python zstandard v1",
    slug="py-zstandard",
    extension=".zst",
    module="zstandard",
    available=zstandard is not None,
    availability_reason="pip install zstandard" if zstandard is None else "",
)


def compress_bytes(data: bytes, level: int) -> bytes:
    if zstandard is None:
        raise RuntimeError(DESCRIPTOR.availability_reason)
    return zstandard.ZstdCompressor(level=max(1, min(level, 22))).compress(data)


def decompress_bytes(data: bytes) -> bytes:
    if zstandard is None:
        raise RuntimeError(DESCRIPTOR.availability_reason)
    return zstandard.ZstdDecompressor().decompress(data)


def codec() -> PythonCodecModule:
    return PythonCodecModule(DESCRIPTOR, compress_bytes, decompress_bytes)

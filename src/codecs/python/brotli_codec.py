from __future__ import annotations

from common import PythonCodecDescriptor, PythonCodecModule

try:
    import brotli
except ModuleNotFoundError:
    brotli = None

DESCRIPTOR = PythonCodecDescriptor(
    id="py.brotli_v1",
    label="Python brotli v1",
    slug="py-brotli",
    extension=".br",
    module="brotli",
    available=brotli is not None,
    availability_reason="pip install brotli" if brotli is None else "",
)


def compress_bytes(data: bytes, level: int) -> bytes:
    if brotli is None:
        raise RuntimeError(DESCRIPTOR.availability_reason)
    return brotli.compress(data, quality=max(0, min(level, 11)))


def decompress_bytes(data: bytes) -> bytes:
    if brotli is None:
        raise RuntimeError(DESCRIPTOR.availability_reason)
    return brotli.decompress(data)


def codec() -> PythonCodecModule:
    return PythonCodecModule(DESCRIPTOR, compress_bytes, decompress_bytes)

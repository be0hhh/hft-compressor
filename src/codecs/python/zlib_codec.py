from __future__ import annotations

import zlib

from common import PythonCodecDescriptor, PythonCodecModule

DESCRIPTOR = PythonCodecDescriptor(
    id="py.zlib_v1",
    label="Python zlib v1",
    slug="py-zlib",
    extension=".zz",
    module="zlib",
    available=True,
)


def compress_bytes(data: bytes, level: int) -> bytes:
    return zlib.compress(data, level=max(1, min(level, 9)))


def decompress_bytes(data: bytes) -> bytes:
    return zlib.decompress(data)


def codec() -> PythonCodecModule:
    return PythonCodecModule(DESCRIPTOR, compress_bytes, decompress_bytes)

from __future__ import annotations

import gzip

from Common import PythonCodecDescriptor, PythonCodecModule

DESCRIPTOR = PythonCodecDescriptor(
    id="py.gzip",
    label="Python gzip",
    slug="py-gzip",
    extension=".gz",
    module="gzip",
    available=True,
)


def compress_bytes(data: bytes, level: int) -> bytes:
    return gzip.compress(data, compresslevel=max(1, min(level, 9)), mtime=0)


def decompress_bytes(data: bytes) -> bytes:
    return gzip.decompress(data)


def codec() -> PythonCodecModule:
    return PythonCodecModule(DESCRIPTOR, compress_bytes, decompress_bytes)

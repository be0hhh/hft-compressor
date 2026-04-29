from __future__ import annotations

import lzma

from common import PythonCodecDescriptor, PythonCodecModule

DESCRIPTOR = PythonCodecDescriptor(
    id="py.lzma_v1",
    label="Python lzma v1",
    slug="py-lzma",
    extension=".xz",
    module="lzma",
    available=True,
)


def compress_bytes(data: bytes, level: int) -> bytes:
    return lzma.compress(data, preset=max(0, min(level, 9)))


def decompress_bytes(data: bytes) -> bytes:
    return lzma.decompress(data)


def codec() -> PythonCodecModule:
    return PythonCodecModule(DESCRIPTOR, compress_bytes, decompress_bytes)

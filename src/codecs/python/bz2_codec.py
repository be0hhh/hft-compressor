from __future__ import annotations

import bz2

from common import PythonCodecDescriptor, PythonCodecModule

DESCRIPTOR = PythonCodecDescriptor(
    id="py.bz2_v1",
    label="Python bz2 v1",
    slug="py-bz2",
    extension=".bz2",
    module="bz2",
    available=True,
)


def compress_bytes(data: bytes, level: int) -> bytes:
    return bz2.compress(data, compresslevel=max(1, min(level, 9)))


def decompress_bytes(data: bytes) -> bytes:
    return bz2.decompress(data)


def codec() -> PythonCodecModule:
    return PythonCodecModule(DESCRIPTOR, compress_bytes, decompress_bytes)

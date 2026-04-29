from __future__ import annotations

from common import PythonCodecDescriptor, PythonCodecModule

try:
    import lz4.frame
except ModuleNotFoundError:
    lz4 = None
else:
    import lz4

DESCRIPTOR = PythonCodecDescriptor(
    id="py.lz4_frame_v1",
    label="Python lz4.frame v1",
    slug="py-lz4-frame",
    extension=".lz4",
    module="lz4.frame",
    available=lz4 is not None,
    availability_reason="pip install lz4" if lz4 is None else "",
)


def compress_bytes(data: bytes, level: int) -> bytes:
    if lz4 is None:
        raise RuntimeError(DESCRIPTOR.availability_reason)
    return lz4.frame.compress(data, compression_level=max(0, min(level, 16)))


def decompress_bytes(data: bytes) -> bytes:
    if lz4 is None:
        raise RuntimeError(DESCRIPTOR.availability_reason)
    return lz4.frame.decompress(data)


def codec() -> PythonCodecModule:
    return PythonCodecModule(DESCRIPTOR, compress_bytes, decompress_bytes)

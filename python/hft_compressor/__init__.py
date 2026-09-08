"""Thin terminal/API wrapper over codecs stored in src/Runtime/src/Codecs/Python."""

from .Api import compress_file, decode_file, list_codecs, verify_file

__all__ = ["compress_file", "decode_file", "list_codecs", "verify_file"]

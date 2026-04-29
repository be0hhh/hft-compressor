"""Thin terminal/API wrapper over codecs stored in src/codecs/python."""

from .api import compress_file, decode_file, list_codecs, verify_file

__all__ = ["compress_file", "decode_file", "list_codecs", "verify_file"]

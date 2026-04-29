from __future__ import annotations

import argparse
import json
import sys

from .api import compress_file, decode_file, list_codecs, verify_file


def print_json(value) -> None:
    print(json.dumps(value, ensure_ascii=False, indent=2, default=str))


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="python -m hft_compressor")
    sub = parser.add_subparsers(dest="cmd", required=True)

    sub.add_parser("list")

    p_compress = sub.add_parser("compress")
    p_compress.add_argument("input_path")
    p_compress.add_argument("--codec", required=True)
    p_compress.add_argument("--output-root", required=True)
    p_compress.add_argument("--level", type=int, default=6)
    p_compress.add_argument("--mode", default="both", choices=["byte_exact", "record_exact", "both"])

    p_verify = sub.add_parser("verify")
    p_verify.add_argument("artifact_path")
    p_verify.add_argument("canonical_path")
    p_verify.add_argument("--codec", required=True)
    p_verify.add_argument("--mode", default="both", choices=["byte_exact", "record_exact", "both"])

    p_decode = sub.add_parser("decode")
    p_decode.add_argument("artifact_path")
    p_decode.add_argument("--codec", required=True)

    args = parser.parse_args(argv)
    if args.cmd == "list":
        print_json(list_codecs())
        return 0
    if args.cmd == "compress":
        result = compress_file(args.input_path, args.codec, args.output_root, level=args.level, verify_mode=args.mode)
        print_json(result)
        return 0 if result.get("ok") else 1
    if args.cmd == "verify":
        result = verify_file(args.artifact_path, args.canonical_path, args.codec, verify_mode=args.mode)
        print_json(result)
        return 0 if result.get("ok") else 1
    if args.cmd == "decode":
        sys.stdout.buffer.write(decode_file(args.artifact_path, args.codec))
        return 0
    return 2


if __name__ == "__main__":
    raise SystemExit(main())

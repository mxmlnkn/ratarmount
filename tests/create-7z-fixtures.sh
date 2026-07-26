#!/usr/bin/env bash
# Generate 7z fixtures used by core/tests/test_sevenzip.py.
# Prefers the system 7z/7zz/7za CLI when available; otherwise uses py7zr.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")" && pwd)"
OUT="${1:-$ROOT}"
mkdir -p "$OUT"

echo "Output directory: $OUT"

python3 - <<'PY' "$OUT"
import sys
from pathlib import Path

out = Path(sys.argv[1])
td = out / "_fixture_src"
td.mkdir(exist_ok=True)

(td / "a.txt").write_bytes(b"AAA" + b"a" * 200 + b"AAA")
(td / "b.txt").write_bytes(b"BBB" + b"b" * 200 + b"BBB")
(td / "medium.bin").write_bytes(b"X" * (2 * 1024 * 1024))
(td / "secret.txt").write_bytes(b"secret content\n")
(td / "hello.txt").write_bytes(b"hello from nested\n")

try:
    import py7zr
    from py7zr.properties import FILTER_COPY, FILTER_LZMA2
except ImportError as exc:
    raise SystemExit("py7zr is required to generate fixtures: pip install 'py7zr~=1.0'") from exc

store = out / "store-copy-two-files.7z"
with py7zr.SevenZipFile(store, "w", filters=[{"id": FILTER_COPY}]) as z:
    z.write(td / "a.txt", "a.txt")
    z.write(td / "b.txt", "b.txt")

lzma = out / "lzma2-two-files-and-medium.7z"
with py7zr.SevenZipFile(lzma, "w", filters=[{"id": FILTER_LZMA2, "preset": 3}]) as z:
    z.write(td / "a.txt", "a.txt")
    z.write(td / "b.txt", "b.txt")
    z.write(td / "medium.bin", "medium.bin")

enc = out / "encrypted-hello.7z"
with py7zr.SevenZipFile(enc, "w", password="secret", header_encryption=False) as z:
    z.write(td / "secret.txt", "secret.txt")

inner = td / "inner-hello.7z"
with py7zr.SevenZipFile(inner, "w", filters=[{"id": FILTER_COPY}]) as z:
    z.write(td / "hello.txt", "hello.txt")
outer = out / "nested-inner-hello.7z"
with py7zr.SevenZipFile(outer, "w", filters=[{"id": FILTER_COPY}]) as z:
    z.write(inner, "inner-hello.7z")

inner_enc = td / "inner-encrypted.7z"
with py7zr.SevenZipFile(inner_enc, "w", password="innerpw", header_encryption=False) as z:
    z.write(td / "secret.txt", "payload.txt")
outer_enc = out / "nested-encrypted-inner.7z"
with py7zr.SevenZipFile(outer_enc, "w", filters=[{"id": FILTER_COPY}]) as z:
    z.write(inner_enc, "inner-encrypted.7z")

print("Created:")
for p in (store, lzma, enc, outer, outer_enc):
    print(f"  {p.name:40} {p.stat().st_size:8} bytes")
PY

# Optional non-solid store via CLI (extra coverage when 7z is installed)
if command -v 7z >/dev/null 2>&1 || command -v 7zz >/dev/null 2>&1 || command -v 7za >/dev/null 2>&1; then
  SEVEN=$(command -v 7z || command -v 7zz || command -v 7za)
  SRC="$OUT/_fixture_src"
  "$SEVEN" a -t7z -mx=0 -ms=off "$OUT/store-nonsolid-cli.7z" "$SRC/a.txt" "$SRC/b.txt" >/dev/null
  echo "  store-nonsolid-cli.7z (via $SEVEN)"
fi

echo "Done."

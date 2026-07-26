#!/usr/bin/env python3
"""Benchmark 7z open/seek performance across backends.

Usage:
  python benchmarks/benchmark-7z-random-access.py [archive.7z ...]

If no archives are given, uses tests/two-large-files-32Ki-lines-each-1024B.7z
and tests/folder-symlink.7z when present.

Metrics (per backend that can open the archive):
  - index/mount time
  - open mid-list file
  - pread 4 KiB at start / middle / end of largest member
  - backward seek (end -> start)
"""

from __future__ import annotations

import argparse
import os
import statistics
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "core"))

from ratarmountcore.mountsource.formats.libarchive import LibarchiveMountSource  # noqa: E402
from ratarmountcore.mountsource.formats.sevenzip import SevenZipMountSource  # noqa: E402


def _time_call(fn, repeats: int = 1) -> float:
    samples = []
    for _ in range(repeats):
        t0 = time.perf_counter()
        fn()
        samples.append(time.perf_counter() - t0)
    return statistics.median(samples)


def _iter_files(mount_source):
    import stat

    files = []

    def walk(path: str):
        listing = mount_source.list(path)
        if not listing:
            return
        # list() may return dict[str, FileInfo] or list of names depending on backend version.
        if isinstance(listing, dict):
            items = listing.items()
        else:
            items = ((name, mount_source.lookup(f"{path.rstrip('/')}/{name}" if path != "/" else f"/{name}")) for name in listing)
        for name, info in items:
            if info is None:
                continue
            child = f"{path.rstrip('/')}/{name}" if path != "/" else f"/{name}"
            if stat.S_ISDIR(info.mode):
                walk(child)
            elif not stat.S_ISLNK(info.mode):
                files.append((child, info))

    walk("/")
    return files


def bench_backend(name: str, ctor, archive: str) -> dict:
    result = {"backend": name, "archive": archive}
    try:
        t0 = time.perf_counter()
        mount_source = ctor(archive, indexFilePath=":memory:")
        result["mount_s"] = time.perf_counter() - t0
    except Exception as exception:
        result["error"] = f"mount failed: {exception}"
        return result

    try:
        files = _iter_files(mount_source)
        if not files:
            result["error"] = "no regular files"
            return result

        mid = files[min(1, len(files) - 1)]
        large = max(files, key=lambda item: item[1].size)

        path, info = mid
        size_mid = info.size

        # Cold open (first touch after mount) — important for stream codecs.
        try:
            t0 = time.perf_counter()
            with mount_source.open(info) as file:
                file.read(1)
            result["open_mid_cold_s"] = time.perf_counter() - t0
        except Exception as exception:
            result["open_mid_cold_s"] = float("nan")
            result["open_error"] = str(exception)

        def open_mid():
            with mount_source.open(info) as file:
                file.read(1)

        try:
            result["open_mid_warm_s"] = _time_call(open_mid, repeats=3)
        except Exception as exception:
            result["open_mid_warm_s"] = float("nan")
            result["open_error"] = str(exception)

        path, info = large
        size = info.size
        if size > 0:
            # Cold mid pread on a fresh mount would require a new ctor; measure first
            # pread_mid after the opens above (partially warm). Also measure warm.
            for key, offset in (
                ("pread_start_s", 0),
                ("pread_mid_s", max(0, size // 2)),
                ("pread_end_s", max(0, size - 4096)),
            ):

                def pread(off=offset, file_info=info):
                    with mount_source.open(file_info) as file:
                        file.seek(off)
                        file.read(4096)

                try:
                    # First call after any prior work
                    t0 = time.perf_counter()
                    pread()
                    first = time.perf_counter() - t0
                    warm = _time_call(pread, repeats=3)
                    result[key] = warm
                    result[key.replace("_s", "_first_s")] = first
                except Exception:
                    result[key] = float("nan")

            def backward(file_info=info, file_size=size):
                with mount_source.open(file_info) as file:
                    file.seek(max(0, file_size - 4096))
                    file.read(4096)
                    file.seek(0)
                    file.read(4096)

            try:
                result["seek_back_s"] = _time_call(backward, repeats=3)
            except Exception:
                result["seek_back_s"] = float("nan")
            result["largest_member"] = path
            result["largest_size"] = size
    except Exception as exception:
        result["error"] = str(exception)
    finally:
        try:
            mount_source.close()
        except Exception:
            pass
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archives", nargs="*", help="7z archives to benchmark")
    parser.add_argument("--skip-libarchive", action="store_true")
    parser.add_argument("--skip-py7zr", action="store_true")
    args = parser.parse_args()

    archives = [Path(a) for a in args.archives]
    if not archives:
        for rel in (
            "tests/two-large-files-32Ki-lines-each-1024B.7z",
            "tests/folder-symlink.7z",
            "tests/nested-with-symlink.7z",
        ):
            path = ROOT / rel
            if path.is_file():
                archives.append(path)

    if not archives:
        print("No archives found.", file=sys.stderr)
        return 1

    backends = [("sevenzip", SevenZipMountSource)]
    if not args.skip_libarchive:
        backends.append(("libarchive", LibarchiveMountSource))
    if not args.skip_py7zr:
        try:
            from ratarmountcore.mountsource.formats.py7zr import Py7zrMountSource, py7zr

            if py7zr is not None:
                backends.append(("py7zr", Py7zrMountSource))
        except Exception:
            pass

    print(
        f"{'archive':40} {'backend':12} {'mount':>8} "
        f"{'open_cold':>9} {'open_warm':>9} {'mid_first':>9} {'mid_warm':>9} {'seek_back':>9}"
    )
    for archive in archives:
        for name, ctor in backends:
            row = bench_backend(name, ctor, str(archive))
            if "error" in row:
                print(f"{archive.name:40} {name:12} ERROR {row['error']}")
                continue
            print(
                f"{archive.name:40} {name:12} "
                f"{row.get('mount_s', float('nan')):8.4f} "
                f"{row.get('open_mid_cold_s', float('nan')):9.4f} "
                f"{row.get('open_mid_warm_s', float('nan')):9.4f} "
                f"{row.get('pread_mid_first_s', float('nan')):9.4f} "
                f"{row.get('pread_mid_s', float('nan')):9.4f} "
                f"{row.get('seek_back_s', float('nan')):9.4f}"
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

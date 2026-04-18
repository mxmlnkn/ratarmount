#!/usr/bin/env python3
import os
import struct
import subprocess
import sys
import tempfile
from pathlib import Path

MARKER = b"PYTHON_ARGCOMPLETE_OK"
LIMIT = 1024
PT_LOAD = 1
SHT_NOBITS = 8


def run(*cmd, env=None, pass_fds=(), timeout=60):
    p = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        env=env,
        pass_fds=pass_fds,
        timeout=timeout,
        check=True,
    )
    if p.returncode != 0:
        raise SystemExit(f"FAILED: {' '.join(cmd)}\nstdout:\n{p.stdout}\nstderr:\n{p.stderr}")
    return p.stdout


def main():
    if len(sys.argv) not in (2, 3):
        raise SystemExit(f"usage: {sys.argv[0]} APPIMAGE [CMD_NAME]")

    path = Path(sys.argv[1])
    cmd_name = sys.argv[2] if len(sys.argv) == 3 else path.name
    blob = path.read_bytes()

    if blob[:4] != b"\x7fELF":
        raise RuntimeError("not an ELF file")
    if blob[4] != 2 or blob[5] != 1:
        raise RuntimeError("expected ELF64 little-endian")

    # ph: program header, sh: section header
    e_phoff = struct.unpack_from("<Q", blob, 32)[0]
    e_shoff = struct.unpack_from("<Q", blob, 40)[0]
    e_ehsize, e_phentsize, e_phnum = struct.unpack_from("<HHH", blob, 52)
    e_shentsize, e_shnum = struct.unpack_from("<HH", blob, 58)
    if e_ehsize != 64 or e_phentsize < 56 or (e_shnum and e_shentsize < 64):
        raise RuntimeError("unexpected ELF header/program/section header sizes")

    ph_end = e_phoff + e_phentsize * e_phnum
    first_used_after_ph = len(blob)

    # Find the earliest file-backed payload offset at/after the program header table.
    for i in range(e_phnum):
        off = e_phoff + i * e_phentsize
        p_offset = struct.unpack_from("<Q", blob, off + 8)[0]
        p_filesize = struct.unpack_from("<Q", blob, off + 32)[0]
        if p_filesize and p_offset >= ph_end:
            first_used_after_ph = min(first_used_after_ph, p_offset)

    for i in range(e_shnum):
        off = e_shoff + i * e_shentsize
        sh_type = struct.unpack_from("<I", blob, off + 4)[0]
        sh_offset = struct.unpack_from("<Q", blob, off + 24)[0]
        sh_size = struct.unpack_from("<Q", blob, off + 32)[0]
        if sh_type != SHT_NOBITS and sh_size and sh_offset >= ph_end:
            first_used_after_ph = min(first_used_after_ph, sh_offset)

    print("First data use after program header:", first_used_after_ph)
    safe_limit = min(LIMIT, first_used_after_ph)
    if ph_end + len(MARKER) > safe_limit:
        raise RuntimeError("program header table leaves no safe room before used ELF data or byte 1024")

    before_file = run("file", str(path))
    before_readelf = run("readelf", "-e", str(path))

    # Patch the marker only into bytes before the first known file-backed ELF payload.
    if MARKER not in blob[:LIMIT]:
        pos = None
        for i in range(ph_end + e_phentsize, safe_limit - len(MARKER) + 1, e_phentsize):
            if all(b == 0 for b in blob[i : i + len(MARKER)]):
                pos = i
                break
        if pos is None:
            raise RuntimeError("no zero-filled patch window before used ELF data or byte 1024")

        patched = bytearray(blob)
        patched[pos : pos + len(MARKER)] = MARKER
        path.write_bytes(patched)
        blob = bytes(patched)
        print(f"Patched marker into offset: {pos}")

    if MARKER not in blob[:LIMIT]:
        raise RuntimeError("marker missing after patch")

    # Structural checks
    after_file = run("file", str(path))
    if before_file != after_file:
        print("== before ==")
        sys.stdout.write(before_file)
        print("== after ==")
        sys.stdout.write(after_file)
        raise RuntimeError("file output changed after patch")

    after_readelf = run("readelf", "-e", str(path))
    if before_readelf != after_readelf:
        print("== before ==")
        sys.stdout.write(before_file)
        print("== after ==")
        sys.stdout.write(after_file)
        raise RuntimeError("readelf output changed after patch")

    run(f"./{path}", "--help")

    # Argcomplete test: completions go to fd 8
    env = os.environ.copy()
    env["_ARGCOMPLETE"] = "1"
    env["_ARGCOMPLETE_SUPPRESS_SPACE"] = "1"
    env["_ARGCOMPLETE_SHELL"] = "bash"
    env["_ARGCOMPLETE_SUPPRESS_SPACE"] = "1"
    env["_ARGCOMPLETE_IFS"] = "\t"
    env["_ARGCOMPLETE_COMP_WORDBREAKS"] = " \t\n\"'><=;|&(:"
    env["COMP_LINE"] = f"{cmd_name} --"
    env["COMP_POINT"] = str(len(env["COMP_LINE"]))
    # env["_ARC_DEBUG"] = "1"
    ARGCOMPLETE_OUTPUT_FD = 8  # by convention

    with tempfile.TemporaryFile() as fd8:
        original_fd = fd8.fileno()

        def map_fd_to_8():
            # argcomplete writes to file descriptor 8 by convention.
            if original_fd != ARGCOMPLETE_OUTPUT_FD:
                os.dup2(original_fd, ARGCOMPLETE_OUTPUT_FD)

        print(f"Argcomplete capture fd mapping: parent fd={original_fd} -> child fd={ARGCOMPLETE_OUTPUT_FD}")
        p = subprocess.run(
            [f"./{path}", "--"],
            capture_output=True,
            text=True,
            env=env,
            pass_fds=(original_fd, ARGCOMPLETE_OUTPUT_FD),
            preexec_fn=map_fd_to_8,
            timeout=60,
            check=True,
        )

        fd8.seek(0)
        out = fd8.read()

        # It is expected to return 1 because the mount source is not specified.
        if p.returncode != 0:
            sys.stdout.buffer.write(out)
            raise RuntimeError(
                f"argcomplete test failed with rc={p.returncode}\nstdout:\n{p.stdout}\nstderr:\n{p.stderr}"
            )
        if "Traceback" in p.stdout or "Traceback" in p.stderr:
            raise RuntimeError("traceback during argcomplete test")
        if "error: the following arguments are required" in p.stderr:
            raise RuntimeError("argcomplete test fell through to argparse")

        if isinstance(out, bytes):
            out = out.decode("utf-8", errors="replace")
        if not out.strip():
            raise RuntimeError("argcomplete test produced no completions on fd 8")


if __name__ == "__main__":
    main()

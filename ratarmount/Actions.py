#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import importlib
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import time
import urllib.request
import zipfile
from typing import List, Optional

from ratarmountcore.compressions import supportedCompressions, stripSuffixFromTarFile
from ratarmountcore.utils import imeta, findModuleVersion, removeDuplicatesStable, RatarmountError
import ratarmountcore.version

try:
    import rarfile
except ImportError:
    pass

from .fuse import fuse
from .version import __version__
from .CLIHelpers import checkInputFileType
from .WriteOverlay import commitOverlay


def hasFUSENonEmptySupport() -> bool:
    try:
        # Check suffix of shared library
        if 'fuse' in globals() and getattr(fuse, '_libfuse_path', '').endswith(".so.2"):
            return True

        # Note that in Ubuntu 22.04 libfuse3 and libfuse2 can be installed side-by-side with fusermount 3 being
        # detected with precedence even though fusepy will use libfuse-2.9.9.
        with os.popen('fusermount -V') as pipe:
            match = re.search(r'([0-9]+)[.][0-9]+[.][0-9]+', pipe.read())
            if match:
                return int(match.group(1)) < 3
    except Exception:
        pass

    return False  # On macOS, fusermount does not exist and macfuse also seems to complain with nonempty option.


def printVersions() -> None:
    print("ratarmount", __version__)
    print("ratarmountcore", ratarmountcore.version.__version__)

    print()
    print("System Software:")
    print()
    print("Python", sys.version.split(' ', maxsplit=1)[0])

    try:
        fusermountVersion = subprocess.run(
            ["fusermount", "--version"], stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=False
        ).stdout.strip()
        print("fusermount", re.sub('.* ([0-9][.][0-9.]+).*', r'\1', fusermountVersion.decode()))
    except Exception:
        pass

    if hasattr(fuse, 'fuse_version_major') and hasattr(fuse, 'fuse_version_minor'):
        print(f"FUSE: {fuse.fuse_version_major}.{fuse.fuse_version_minor}")

    print("libsqlite3", sqlite3.sqlite_version)

    print()
    print("Compression Backends:")
    print()

    def printModuleVersion(moduleName: str):
        try:
            importlib.import_module(moduleName)
        except ImportError:
            pass

        moduleVersion: Optional[str] = None
        if moduleName in sys.modules:
            moduleVersion = findModuleVersion(sys.modules[moduleName])
        else:
            try:
                # May raise importlib.metadata.PackageNotFoundError
                moduleVersion = imeta.version(moduleName)
            except Exception:
                pass
        if moduleVersion:
            print(moduleName, moduleVersion)

    modules = [module.name for _, info in supportedCompressions.items() for module in info.modules]
    # Indirect dependencies for PySquashfsImage and other things.
    modules += ["lz4", "python-lzo", "zstandard", "isal", "fast_zip_decryption", "pygit2"]
    for moduleName in sorted(list(set(modules))):
        printModuleVersion(moduleName)

    print()
    print("Fsspec Backends:")
    print()

    # fmt: off
    modules = [
        "fsspec",
        "sshfs",
        "smbprotocol",
        "dropboxdrivefs",
        "ipfsspec",
        "s3fs",
        "webdav4",
        # Indirect dependencies. Would be nice to be able to get this programmatically but
        # this might be too much to ask for.
        "asyncssh",         # sshfs
        "requests",
        "aiohttp",          # httpfs, s3fs, ...
        "pyopenssl",        # sshfs
        "cryptography",     # smbprotocol
        "pyspnego",         # smbprotocol
        "dropbox",
        "multiformats",
        "dag-cbor",         # ipfsspec
        "pure-protobuf",
        "aiobotocore",      # s3fs
        "httpx",            # webdav4
        "python-dateutil",  # webdav4
    ]
    # fmt: on
    for moduleName in sorted(list(modules)):
        printModuleVersion(moduleName)

    mappedFilesFolder = f"/proc/{os.getpid()}/map_files"
    if os.path.isdir(mappedFilesFolder):
        libraries = set(os.readlink(os.path.join(mappedFilesFolder, link)) for link in os.listdir(mappedFilesFolder))
        # Only look for shared libraries with versioning suffixed. Ignore all ending on .so.
        libraries = set(library for library in libraries if '.so.' in library)

        if libraries:
            print()
            print("Versioned Loaded Shared Libraries:")
            print()

        for library in sorted(list(libraries)):
            print(library.rsplit('/', maxsplit=1)[-1])


def printOSSAttributions() -> None:
    licenses = [
        ("fusepy", "/fusepy/fusepy/master/LICENSE"),  # ISC
        ("python-xz", "/Rogdham/python-xz/master/LICENSE.txt"),  # MIT
        ("rarfile", "/markokr/rarfile/master/LICENSE"),  # ISC
        ("libfuse", "/libfuse/libfuse/master/LGPL2.txt"),  # LGPL 2.1
        ("libsqlite3", "/sqlite/sqlite/master/LICENSE.md"),  # "The author disclaims copyright to this source code"
        ("cpython", "/python/cpython/main/LICENSE"),  # PYTHON SOFTWARE FOUNDATION LICENSE VERSION 2
        ("libzstd-seek", "/martinellimarco/libzstd-seek/main/LICENSE"),  # MIT
        ("zstd", "/facebook/zstd/dev/LICENSE"),  # BSD-3 with "name of the copyright holder" explicitly filled in
        ("zlib", "/madler/zlib/master/LICENSE"),  # zlib License
        ("ratarmountcore", "/mxmlnkn/ratarmount/master/core/LICENSE"),  # MIT
        ("indexed_gzip", "/pauldmccarthy/indexed_gzip/master/LICENSE"),  # zlib License
        ("indexed_zstd", "/martinellimarco/indexed_zstd/master/LICENSE"),  # MIT
        ("rapidgzip", "/mxmlnkn/rapidgzip/master/LICENSE-MIT"),  # MIT or Apache License 2.0
        ("fast-zip-decryption", "/mxmlnkn/fast-zip-decryption/refs/heads/master/LICENSE"),  # MIT
        ("fsspec", "/fsspec/filesystem_spec/refs/heads/master/LICENSE"),  # BSD-3
        ("sshfs", "/fsspec/sshfs/refs/heads/main/LICENSE"),  # Apache License 2.0
        ("ipfsspec", "/fsspec/ipfsspec/refs/heads/main/LICENSE"),  # MIT
        ("smbprotocol", "/jborean93/smbprotocol/refs/heads/master/LICENSE"),  # MIT
        ("dropboxdrivefs", "/fsspec/dropboxdrivefs/refs/heads/master/LICENSE"),  # BSD-3
        ("s3fs", "/fsspec/s3fs/refs/heads/main/LICENSE.txt"),  # BSD-3
        ("webdav4", "/skshetry/webdav4/refs/heads/main/LICENSE"),  # MIT
        ("asyncssh", "/ronf/asyncssh/refs/heads/develop/LICENSE"),  # EPL 2.0
    ]
    for name, githubPath in sorted(licenses):
        licenseUrl = "https://raw.githubusercontent.com" + githubPath
        try:
            licenseContents = urllib.request.urlopen(licenseUrl).read().decode()
        except urllib.error.HTTPError as error:
            licenseContents = f"Failed to get license at {licenseUrl} because of: {str(error)}"
        homepage = "https://github.com" + '/'.join(githubPath.split('/', 3)[:3])
        print(f"# {name}\n\n{homepage}\n\n\n```\n{licenseContents}\n```\n\n")


def unmount(mountPoint: str, printDebug: int = 0) -> None:
    # Do not test with os.path.ismount or anything other because if the FUSE process was killed without
    # unmounting, then any file system query might return with errors.
    # https://github.com/python/cpython/issues/96328#issuecomment-2027458283

    try:
        subprocess.run(["fusermount", "-u", mountPoint], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        if printDebug >= 2:
            print("[Info] Successfully called fusermount -u.")
        return
    except Exception as exception:
        if printDebug >= 2:
            print(f"[Warning] fusermount -u {mountPoint} failed with: {exception}")
        if printDebug >= 3:
            subprocess.run(["fusermount", "-V", mountPoint], check=False)

    # If called from AppImage, then try to call the user-installed fusermount because FUSE might require
    # extra permissions depending on the policy and some systems then provide a fusermount binary with
    # ownership root and the setuid flag set.
    if os.path.ismount(mountPoint):
        fusermountPath = shutil.which("fusermount")
        if fusermountPath is None:
            fusermountPath = ""
        for folder in os.environ.get("PATH", "").split(os.pathsep):
            if not folder:
                continue
            binaryPath = os.path.join(folder, "fusermount")
            if fusermountPath != binaryPath and os.path.isfile(binaryPath) and os.access(binaryPath, os.X_OK):
                try:
                    subprocess.run(
                        [binaryPath, "-u", mountPoint], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE
                    )
                    if printDebug >= 2:
                        print(f"[Info] Successfully called {binaryPath} -u '{mountPoint}'.")
                    return
                except Exception as exception:
                    if printDebug >= 2:
                        print(f"[Warning] {fusermountPath} -u {mountPoint} failed with: {exception}")
                    if printDebug >= 3:
                        subprocess.run([fusermountPath, "-V", mountPoint], check=False)

    if os.path.ismount(mountPoint):
        try:
            subprocess.run(["umount", mountPoint], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            if printDebug >= 2:
                print(f"[Info] Successfully called umount -u '{mountPoint}'.")
            return
        except Exception as exception:
            if printDebug >= 2:
                print(f"[Warning] umount {mountPoint} failed with: {exception}")


def processParsedArguments(args) -> int:
    if args.unmount:
        # args.mount_source suffices because it eats all arguments and args.mount_point is always empty by default.
        mountPoints = [mountPoint for mountPoint in args.mount_source if mountPoint] if args.mount_source else []
        if not mountPoints:
            raise argparse.ArgumentTypeError("Unmounting requires a path to the mount point!")

        for mountPoint in mountPoints:
            unmount(mountPoint, printDebug=args.debug)

        # Unmounting might take some time and I had cases where fusermount returned exit code 1.
        # and still unmounted it successfully. It would be nice to automate this but it seems impossible to do
        # reliably, without any regular expression heuristics. /proc/<pid>/fd/5 links to /dev/fuse. This could
        # be used to reliable detect FUSE-providing processes, but we still wouldn't know which exact mount
        # point they provide.
        # This check is done outside of 'unmount' in order to only do one time.sleep for all mount points.
        errorPrinted = False
        if any(os.path.ismount(mountPoint) for mountPoint in mountPoints):
            time.sleep(1)
            for mountPoint in mountPoints:
                if not os.path.ismount(mountPoint):
                    continue
                if not errorPrinted:
                    print("[Error] Failed to unmount the given mount point. Alternatively, the process providing ")
                    print("[Error] the mount point can be looked for and killed, e.g., with this command:")
                    errorPrinted = True
                print(f"""[Error]     pkill --full 'ratarmount.*{mountPoint}' -G "$( id -g )" --newest""")

        return 1 if errorPrinted else 0

    args.gzipSeekPointSpacing = int(args.gzip_seek_point_spacing * 1024 * 1024)

    if args.recursive and args.recursion_depth is None:
        args.recursion_depth = -1
    if args.recursion_depth is None:
        args.recursion_depth = 0

    if (args.strip_recursive_tar_extension or args.transform_recursive_mount_point) and not args.recursion_depth:
        print("[Warning] The options --strip-recursive-tar-extension and --transform-recursive-mount-point")
        print("[Warning] only have an effect when used with recursive mounting.")

    if args.transform_recursive_mount_point:
        args.transform_recursive_mount_point = tuple(args.transform_recursive_mount_point)

    # This is a hack but because we have two positional arguments (and want that reflected in the auto-generated help),
    # all positional arguments, including the mountpath will be parsed into the tar file path's namespace and we have to
    # manually separate them depending on the type.
    lastArgument = args.mount_source[-1]
    if '://' not in lastArgument and (os.path.isdir(lastArgument) or not os.path.exists(lastArgument)):
        args.mount_point = lastArgument
        args.mount_source = args.mount_source[:-1]
    if not args.mount_source and not args.write_overlay:
        raise argparse.ArgumentTypeError(
            "You must at least specify one path to a valid TAR file or union mount source directory!"
        )

    # Sanitize different ways to specify passwords into a simple list
    # Better initialize it before calling checkMountSource, which might use args.passwords in the future.
    args.passwords = []
    if args.password:
        args.passwords.append(args.password.encode())

    if args.password_file:
        with open(args.password_file, 'rb') as file:
            args.passwords += file.read().split(b'\n')

    args.passwords = removeDuplicatesStable(args.passwords)

    # Manually check that all specified TARs and folders exist
    def checkMountSource(path):
        try:
            return checkInputFileType(path, encoding=args.encoding, printDebug=args.debug)[0]
        except argparse.ArgumentTypeError as e:
            if (
                os.path.isdir(path)
                or zipfile.is_zipfile(path)
                or ('rarfile' in sys.modules and rarfile.is_rarfile(path))
            ):
                return os.path.realpath(path)
            raise e

    mountSources: List[str] = []
    for path in args.mount_source:
        fixedPath = checkMountSource(path)
        # Skip neighboring duplicates
        if mountSources and mountSources[-1] == fixedPath:
            if args.debug >= 2:
                print(f"[Info] Skip duplicate mount source: {fixedPath}")
            continue
        mountSources.append(fixedPath)
    args.mount_source = mountSources

    for path in args.mount_source:
        if args.mount_source.count(path) > 1:
            raise argparse.ArgumentTypeError(f"Path may not appear multiple times at different locations: {path}")

    # Automatically generate a default mount path
    if not args.mount_point:
        autoMountPoint = stripSuffixFromTarFile(args.mount_source[0])
        if args.mount_point == autoMountPoint:
            args.mount_point = os.path.splitext(args.mount_source[0])[0]
        else:
            args.mount_point = autoMountPoint
        if '://' in args.mount_point:
            args.mount_point = "ratarmount.mounted"
    args.mount_point = os.path.abspath(args.mount_point)

    # Preprocess the --index-folders list as a string argument
    if args.index_folders and isinstance(args.index_folders, str):
        if args.index_folders[0] == '[':
            args.index_folders = json.loads(args.index_folders)
        elif ',' in args.index_folders:
            args.index_folders = args.index_folders.split(',')
        else:
            args.index_folders = [args.index_folders]

    # Check the parallelization argument and move to global variable
    assert isinstance(args.parallelization, int)
    if args.parallelization < 0:
        raise argparse.ArgumentTypeError("Argument for parallelization must be non-negative!")
    if args.parallelization == 0:
        args.parallelization = len(os.sched_getaffinity(0)) if hasattr(os, 'sched_getaffinity') else os.cpu_count()

    # Clean backend list
    supportedModuleNames = [module.name for _, info in supportedCompressions.items() for module in info.modules]
    args.prioritizedBackends = (
        [
            backend
            for backendString in args.use_backend
            for backend in backendString.split(',')
            if backend in supportedModuleNames
        ][::-1]
        if args.use_backend
        else []
    )

    if args.commit_overlay:
        if len(args.mount_source) != 1:
            raise RatarmountError("Currently, only modifications to a single TAR may be committed.")

        commitOverlay(args.write_overlay, args.mount_source[0], encoding=args.encoding, printDebug=args.debug)
        return 0

    createFuseMount(args)  # Throws on errors.
    return 0


def createFuseMount(args) -> None:
    # Convert the comma separated list of key[=value] options into a dictionary for fusepy
    fusekwargs = (
        dict(option.split('=', 1) if '=' in option else (option, True) for option in args.fuse.split(','))
        if args.fuse
        else {}
    )
    if args.prefix:
        fusekwargs['modules'] = 'subdir'
        fusekwargs['subdir'] = args.prefix

    if os.path.isdir(args.mount_point) and os.listdir(args.mount_point):
        if hasFUSENonEmptySupport():
            fusekwargs['nonempty'] = True

    from .FuseMount import FuseMount  # pylint: disable=import-outside-toplevel

    with FuseMount(
        # fmt: off
        pathToMount                  = args.mount_source,
        clearIndexCache              = bool(args.recreate_index),
        recursive                    = bool(args.recursive),
        recursionDepth               = int(args.recursion_depth),
        gzipSeekPointSpacing         = int(args.gzipSeekPointSpacing),
        mountPoint                   = args.mount_point,
        encoding                     = args.encoding,
        ignoreZeros                  = bool(args.ignore_zeros),
        verifyModificationTime       = bool(args.verify_mtime),
        stripRecursiveTarExtension   = args.strip_recursive_tar_extension,
        indexFilePath                = args.index_file,
        indexFolders                 = args.index_folders,
        lazyMounting                 = bool(args.lazy),
        passwords                    = list(args.passwords),
        parallelization              = args.parallelization,
        isGnuIncremental             = args.gnu_incremental,
        writeOverlay                 = args.write_overlay,
        printDebug                   = int(args.debug),
        transformRecursiveMountPoint = args.transform_recursive_mount_point,
        transform                    = args.transform,
        prioritizedBackends          = args.prioritizedBackends,
        disableUnionMount            = args.disable_union_mount,
        maxCacheDepth                = args.union_mount_cache_max_depth,
        maxCacheEntries              = args.union_mount_cache_max_entries,
        maxSecondsToCache            = args.union_mount_cache_timeout,
        indexMinimumFileCount        = args.index_minimum_file_count,
        foreground                   = bool(args.foreground),
        # fmt: on
    ) as fuseOperationsObject:
        try:
            fuse.FUSE(
                operations=fuseOperationsObject,
                mountpoint=args.mount_point,
                foreground=args.foreground,
                nothreads=True,  # Cannot access SQLite database connection object from multiple threads
                **fusekwargs,
            )
        except RuntimeError as exception:
            raise RatarmountError(
                "FUSE mountpoint could not be created. See previous output for more information."
            ) from exception

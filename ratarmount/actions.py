import argparse
import contextlib
import importlib
import json
import os
import re
import shutil
import sqlite3
import subprocess
import sys
import threading
import time
import urllib.request
import zipfile
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from ratarmountcore.compressions import strip_suffix_from_archive
from ratarmountcore.utils import RatarmountError, determine_recursion_depth, imeta, remove_duplicates_stable

with contextlib.suppress(ImportError):
    import rarfile

from .CLIHelpers import check_input_file_type
from .fuse import fuse
from .WriteOverlay import commit_overlay


def has_fuse_non_empty_support() -> bool:
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


def parse_requirement(requirement: str) -> Optional[Tuple[str, List[str], Optional[str]]]:
    # https://packaging.python.org/en/latest/specifications/name-normalization/
    # Match only valid project name and avoid cruft like extras and requirements, e.g.,
    # indexed_gzip >= 1.6.3, != 1.9.4; python_version < '3.8'
    match = re.match(
        r"^([A-Za-z0-9]([^A-Za-z0-9]|$)|[A-Za-z0-9][A-Za-z0-9._-]*[A-Za-z0-9])(\[([A-Za-z0-9, ]+)\])?"
        r"""(.*;.* extra ?== ?["']([^"']+)["'])?""",
        requirement,
    )
    if not match:
        return None
    # print(requirement," -> GROUPS:", [match.group(i) for i in range(1 + len(match.groups()))])
    return match.group(1), match.group(4).split(',') if match.group(4) else [], match.group(6)


def print_metadata_recursively(
    packages: Dict[str, Set[str]],
    doWithDistribution: Callable[[Any], None],
    doOnNewLevel: Optional[Callable[[int], None]] = None,
    level: int = 0,
    processedPackages: Optional[Set[str]] = None,
):
    if processedPackages is None:
        processedPackages = set()

    if not any(package not in processedPackages for package, _ in packages.items()):
        return
    if doOnNewLevel:
        doOnNewLevel(level)

    requirements: Dict[str, Set[str]] = {}
    for package, enabledExtras in sorted(packages.items()):
        # For now, packages specified with different extra will result in the latter being omitted.
        if package in processedPackages:
            continue
        processedPackages.add(package)

        try:
            distribution = imeta.distribution(package)
        except imeta.PackageNotFoundError:
            # Will happen for uninstalled optional or Python version dependent files and built-in modules, such as:
            #   fastzipfile, argparse, ...
            continue

        doWithDistribution(distribution)

        for requirement in distribution.requires or []:
            parsed = parse_requirement(requirement)
            if not parsed:
                # Should not happen and does not in my tests.
                print(f"  Cannot parse requirement: {requirement}")
                continue

            requiredPackage, packageExtras, extraNamespace = parsed
            if "full" not in enabledExtras and extraNamespace is not None and extraNamespace not in enabledExtras:
                continue
            if requiredPackage in processedPackages:
                continue

            requirements[requiredPackage] = requirements.get(requiredPackage, set()).union(set(packageExtras))

    print_metadata_recursively(requirements, doWithDistribution, doOnNewLevel, level + 1, processedPackages)


def print_versions() -> None:
    def do_for_distribution(distribution):
        if 'Name' not in distribution.metadata:
            return
        print(distribution.metadata['Name'] + " " + distribution.version)

        # Import the module in order to open the shared libraries so that we can look for loaded shared
        # libraries and list their versions!
        topLevel = distribution.read_text('top_level.txt')
        if topLevel:
            for module in topLevel.strip().split('\n'):
                # dropboxdrivefs installs a module named "test" and unicrypto a module named "tests".
                # This seems like a packaging bug to me because the names are too broad and the modules useless.
                if module and not module.startswith('_') and not module.startswith('test'):
                    with contextlib.suppress(Exception):
                        importlib.import_module(module)

    def print_on_new_level(level):
        if level > 1:
            print(f"\nLevel {level} Dependencies:\n")

    print_metadata_recursively({"ratarmount": {"full"}}, do_for_distribution, print_on_new_level)

    print()
    print("System Software:")
    print()
    print("Python", sys.version.split(' ', maxsplit=1)[0])

    try:
        fusermountVersion = subprocess.run(["fusermount", "--version"], capture_output=True, check=False).stdout.strip()
        print("fusermount", re.sub('.* ([0-9][.][0-9.]+).*', r'\1', fusermountVersion.decode()))
    except Exception:
        pass

    if hasattr(fuse, 'fuse_version_major') and hasattr(fuse, 'fuse_version_minor'):
        print(f"FUSE: {fuse.fuse_version_major}.{fuse.fuse_version_minor}")

    print("libsqlite3", sqlite3.sqlite_version)

    mappedFilesFolder = f"/proc/{os.getpid()}/map_files"
    if os.path.isdir(mappedFilesFolder):
        libraries = {os.readlink(os.path.join(mappedFilesFolder, link)) for link in os.listdir(mappedFilesFolder)}
        # Only look for shared libraries with versioning suffixed. Ignore all ending on .so.
        libraries = {library for library in libraries if '.so.' in library}

        if libraries:
            print()
            print("Versioned Loaded Shared Libraries:")
            print()

        for library in sorted(libraries):
            print(library.rsplit('/', maxsplit=1)[-1])


def find_short_license(distribution) -> str:
    shortLicense = ""

    # Check classifiers
    for key, value in distribution.metadata.items():
        if key == "Classifier" and value.startswith("License ::") and not value.endswith(":: OSI Approved"):
            if shortLicense:
                shortLicense += " OR "
            shortLicense += re.sub(r"([A-Z]+) License", r"\1", value.rsplit("::", 1)[-1].strip())

    # webdav4 only has this License-Expression.
    if not shortLicense:
        for key, value in distribution.metadata.items():
            if key == "License-Expression":
                shortLicense += value
                break

    # Check LICENSE key.
    if not shortLicense and 'LICENSE' in distribution.metadata and '\n' not in distribution.metadata['LICENSE']:
        shortLicense = distribution.metadata['LICENSE']

    # Analyze LICENSE file.
    if not shortLicense and 'License-File' in distribution.metadata:
        licenseContents = distribution.read_text(distribution.metadata['License-File'])
        if licenseContents:
            matched = re.match(r"^((MIT|BSD|GPL|LGPL).*)( License)?", licenseContents.split('\n')[0])
            if matched:
                shortLicense = matched.group(2)

    return shortLicense


def print_oss_attributions_short() -> None:
    def do_for_distribution(distribution):
        if 'Name' in distribution.metadata:
            print(f"{distribution.metadata['Name']:20} {distribution.version:12} {find_short_license(distribution)}")

    def print_on_new_level(level):
        if level > 1:
            print(f"\nLevel {level} Dependencies:\n")

    print_metadata_recursively({"ratarmount": {"full"}}, do_for_distribution, print_on_new_level)


def print_oss_attributions() -> None:
    def do_for_distribution(distribution):
        if 'Name' not in distribution.metadata:
            return
        name = distribution.metadata['Name']
        print("# " + name)
        print()

        if 'Summary' in distribution.metadata:
            print(distribution.metadata['Summary'])
            print()

        urls = [x for key, x in distribution.metadata.items() if key == 'Project-URL' and x]
        if urls:
            print("\n".join(urls))
            print()

        authors = [x for key, x in distribution.metadata.items() if key == 'Author' and x]
        if authors:
            print("Authors:", ", ".join(authors))
            print()

        # Analyze LICENSE file.
        urls = [x for key, x in distribution.metadata.items() if key == 'Project-URL' and x]
        licenses = []
        for key, value in distribution.metadata.items():
            if key == 'License-File':
                # All system-installed packages do not seem to be distributed with a license:
                # find /usr/lib/python3/dist-packages/ -iname '*license*'
                licenseContents = distribution.read_text(value) or distribution.read_text(
                    os.path.join("licenses", value)
                )
                if licenseContents:
                    licenses.append(licenseContents)
                    continue

        # This is known to happen for system-installed packages :/, and --editable installed packages.
        if not licenses:
            path = Path(f"/usr/share/doc/python3-{name}/copyright")
            if path.is_file():
                licenses.append(path.read_text(encoding='utf-8'))

        if licenses:
            for licenseContents in licenses:
                print("```\n" + licenseContents.strip('\n') + "\n```\n")
        else:
            print(name, "License:", find_short_license(distribution))
        print()

    print_metadata_recursively({"ratarmount": {"full"}}, do_for_distribution)

    # Licenses for non-Python libraries
    licenses = [
        ("libfuse", "/libfuse/libfuse/refs/heads/master/LGPL2.txt"),  # LGPL 2.1
        ("libsqlite3", "/sqlite/sqlite/master/LICENSE.md"),  # "The author disclaims copyright to this source code"
        ("cpython", "/python/cpython/main/LICENSE"),  # PYTHON SOFTWARE FOUNDATION LICENSE VERSION 2
        ("libzstd-seek", "/martinellimarco/libzstd-seek/main/LICENSE"),  # MIT
        ("zstd", "/facebook/zstd/dev/LICENSE"),  # BSD-3 with "name of the copyright holder" explicitly filled in
        ("zlib", "/madler/zlib/refs/heads/master/LICENSE"),  # zlib License
        # BSD-3 with "name of the copyright holder" explicitly filled in
        ("sqlcipher", "/sqlcipher/sqlcipher/refs/heads/master/LICENSE.txt"),
        ("python-ext4", "/Eeems/python-ext4/refs/heads/main/LICENSE"),  # MIT
    ]
    for name, githubPath in sorted(licenses):
        licenseUrl = "https://raw.githubusercontent.com" + githubPath
        try:
            licenseContents = urllib.request.urlopen(licenseUrl).read().decode()
        except urllib.error.HTTPError as error:
            licenseContents = f"Failed to get license at {licenseUrl} because of: {error!s}"
        homepage = "https://github.com" + '/'.join(githubPath.split('/', 3)[:3])
        print(f"# {name}\n\n{homepage}\n\n\n```\n{licenseContents}\n```\n\n")


def unmount(mountPoint: str, printDebug: int = 0) -> None:
    # Do not test with os.path.ismount or anything other because if the FUSE process was killed without
    # unmounting, then any file system query might return with errors.
    # https://github.com/python/cpython/issues/96328#issuecomment-2027458283

    try:
        subprocess.run(["fusermount", "-u", mountPoint], check=True, capture_output=True)
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
                    subprocess.run([binaryPath, "-u", mountPoint], check=True, capture_output=True)
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
            subprocess.run(["umount", mountPoint], check=True, capture_output=True)
            if printDebug >= 2:
                print(f"[Info] Successfully called umount -u '{mountPoint}'.")
            return
        except Exception as exception:
            if printDebug >= 2:
                print(f"[Warning] umount {mountPoint} failed with: {exception}")


def process_parsed_arguments(args) -> int:
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

    if (args.strip_recursive_tar_extension or args.transform_recursive_mount_point) and determine_recursion_depth(
        recursive=args.recursive, recursion_depth=args.recursion_depth
    ) <= 0:
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
    # Better initialize it before calling check_mount_source, which might use args.passwords in the future.
    args.passwords = []
    if args.password:
        args.passwords.append(args.password.encode())

    if args.password_file:
        args.passwords.extend(Path(args.password_file).read_bytes().split(b'\n'))

    args.passwords = remove_duplicates_stable(args.passwords)

    # Manually check that all specified TARs and folders exist
    def check_mount_source(path):
        try:
            return check_input_file_type(path, printDebug=args.debug)
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
        fixedPath = check_mount_source(path)
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
        autoMountPoint = strip_suffix_from_archive(args.mount_source[0])
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

    # Check the parallelization argument
    parallelizations = (
        {'': args.parallelization}
        if args.parallelization.isdigit()
        else dict(kv.split(':') for kv in args.parallelization.split(','))
    )
    args.parallelizations = {}
    defaultParallelization = len(os.sched_getaffinity(0)) if hasattr(os, 'sched_getaffinity') else os.cpu_count()
    for backend, parallelizationString in parallelizations.items():
        # isdigit does will be false if there is a minus sign, which is what we want.
        if not parallelizationString.isdigit():
            raise argparse.ArgumentTypeError(
                f"Parallelization must be non-negative number but got {parallelizationString} for {backend}!"
            )
        args.parallelizations[backend] = (
            defaultParallelization if int(parallelizationString) == 0 else int(parallelizationString)
        )
    if '' not in args.parallelizations:
        args.parallelizations[''] = defaultParallelization

    # Clean backend list
    args.prioritizedBackends = (
        [backend for backendString in args.use_backend for backend in backendString.split(',')][::-1]
        if args.use_backend
        else []
    )

    if args.commit_overlay:
        if len(args.mount_source) != 1:
            raise RatarmountError("Currently, only modifications to a single TAR may be committed.")

        commit_overlay(args.write_overlay, args.mount_source[0], encoding=args.encoding, printDebug=args.debug)
        return 0

    create_fuse_mount(args)  # Throws on errors.
    return 0


def create_fuse_mount(args) -> None:
    # Convert the comma separated list of key[=value] options into a dictionary for fusepy
    fusekwargs = (
        dict(option.split('=', 1) if '=' in option else (option, True) for option in args.fuse.split(','))
        if args.fuse
        else {}
    )
    if args.prefix:
        fusekwargs['modules'] = 'subdir'
        fusekwargs['subdir'] = args.prefix

    if os.path.isdir(args.mount_point) and os.listdir(args.mount_point) and has_fuse_non_empty_support():
        fusekwargs['nonempty'] = True

    from .FuseMount import FuseMount  # pylint: disable=import-outside-toplevel

    # fmt: off
    with FuseMount(
        pathToMount                  = args.mount_source,
        clearIndexCache              = bool(args.recreate_index),
        recursive                    = bool(args.recursive),
        recursionDepth               = args.recursion_depth,
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
        parallelizations             = args.parallelizations,
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
        logFile                      = args.log_file,
    ) as fuseOperationsObject:
        # fmt: on
        try:
            # Note that this will not detect threads started in shared libraries, only those started via "threading".
            if not args.foreground and len(threading.enumerate()) > 1:
                threadNames = [thread.name for thread in threading.enumerate() if thread.name != "MainThread"]
                # Fix FUSE hangs with: https://unix.stackexchange.com/a/713621/111050
                raise ValueError(
                    "Daemonizing FUSE into the background may result in errors or unkillable hangs because "
                    f"there are threads still open: {', '.join(threadNames)}!\nCall ratarmount with -f or --foreground."
                    " If you still want to run it in the background, use the usual shell tools to move it into the "
                    "background, i.e., nohup ratarmount -f ... &"
                )

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

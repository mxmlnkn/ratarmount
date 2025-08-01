import argparse
import contextlib
import functools
import importlib
import importlib.metadata
import inspect
import logging
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
from typing import Any, Callable, Optional

from ratarmountcore.compressions import strip_suffix_from_archive
from ratarmountcore.utils import RatarmountError, remove_duplicates_stable

with contextlib.suppress(ImportError):
    import rarfile

from ratarmount import CLIHelpers

from .fuse import fuse
from .WriteOverlay import commit_overlay

logger = logging.getLogger(__name__)


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


def is_inside_fuse_context() -> bool:
    for frame_info in inspect.stack():
        frame = frame_info.frame
        cls = frame.f_locals.get('cls', type(frame.f_locals.get('self', None)))
        if inspect.isclass(cls) and issubclass(cls, fuse.Operations):
            return True
    return False


def forbid_call_from_fuse(func):
    @functools.wraps(func)
    def wrapper(self, *args, **kwargs):
        # We have to take care not to call into ourself by accessing paths inside the mount point from the
        # FUSE-providing process! This also includes is.path.ismount!
        if is_inside_fuse_context():
            raise RuntimeError("A FUSE mount must not be created from another FUSE mount. Start a new subprocess!")
        return func(self, *args, **kwargs)

    return wrapper


def parse_requirement(requirement: str) -> Optional[tuple[str, list[str], Optional[str]]]:
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
    packages: dict[str, set[str]],
    doWithDistribution: Callable[[Any], None],
    doOnNewLevel: Optional[Callable[[int], None]] = None,
    level: int = 0,
    processedPackages: Optional[set[str]] = None,
):
    if processedPackages is None:
        processedPackages = set()

    if not any(package not in processedPackages for package, _ in packages.items()):
        return
    if doOnNewLevel:
        doOnNewLevel(level)

    requirements: dict[str, set[str]] = {}
    for package, enabledExtras in sorted(packages.items()):
        # For now, packages specified with different extra will result in the latter being omitted.
        if package in processedPackages:
            continue
        processedPackages.add(package)

        try:
            distribution = importlib.metadata.distribution(package)
        except importlib.metadata.PackageNotFoundError:
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


def unmount(mountPoint: str) -> None:
    # Do not test with os.path.ismount or anything other because if the FUSE process was killed without
    # unmounting, then any file system query might return with errors.
    # https://github.com/python/cpython/issues/96328#issuecomment-2027458283

    try:
        subprocess.run(["fusermount", "-u", mountPoint], check=True, capture_output=True)
        logger.info("Successfully called fusermount -u.")
        return
    except Exception as exception:
        logger.info("fusermount -u %s failed with: %s", mountPoint, exception)
        if logger.isEnabledFor(logging.DEBUG):
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
                    logger.info("Successfully called %s -u '%s'.", binaryPath, mountPoint)
                    return
                except Exception as exception:
                    logger.info("%s -u %s failed with: %s", fusermountPath, mountPoint, exception)
                    if logger.isEnabledFor(logging.DEBUG):
                        subprocess.run([fusermountPath, "-V", mountPoint], check=False)

    if os.path.ismount(mountPoint):
        try:
            subprocess.run(["umount", mountPoint], check=True, capture_output=True)
            logger.info("Successfully called umount -u '%s'.", mountPoint)
            return
        except Exception as exception:
            logger.info("umount %s failed with: %s", mountPoint, exception)


@forbid_call_from_fuse
def unmount_list_checked(mountPoints: list[str]) -> int:
    if not mountPoints:
        raise argparse.ArgumentTypeError("Unmounting requires a path to the mount point!")

    for mountPoint in mountPoints:
        unmount(mountPoint)

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
                logger.error(
                    "Failed to unmount the given mount point. Alternatively, the process providing the "
                    "mount point can be looked for and killed, e.g., with this command:"
                )
                errorPrinted = True
            logger.error("""    pkill --full 'ratarmount.*{%s}' -G "$( id -g )" --newest""", mountPoint)

    return 1 if errorPrinted else 0


@forbid_call_from_fuse
def determine_mount_point(mount_source: str):
    mount_point = strip_suffix_from_archive(mount_source)

    if '://' in mount_point:
        # There will be at least 2 slashes in mount_point, namely from ://.
        mount_point = mount_point.rsplit('/', 1)[1]

    # Files might not have a standard archive file extension, e.g., chimera files or docx (ZIP) and so on.
    # Therefore, try to generically strip the file extension.
    if mount_point == mount_source:
        # splitext is smarter than split('.') and will not split dots in parent folders!
        mount_point = os.path.splitext(mount_point)[0]

    # If the file has no extension at all, then add one to get a different mount point:
    if mount_point == mount_source:
        mount_point = mount_point + ".mounted"

    if os.path.exists(mount_point) and not os.path.isdir(mount_point):
        raise argparse.ArgumentTypeError(
            "No mount point was specified and failed to automatically infer a valid one. "
            "Please explicitly specify a mount point. See --help."
        )

    logger.info("No mount point specified. Automatically inferred: %s", mount_point)
    return mount_point


@forbid_call_from_fuse
def process_parsed_arguments(args) -> int:
    if args.unmount:
        # args.mount_source suffices because it eats all arguments and args.mount_point is always empty by default.
        return unmount_list_checked([mountPoint for mountPoint in args.mount_source or [] if mountPoint])

    # This is a hack but because we have two positional arguments (and want that reflected in the auto-generated help),
    # all positional arguments, including the mountpath will be parsed into args.mount_source and we have to
    # manually separate them depending on the type.
    lastArgument = args.mount_source[-1]
    if '://' not in lastArgument and (os.path.isdir(lastArgument) or not os.path.exists(lastArgument)):
        args.mount_point = args.mount_source.pop()
    if not args.mount_source and not args.write_overlay and not args.control_interface:
        raise argparse.ArgumentTypeError("You must specify at least one path to a valid archive or folder!")

    # Manually check that all specified TARs and folders exist
    def check_mount_source(path):
        try:
            return CLIHelpers.check_input_file_type(path)
        except argparse.ArgumentTypeError as e:
            if not os.path.exists(path):
                raise e
            if (
                os.path.isdir(path)
                or zipfile.is_zipfile(path)
                or ('rarfile' in sys.modules and rarfile.is_rarfile(path))
            ):
                return os.path.realpath(path)
            raise e

    mountSources: list[str] = []
    for path in args.mount_source:
        fixedPath = check_mount_source(path)
        # Skip neighboring duplicates
        if mountSources and mountSources[-1] == fixedPath:
            logger.info("Skip duplicate mount source: %s", fixedPath)
            continue
        mountSources.append(fixedPath)
    args.mount_source = mountSources

    # This can only be called after post-processing all required args members!
    if args.commit_overlay:
        if len(args.mount_source) != 1:
            raise RatarmountError("Currently, only modifications to a single TAR may be committed.")

        commit_overlay(args.write_overlay, args.mount_source[0], encoding=args.encoding, printDebug=args.debug)
        return 0

    # Automatically generate a default mount path
    if not args.mount_point:
        args.mount_point = determine_mount_point(args.mount_source[0]) if args.mount_source else 'mounted'
    args.mount_point = os.path.realpath(args.mount_point)

    CLIHelpers.process_trivial_parsed_arguments(args)

    if args.password_file:
        args.passwords.extend(Path(args.password_file).read_bytes().split(b'\n'))
    args.passwords = remove_duplicates_stable(args.passwords)

    create_fuse_mount(args)  # Throws on errors.
    return 0


def create_fuse_mount(args) -> None:
    if is_inside_fuse_context():
        raise RuntimeError("A FUSE mount must not be created from another FUSE mount. Start a new subprocess!")

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

    # Import late to avoid recursion and overhead during argcomplete!
    from .FuseMount import FuseMount  # pylint: disable=import-outside-toplevel

    with FuseMount(**CLIHelpers.parsed_args_to_options(args)) as fuseOperationsObject:
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

import argparse
import contextlib
import functools
import inspect
import logging
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import zipfile
from pathlib import Path

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

    if args.mount:
        create_fuse_mount(args)  # Throws on errors.
    else:
        # Import late to avoid recursion and overhead during argcomplete!
        from .FuseMount import FuseMount  # pylint: disable=import-outside-toplevel

        # Simply calling the FuseMount constructor and destructor should create the indexes as a side effect.
        with FuseMount(**CLIHelpers.parsed_args_to_options(args)):
            pass

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

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import json
import os
import re
import stat
import sys
import tarfile
import traceback
import zipfile
from typing import Any, Dict, Iterable, IO, List, Optional, Tuple, Union
import fuse

try:
    import rarfile
except ImportError:
    pass

import ratarmountcore as core
from ratarmountcore import (
    SQLiteIndexedTar,
    MountSource,
    UnionMountSource,
    AutoMountLayer,
    FileVersionLayer,
    openMountSource,
    FolderMountSource,
    overrides,
    supportedCompressions,
    stripSuffixFromTarFile,
    FileInfo,
)


__version__ = '0.10.0'


def hasNonEmptySupport() -> bool:
    try:
        with os.popen('fusermount -V') as pipe:
            match = re.search(r'([0-9]+)[.][0-9]+[.][0-9]+', pipe.read())
            if match:
                return int(match.group(1)) < 3
    except Exception:
        pass

    return False  # On macOS, fusermount does not exist and macfuse also seems to complain with nonempty option.


class FuseMount(fuse.Operations):
    """
    This class implements the fusepy interface in order to create a mounted file system view to a MountSource.
    Tasks of this class itself:
       - Changes all file permissions to read-only
       - Get actual file contents either by directly reading from the TAR or by using StenciledFile and tarfile
       - Enabling FolderMountSource to bind to the nonempty folder under the mountpoint itself.
    Other functionalities like file versioning, hard link resolving, and union mounting are implemented by using
    the respective MountSource derived classes.

    Documentation for FUSE methods can be found in the fusepy or libfuse headers. There seems to be no complete
    rendered documentation aside from the header comments.

    https://github.com/fusepy/fusepy/blob/master/fuse.py
    https://github.com/libfuse/libfuse/blob/master/include/fuse.h
    https://man7.org/linux/man-pages/man3/errno.3.html

    All path arguments for overriden fusepy methods do have a leading slash ('/')!
    This is why MountSource also should expect leading slashes in all paths.
    """

    def __init__(self, pathToMount: Union[str, List[str]], mountPoint: str, **options) -> None:
        if not isinstance(pathToMount, list):
            try:
                os.fspath(pathToMount)
                pathToMount = [pathToMount]
            except Exception:
                pass

        options['writeIndex'] = True

        self.printDebug = options.get('printDebug', 0)

        # This also will create or load the block offsets for compressed formats
        mountSources = [openMountSource(path, **options) for path in pathToMount]

        # No threads should be created and still be open before FUSE forks.
        # Instead, they should be created in 'init'.
        # Therefore, close threads opened by the ParallelBZ2Reader for creating the block offsets.
        # Those threads will be automatically recreated again on the next read call.
        # Without this, the ratarmount background process won't quit even after unmounting!
        for mountSource in mountSources:
            if (
                isinstance(mountSource, SQLiteIndexedTar)
                and hasattr(mountSource, 'tarFileObject')
                and hasattr(mountSource.tarFileObject, 'join_threads')
            ):
                mountSource.tarFileObject.join_threads()

        self.mountSource: MountSource = UnionMountSource(mountSources, printDebug=self.printDebug)
        if options.get('recursive', False):
            self.mountSource = AutoMountLayer(self.mountSource, **options)
        self.mountSource = FileVersionLayer(self.mountSource)

        self.rootFileInfo = FuseMount._makeMountPointFileInfoFromStats(os.stat(pathToMount[0]))

        self.openedFiles: Dict[int, IO[bytes]] = {}
        self.lastFileHandle: int = 0  # It will be incremented before being returned. It can't hurt to never return 0.

        # Create mount point if it does not exist
        self.mountPointWasCreated = False
        if mountPoint and not os.path.exists(mountPoint):
            os.mkdir(mountPoint)
            self.mountPointWasCreated = True
        self.mountPoint = os.path.realpath(mountPoint)

        statResults = os.lstat(self.mountPoint)
        self.mountPointInfo = {key: getattr(statResults, key) for key in dir(statResults) if key.startswith('st_')}

        # Take care that bind-mounting folders to itself works
        self.mountPointFd = None
        self.selfBindMount: Optional[FolderMountSource] = None
        for mountSource in mountSources:
            if isinstance(mountSource, FolderMountSource) and mountSource.root == self.mountPoint:
                self.selfBindMount = mountSource
                self.mountPointFd = os.open(self.mountPoint, os.O_RDONLY)

    def __del__(self) -> None:
        try:
            if self.mountPointWasCreated:
                os.rmdir(self.mountPoint)
        except Exception:
            pass

        try:
            if self.mountPointFd is not None:
                os.close(self.mountPointFd)
        except Exception:
            pass

    @staticmethod
    def _makeMountPointFileInfoFromStats(stats: os.stat_result) -> FileInfo:
        # make the mount point read only and executable if readable, i.e., allow directory listing
        # clear higher bits like S_IFREG and set the directory bit instead
        mountMode = (
            (stats.st_mode & 0o777)
            | stat.S_IFDIR
            | (stat.S_IXUSR if stats.st_mode & stat.S_IRUSR != 0 else 0)
            | (stat.S_IXGRP if stats.st_mode & stat.S_IRGRP != 0 else 0)
            | (stat.S_IXOTH if stats.st_mode & stat.S_IROTH != 0 else 0)
        )

        fileInfo = FileInfo(
            # fmt: off
            size     = stats.st_size,
            mtime    = stats.st_mtime,
            mode     = mountMode,
            linkname = "",
            uid      = stats.st_uid,
            gid      = stats.st_gid,
            userdata = [],
            # fmt: on
        )

        return fileInfo

    def _getFileInfo(self, path: str) -> FileInfo:
        fileInfo = self.mountSource.getFileInfo(path)
        if fileInfo is None:
            raise fuse.FuseOSError(fuse.errno.ENOENT)
        return fileInfo

    @overrides(fuse.Operations)
    def init(self, path) -> None:
        if self.selfBindMount is not None and self.mountPointFd is not None:
            self.selfBindMount.setFolderDescriptor(self.mountPointFd)

    @staticmethod
    def _fileInfoToDict(fileInfo: FileInfo):
        # dictionary keys: https://pubs.opengroup.org/onlinepubs/007904875/basedefs/sys/stat.h.html
        statDict = {"st_" + key: getattr(fileInfo, key) for key in ('size', 'mtime', 'mode', 'uid', 'gid')}
        statDict['st_mtime'] = int(statDict['st_mtime'])
        statDict['st_nlink'] = 1  # TODO: this is wrong for files with hardlinks

        # du by default sums disk usage (the number of blocks used by a file)
        # instead of file size directly. Tar files are usually a series of 512B
        # blocks, so we report a 1-block header + ceil(filesize / 512).
        statDict['st_blksize'] = 512
        statDict['st_blocks'] = 1 + ((fileInfo.size + 511) // 512)

        return statDict

    @overrides(fuse.Operations)
    def getattr(self, path: str, fh=None) -> Dict[str, Any]:
        return self._fileInfoToDict(self._getFileInfo(path))

    @overrides(fuse.Operations)
    def readdir(self, path: str, fh):
        '''
        Can return either a list of names, or a list of (name, attrs, offset)
        tuples. attrs is a dict as in getattr.
        '''

        files = self.mountSource.listDir(path)

        # we only need to return these special directories. FUSE automatically expands these and will not ask
        # for paths like /../foo/./../bar, so we don't need to worry about cleaning such paths
        if isinstance(files, dict):
            yield '.', self.getattr(path), 0

            if path == '/':
                yield '..', self.mountPointInfo, 0
            else:
                yield '..', self.getattr(path.rsplit('/', 1)[0]), 0
        else:
            yield '.'
            yield '..'

        if isinstance(files, dict):
            for key, fileInfo in files.items():
                yield key, self._fileInfoToDict(fileInfo), 0
        elif files is not None:
            for key in files:
                yield key

    @overrides(fuse.Operations)
    def readlink(self, path: str) -> str:
        return self._getFileInfo(path).linkname

    @overrides(fuse.Operations)
    def open(self, path, flags):
        """Returns file handle of opened path."""

        fileInfo = self._getFileInfo(path)

        try:
            self.lastFileHandle += 1
            self.openedFiles[self.lastFileHandle] = self.mountSource.open(fileInfo)
            return self.lastFileHandle
        except Exception as exception:
            traceback.print_exc()
            print("Caught exception when trying to open file.", fileInfo)
            raise fuse.FuseOSError(fuse.errno.EIO) from exception

    @overrides(fuse.Operations)
    def release(self, path, fh):
        if fh not in self.openedFiles:
            raise fuse.FuseOSError(fuse.errno.ESTALE)

        self.openedFiles[fh].close()
        del self.openedFiles[fh]
        return fh

    @overrides(fuse.Operations)
    def read(self, path: str, size: int, offset: int, fh: int) -> bytes:
        if fh in self.openedFiles:
            self.openedFiles[fh].seek(offset, os.SEEK_SET)
            return self.openedFiles[fh].read(size)

        # As far as I understand FUSE and my own file handle cache, this should never happen. But you never know.
        if self.printDebug >= 1:
            print("[Warning] Given file handle does not exist. Will open file before reading which might be slow.")

        fileInfo = self._getFileInfo(path)

        try:
            return self.mountSource.read(fileInfo, size, offset)
        except Exception as exception:
            traceback.print_exc()
            print("Caught exception when trying to read data from underlying TAR file! Returning errno.EIO.")
            raise fuse.FuseOSError(fuse.errno.EIO) from exception


class TarFileType:
    """
    Similar to argparse.FileType but raises an exception if it is not a valid TAR file.
    """

    def __init__(self, encoding: str = tarfile.ENCODING, printDebug: int = 0) -> None:
        self.encoding = encoding
        self.printDebug = printDebug

    def __call__(self, tarFile: str) -> Tuple[str, Optional[str]]:
        if not os.path.isfile(tarFile):
            raise argparse.ArgumentTypeError(f"File '{tarFile}' is not a file!")

        with open(tarFile, 'rb') as fileobj:
            fileSize = os.stat(tarFile).st_size

            # Header checks are enough for this step.
            oldOffset = fileobj.tell()
            compression = None
            for compressionId, compressionInfo in supportedCompressions.items():
                try:
                    if compressionInfo.checkHeader(fileobj):
                        compression = compressionId
                        break
                finally:
                    fileobj.seek(oldOffset)

            try:
                # Determining if there are many frames in zstd is O(1) with is_multiframe
                if compression != 'zst' or supportedCompressions[compression].moduleName not in sys.modules:
                    raise Exception()  # early exit because we catch it anyways

                zstdFile = supportedCompressions[compression].open(fileobj)

                if not zstdFile.is_multiframe() and fileSize > 1024 * 1024:
                    print(f"[Warning] The specified file '{tarFile}'")
                    print("[Warning] is compressed using zstd but only contains one zstd frame. This makes it ")
                    print("[Warning] impossible to use true seeking! Please (re)compress your TAR using multiple ")
                    print("[Warning] frames in order for ratarmount to do be able to do fast seeking to requested ")
                    print("[Warning] files. Else, each file access will decompress the whole TAR from the beginning!")
                    print("[Warning] You can try out t2sz for creating such archives:")
                    print("[Warning] https://github.com/martinellimarco/t2sz")
                    print("[Warning] Here you can find a simple bash script demonstrating how to do this:")
                    print("[Warning] https://github.com/mxmlnkn/ratarmount#xz-and-zst-files")
                    print()
            except Exception:
                pass

            if compression not in supportedCompressions:
                if SQLiteIndexedTar._detectTar(fileobj, self.encoding, printDebug=self.printDebug):
                    return tarFile, compression

                if self.printDebug >= 2:
                    print(f"Archive '{tarFile}' (compression: {compression}) can't be opened!")

                raise argparse.ArgumentTypeError(f"Archive '{tarFile}' can't be opened!\n")

        cinfo = supportedCompressions[compression]
        if cinfo.moduleName not in sys.modules:
            raise argparse.ArgumentTypeError(
                f"Can't open a {compression} compressed TAR file '{fileobj.name}' without {cinfo.moduleName} module!"
            )

        return tarFile, compression


def _removeDuplicatesStable(iterable: Iterable):
    seen = set()
    deduplicated = []
    for x in iterable:
        if x not in seen:
            deduplicated.append(x)
            seen.add(x)
    return deduplicated


class _CustomFormatter(argparse.ArgumentDefaultsHelpFormatter, argparse.RawDescriptionHelpFormatter):
    def add_arguments(self, actions):
        actions = sorted(actions, key=lambda x: getattr(x, 'option_strings'))
        super().add_arguments(actions)


def _parseArgs(rawArgs: Optional[List[str]] = None):
    parser = argparse.ArgumentParser(
        formatter_class=_CustomFormatter,
        description='''\
With ratarmount, you can:
  - Mount a (compressed) TAR file to a folder for read-only access
  - Mount a compressed file to `<mountpoint>/<filename>`
  - Bind mount a folder to another folder for read-only access
  - Union mount a list of TARs, compressed files, and folders to a mount point
    for read-only access
''',
        epilog='''\
# Metadata Index Cache

In order to reduce the mounting time, the created index for random access
to files inside the tar will be saved to one of these locations. These
locations are checked in order and the first, which works sufficiently, will
be used. This is the default location order:

  1. <path to tar>.index.sqlite
  2. ~/.ratarmount/<path to tar: '/' -> '_'>.index.sqlite
     E.g., ~/.ratarmount/_media_cdrom_programm.tar.index.sqlite

This list of fallback folders can be overwritten using the `--index-folders`
option. Furthermore, an explicitly named index file may be specified using
the `--index-file` option. If `--index-file` is used, then the fallback
folders, including the default ones, will be ignored!

# Bind Mounting

The mount sources can be TARs and/or folders.  Because of that, ratarmount
can also be used to bind mount folders read-only to another path similar to
`bindfs` and `mount --bind`. So, for:

    ratarmount folder mountpoint

all files in `folder` will now be visible in mountpoint.

# Union Mounting

If multiple mount sources are specified, the sources on the right side will be
added to or update existing files from a mount source left of it. For example:

    ratarmount folder1 folder2 mountpoint

will make both, the files from folder1 and folder2, visible in mountpoint.
If a file exists in both multiple source, then the file from the rightmost
mount source will be used, which in the above example would be `folder2`.

If you want to update / overwrite a folder with the contents of a given TAR,
you can specify the folder both as a mount source and as the mount point:

    ratarmount folder file.tar folder

The FUSE option -o nonempty will be automatically added if such a usage is
detected. If you instead want to update a TAR with a folder, you only have to
swap the two mount sources:

    ratarmount file.tar folder folder

# File versions

If a file exists multiple times in a TAR or in multiple mount sources, then
the hidden versions can be accessed through special <file>.versions folders.
For example, consider:

    ratarmount folder updated.tar mountpoint

and the file `foo` exists both in the folder and as two different versions
in `updated.tar`. Then, you can list all three versions using:

    ls -la mountpoint/foo.versions/
        dr-xr-xr-x 2 user group     0 Apr 25 21:41 .
        dr-x------ 2 user group 10240 Apr 26 15:59 ..
        -r-x------ 2 user group   123 Apr 25 21:41 1
        -r-x------ 2 user group   256 Apr 25 21:53 2
        -r-x------ 2 user group  1024 Apr 25 22:13 3

In this example, the oldest version has only 123 bytes while the newest and
by default shown version has 1024 bytes. So, in order to look at the oldest
version, you can simply do:

    cat mountpoint/foo.versions/1

Note that these version numbers are the same as when used with tar's
`--occurrence=N` option.

## Prefix Removal

Use `ratarmount -o modules=subdir,subdir=<prefix>` to remove path prefixes
using the FUSE `subdir` module. Because it is a standard FUSE feature, the
`-o ...` argument should also work for other FUSE applications.

When mounting an archive created with absolute paths, e.g.,
`tar -P cf /var/log/apt/history.log`, you would see the whole `var/log/apt`
hierarchy under the mount point. To avoid that, specified prefixes can be
stripped from paths so that the mount target directory **directly** contains
`history.log`. Use `ratarmount -o modules=subdir,subdir=/var/log/apt/` to do
so. The specified path to the folder inside the TAR will be mounted to root,
i.e., the mount point.

# Compressed non-TAR files

If you want a compressed file not containing a TAR, e.g., `foo.bz2`, then
you can also use ratarmount for that. The uncompressed view will then be
mounted to `<mountpoint>/foo` and you will be able to leverage ratarmount's
seeking capabilities when opening that file.
''',
    )

    # fmt: off
    parser.add_argument(
        '-f', '--foreground', action='store_true', default = False,
        help = 'Keeps the python program in foreground so it can print debug '
               'output when the mounted path is accessed.' )

    parser.add_argument(
        '-d', '--debug', type = int, default = 1,
        help = 'Sets the debugging level. Higher means more output. Currently, 3 is the highest.' )

    parser.add_argument(
        '-c', '--recreate-index', action='store_true', default = False,
        help = 'If specified, pre-existing .index files will be deleted and newly created.' )

    parser.add_argument(
        '-r', '--recursive', action='store_true', default = False,
        help = 'Mount TAR archives inside the mounted TAR recursively. '
               'Note that this only has an effect when creating an index. '
               'If an index already exists, then this option will be effectively ignored. '
               'Recreate the index if you want change the recursive mounting policy anyways.' )

    parser.add_argument(
        '-l', '--lazy', action='store_true', default = False,
        help = 'When used with recursively bind-mounted folders, TAR files inside the mounted folder will only be '
               'mounted on first access to it.' )

    # Considerations for the default value:
    #   - seek times for the bz2 backend are between 0.01s and 0.1s
    #   - seek times for the gzip backend are roughly 1/10th compared to bz2 at a default spacing of 4MiB
    #     -> we could do a spacing of 40MiB (however the comparison are for another test archive, so it might not apply)
    #   - ungziping firefox 66 inflates the compressed size of 66MiB to 184MiB (~3 times more) and takes 1.4s on my PC
    #     -> to have a response time of 0.1s, it would require a spacing < 13MiB
    #   - the gzip index takes roughly 32kiB per seek point
    #   - the bzip2 index takes roughly 16B per 100-900kiB of compressed data
    #     -> for the gzip index to have the same space efficiency assuming a compression ratio of only 1,
    #        the spacing would have to be 1800MiB at which point it would become almost useless
    parser.add_argument(
        '-gs', '--gzip-seek-point-spacing', type = float, default = 16,
        help =
        'This only is applied when the index is first created or recreated with the -c option. '
        'The spacing given in MiB specifies the seek point distance in the uncompressed data. '
        'A distance of 16MiB means that archives smaller than 16MiB in uncompressed size will '
        'not benefit from faster seek times. A seek point takes roughly 32kiB. '
        'So, smaller distances lead to more responsive seeking but may explode the index size!' )

    parser.add_argument(
        '-p', '--prefix', type = str, default = '',
        help = '[deprecated] Use "-o modules=subdir,subdir=<prefix>" instead. '
               'This standard way utilizes FUSE itself and will also work for other FUSE '
               'applications. So, it is preferable even if a bit more verbose.'
               'The specified path to the folder inside the TAR will be mounted to root. '
               'This can be useful when the archive as created with absolute paths. '
               'E.g., for an archive created with `tar -P cf /var/log/apt/history.log`, '
               '-p /var/log/apt/ can be specified so that the mount target directory '
               '>directly< contains history.log.' )

    parser.add_argument(
        '--password', type = str, default = '',
        help = 'Specify a single password which shall be used for RAR and ZIP files.' )

    parser.add_argument(
        '--password-file', type = str, default = '',
        help = 'Specify a file with newline separated passwords for RAR and ZIP files. '
               'The passwords will be tried out in order of appearance in the file.' )

    parser.add_argument(
        '-e', '--encoding', type = str, default = tarfile.ENCODING,
        help = 'Specify an input encoding used for file names among others in the TAR. '
               'This must be used when, e.g., trying to open a latin1 encoded TAR on an UTF-8 system. '
               'Possible encodings: https://docs.python.org/3/library/codecs.html#standard-encodings' )

    parser.add_argument(
        '-i', '--ignore-zeros', action = 'store_true',
        help = 'Ignore zeroed blocks in archive. Normally, two consecutive 512-blocks filled with zeroes mean EOF '
               'and ratarmount stops reading after encountering them. This option instructs it to read further and '
               'is useful when reading archives created with the -A option.' )

    parser.add_argument(
        '--gnu-incremental', dest = 'gnu_incremental', action = 'store_true', default = None,
        help = 'Will strip octal modification time prefixes from file paths, which appear in GNU incremental backups '
               'created with GNU tar with the --incremental or --listed-incremental options.')

    parser.add_argument(
        '--no-gnu-incremental', dest = 'gnu_incremental', action = 'store_false',
        help = 'If specified, will never strip octal modification prefixes and will also not do automatic detection.')

    parser.add_argument(
        '--verify-mtime', action = 'store_true',
        help = 'By default, only the TAR file size is checked to match the one in the found existing ratarmount index. '
               'If this option is specified, then also check the modification timestamp. But beware that the mtime '
               'might change during copying or downloading without the contents changing. So, this check might cause '
               'false positives.' )

    parser.add_argument(
        '-s', '--strip-recursive-tar-extension', action = 'store_true',
        help = 'If true, then recursively mounted TARs named <file>.tar will be mounted at <file>/. '
               'This might lead to folders of the same name being overwritten, so use with care. '
               'The index needs to be (re)created to apply this option!' )

    parser.add_argument(
        '--index-file', type = str,
        help = 'Specify a path to the .index.sqlite file. Setting this will disable fallback index folders. '
               'If the given path is ":memory:", then the index will not be written out to disk.' )

    parser.add_argument(
        '--index-folders', default = "," + os.path.join( "~", ".ratarmount" ),
        help = 'Specify one or multiple paths for storing .index.sqlite files. Paths will be tested for suitability '
               'in the given order. An empty path will be interpreted as the location in which the TAR resides. '
               'If the argument begins with a bracket "[", then it will be interpreted as a JSON-formatted list. '
               'If the argument contains a comma ",", it will be interpreted as a comma-separated list of folders. '
               'Else, the whole string will be interpreted as one folder path. Examples: '
               '--index-folders ",~/.foo" will try to save besides the TAR and if that does not work, in ~/.foo. '
               '--index-folders \'["~/.ratarmount", "foo,9000"]\' will never try to save besides the TAR. '
               '--index-folder ~/.ratarmount will only test ~/.ratarmount as a storage location and nothing else. '
               'Instead, it will first try ~/.ratarmount and the folder "foo,9000". ' )

    parser.add_argument(
        '-o', '--fuse', type = str, default = '',
        help = 'Comma separated FUSE options. See "man mount.fuse" for help. '
               'Example: --fuse "allow_other,entry_timeout=2.8,gid=0". ' )

    parser.add_argument(
        '-P', '--parallelization', type = int, default = 1,
        help = 'If an integer other than 1 is specified, then the threaded parallel bzip2 decoder will be used '
               'specified amount of block decoder threads. Further threads with lighter work may be started. '
               f'A value of 0 will use all the available cores ({os.cpu_count()}).')

    parser.add_argument(
        '-v', '--version', action='store_true', help = 'Print version string.' )

    parser.add_argument(
        'mount_source', nargs = '+',
        help = 'The path to the TAR archive to be mounted. '
               'If multiple archives and/or folders are specified, then they will be mounted as if the arguments '
               'coming first were updated with the contents of the archives or folders specified thereafter, '
               'i.e., the list of TARs and folders will be union mounted.' )
    parser.add_argument(
        'mount_point', nargs = '?',
        help = 'The path to a folder to mount the TAR contents into. '
               'If no mount path is specified, the TAR will be mounted to a folder of the same name '
               'but without a file extension.' )
    # fmt: on

    args = parser.parse_args(rawArgs)

    args.gzipSeekPointSpacing = args.gzip_seek_point_spacing * 1024 * 1024

    # This is a hack but because we have two positional arguments (and want that reflected in the auto-generated help),
    # all positional arguments, including the mountpath will be parsed into the tarfilepaths namespace and we have to
    # manually separate them depending on the type.
    if os.path.isdir(args.mount_source[-1]) or not os.path.exists(args.mount_source[-1]):
        args.mount_point = args.mount_source[-1]
        args.mount_source = args.mount_source[:-1]
    if not args.mount_source:
        print("[Error] You must at least specify one path to a valid TAR file or union mount source directory!")
        sys.exit(1)

    # Manually check that all specified TARs and folders exist
    def checkMountSource(path):
        if os.path.isdir(path) or zipfile.is_zipfile(path) or ('rarfile' in sys.modules and rarfile.is_rarfile(path)):
            return os.path.realpath(path)
        return TarFileType(encoding=args.encoding, printDebug=args.debug)(path)[0]

    args.mount_source = [checkMountSource(path) for path in args.mount_source]

    # Automatically generate a default mount path
    if not args.mount_point:
        autoMountPoint = stripSuffixFromTarFile(args.mount_source[0])
        if args.mount_point == autoMountPoint:
            args.mount_point = os.path.splitext(args.mount_source[0])[0]
        else:
            args.mount_point = autoMountPoint
    args.mount_point = os.path.abspath(args.mount_point)

    # Preprocess the --index-folders list as a string argument
    if args.index_folders:
        if args.index_folders[0] == '[':
            args.index_folders = json.loads(args.index_folders)
        elif ',' in args.index_folders:
            args.index_folders = args.index_folders.split(',')

    # Check the parallelization argument and move to global variable
    assert isinstance(args.parallelization, int)
    if args.parallelization < 0:
        raise argparse.ArgumentTypeError("Argument for parallelization must be non-negative!")
    if args.parallelization == 0:
        args.parallelization = os.cpu_count()

    # Sanitize different ways to specify passwords into a simple list
    args.passwords = []
    if args.password:
        args.passwords.append(args.password)

    if args.password_file:
        with open(args.password_file, 'rb') as file:
            args.passwords += file.read().split(b'\n')

    args.passwords = _removeDuplicatesStable(args.passwords)

    return args


def cli(rawArgs: Optional[List[str]] = None) -> None:
    """Command line interface for ratarmount. Call with args = [ '--help' ] for a description."""

    # The first argument, is the path to the script and should be ignored
    tmpArgs = sys.argv[1:] if rawArgs is None else rawArgs
    if '--version' in tmpArgs or '-v' in tmpArgs:
        print("ratarmount", __version__)
        print("ratarmountcore", core.__version__)
        return

    # tmpArgs are only for the manual parsing. In general, rawArgs is None, meaning it reads sys.argv,
    # and maybe sometimes contains arguments when used programmatically. In that case the first argument
    # should not be the path to the script!
    args = _parseArgs(rawArgs)

    # Convert the comma separated list of key[=value] options into a dictionary for fusepy
    fusekwargs = (
        dict([option.split('=', 1) if '=' in option else (option, True) for option in args.fuse.split(',')])
        if args.fuse
        else {}
    )
    if args.prefix:
        fusekwargs['modules'] = 'subdir'
        fusekwargs['subdir'] = args.prefix

    if args.mount_point in args.mount_source and os.path.isdir(args.mount_point) and os.listdir(args.mount_point):
        if hasNonEmptySupport():
            fusekwargs['nonempty'] = True

    fuseOperationsObject = FuseMount(
        # fmt: off
        pathToMount                = args.mount_source,
        clearIndexCache            = args.recreate_index,
        recursive                  = args.recursive,
        gzipSeekPointSpacing       = args.gzipSeekPointSpacing,
        mountPoint                 = args.mount_point,
        encoding                   = args.encoding,
        ignoreZeros                = args.ignore_zeros,
        verifyModificationTime     = args.verify_mtime,
        stripRecursiveTarExtension = args.strip_recursive_tar_extension,
        indexFilePath              = args.index_file,
        indexFolders               = args.index_folders,
        lazyMounting               = args.lazy,
        passwords                  = args.passwords,
        parallelization            = args.parallelization,
        isGnuIncremental           = args.gnu_incremental,
        printDebug                 = args.debug,
        # fmt: on
    )

    fuse.FUSE(
        # fmt: on
        operations=fuseOperationsObject,
        mountpoint=args.mount_point,
        foreground=args.foreground,
        nothreads=True,  # Can't access SQLite database connection object from multiple threads
        # fmt: off
        **fusekwargs
    )


if __name__ == '__main__':
    cli(sys.argv[1:])

# PYTHON_ARGCOMPLETE_OK

# We explicitly do want to import everything as late as possible here in order to speed up calls by argcomplete!
# pylint: disable=import-outside-toplevel

import argparse
import contextlib
import json
import math
import os
import sys
import tarfile
import tempfile
import traceback
from typing import List, Optional

from ratarmountcore.utils import RatarmountError, get_xdg_cache_home

with contextlib.suppress(ImportError):
    import argcomplete


class _CustomFormatter(argparse.ArgumentDefaultsHelpFormatter, argparse.RawDescriptionHelpFormatter):
    def add_arguments(self, actions):
        actions = sorted(actions, key=lambda action: action.option_strings)
        super().add_arguments(actions)


class PrintVersionAction(argparse.Action):
    def __call__(self, parser, args, values, option_string=None):
        from .actions import print_versions

        print_versions()
        parser.exit()


class PrintOSSAttributionAction(argparse.Action):
    def __call__(self, parser, args, values, option_string=None):
        from .actions import print_oss_attributions

        print_oss_attributions()
        parser.exit()


class PrintOSSAttributionShortAction(argparse.Action):
    def __call__(self, parser, args, values, option_string=None):
        from .actions import print_oss_attributions_short

        print_oss_attributions_short()
        parser.exit()


def _parse_args(rawArgs: Optional[List[str]] = None):
    parser = argparse.ArgumentParser(
        prog='ratarmount',
        formatter_class=_CustomFormatter,
        add_help=False,
        description='''\
With ratarmount, you can:
  - Mount an archive to a folder for read-only access
  - Mount a compressed file to `<mountpoint>/<filename>`
  - Bind-mount a folder to another folder for read-only access
  - Union mount a list of archives, compressed files, and folders to a mount point
    for read-only access
  - Mount an archive with a write-overlay mapped to a folder for read-write access
  - Remotely mount an archive from the internet via https:// for read-only access
  - And much more
''',
        # The examples should be kept synchronized with the README.md!
        epilog='''\
Examples:

 - ratarmount archive.tar.gz
 - ratarmount --recursive archive.tar mountpoint
 - ratarmount --unmount mountpoint mountpoint2
 - ratarmount folder mountpoint
 - ratarmount folder1 folder2 mountpoint
 - ratarmount folder archive.zip folder
 - ratarmount --recursive folder-with-many-archives mountpoint
 - ratarmount -o modules=subdir,subdir=squashfs-root archive.squashfs mountpoint
 - ratarmount http://server.org:80/archive.rar folder folder
 - ratarmount ssh://hostname:22/relativefolder/ mountpoint
 - ratarmount ssh://hostname:22//tmp/tmp-abcdef/ mountpoint
 - ratarmount github://mxmlnkn:ratarmount@v0.15.2/tests/single-file.tar mountpoint
 - AWS_ACCESS_KEY_ID=aaaaaaaaaaaaaaaaaaaa AWS_SECRET_ACCESS_KEY=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb \\
   ratarmount s3://127.0.0.1/bucket/single-file.tar mounted

For further information, see the ReadMe on the project's homepage:

    https://github.com/mxmlnkn/ratarmount
''',
    )

    commonGroup = parser.add_argument_group("Optional Arguments")
    positionalGroup = parser.add_argument_group("Positional Options")
    indexGroup = parser.add_argument_group("Index Options")
    recursionGroup = parser.add_argument_group("Recursion Options")
    tarGroup = parser.add_argument_group("Tar Options")
    writeGroup = parser.add_argument_group("Write Overlay Options")
    advancedGroup = parser.add_argument_group("Advanced Options")

    defaultParallelization = len(os.sched_getaffinity(0)) if hasattr(os, 'sched_getaffinity') else os.cpu_count()

    # https://github.com/kislyuk/argcomplete/blob/a2b8bc6461bcfc919bc3f4b3f83c7716bd078583/argcomplete/finders.py#L117
    backendNames: List[str] = []
    DEFAULT_GZIP_SEEK_POINT_SPACING = 16 * 1024 * 1024
    if "_ARGCOMPLETE" not in os.environ:
        # Expensive imports because they import all required modules for each format.
        from ratarmountcore.compressions import COMPRESSION_BACKENDS
        from ratarmountcore.mountsource.archives import ARCHIVE_BACKENDS

        backendNames = sorted(set(ARCHIVE_BACKENDS.keys()).union(set(COMPRESSION_BACKENDS.keys())))

        from ratarmountcore.mountsource.formats.tar import SQLiteIndexedTar

        DEFAULT_GZIP_SEEK_POINT_SPACING = SQLiteIndexedTar.DEFAULT_GZIP_SEEK_POINT_SPACING

    # fmt: off
    commonGroup.add_argument(
        '-h', '--help', action='help', default=argparse.SUPPRESS,
        help='Show this help message and exit.')

    indexGroup.add_argument(
        '-c', '--recreate-index', action='store_true', default=False,
        help='If specified, pre-existing .index files will be deleted and newly created.')

    commonGroup.add_argument(
        '-r', '--recursive', action='store_true', default=False,
        help='Mount archives inside archives recursively. Same as --recursion-depth -1.')

    commonGroup.add_argument(
        '-u', '--unmount', action='store_true',
        help='Unmount the given mount point(s). Equivalent to calling "fusermount -u" for each mount point.')

    commonGroup.add_argument(
        '-P', '--parallelization', type=str, default=":1,rapidgzip-bzip2:0",
        help='If an integer other than 1 is specified, then the threaded parallel decoders will use the '
             'specified amount of block decoder threads. Further threads with lighter work may be started. '
             f'A value of 0 will use all the available cores ({defaultParallelization}). Fine-granular '
             'parallelization for each backend can be specified with: \n'
             '"<backend>:<parallelization>,:<default parallelization>,<backend 2>:<parallelization>,..."')

    commonGroup.add_argument(
        '-v', '--version', action=PrintVersionAction, nargs=0, default=argparse.SUPPRESS,
        help='Print version information and exit.')

    commonGroup.add_argument(
        '--password', type=str, default='',
        help='Specify a single password which shall be used for RAR and ZIP files.')

    # Index Options

    indexGroup.add_argument(
        '--verify-mtime', action='store_true',
        help='By default, only the TAR file size is checked to match the one in the found existing ratarmount index. '
             'If this option is specified, then also check the modification timestamp. But beware that the mtime '
             'might change during copying or downloading without the contents changing. So, this check might cause '
             'false positives.')

    indexGroup.add_argument(
        '--index-file', type=str,
        help='Specify a path to the .index.sqlite file. Setting this will disable fallback index folders. '
             'If the given path is ":memory:", then the index will not be written out to disk. '
             'If the specified path is a remote URL, such as "https://host.org/file.tar.index.sqlite", or '
             'a compressed index, such as "file.tar.index.sqlite.gz", then the index file will be downloaded '
             f'and/or extracted into the default temporary folder ({tempfile.gettempdir()}). This path can be '
             'changed with the environment variable RATARMOUNT_INDEX_TMPDIR. The temporary folder in general '
             'can also be changed with these environment variables in decreasing priority: TMPDIR, TEMP, TMP '
             'as described in the Python tempfile standard library documentation.')

    indexFolders = ['', os.path.join( "~", ".ratarmount")]
    xdgCacheHome = get_xdg_cache_home()
    if xdgCacheHome and os.path.isdir(os.path.expanduser(xdgCacheHome)):
        indexFolders.insert(1, os.path.join(xdgCacheHome, 'ratarmount'))
    containsComma = any(',' in folder for folder in indexFolders)
    indexFoldersAsString = json.dumps(indexFolders) if containsComma else ','.join(indexFolders)

    indexGroup.add_argument(
        '--index-folders', default=indexFoldersAsString,
        help='Specify one or multiple paths for storing .index.sqlite files. Paths will be tested for suitability '
             'in the given order. An empty path will be interpreted as the location in which the TAR resides. '
             'If the argument begins with a bracket "[", then it will be interpreted as a JSON-formatted list. '
             'If the argument contains a comma ",", it will be interpreted as a comma-separated list of folders. '
             'Else, the whole string will be interpreted as one folder path. Examples: '
             '--index-folders ",~/.foo" will try to save besides the TAR and if that does not work, in ~/.foo. '
             '--index-folders \'["~/.ratarmount", "foo,9000"]\' will never try to save besides the TAR. '
             '--index-folder ~/.ratarmount will only test ~/.ratarmount as a storage location and nothing else. '
             'Instead, it will first try ~/.ratarmount and the folder "foo,9000". ')

    # Recursion Options

    # TODO The recursion depth is only heeded by AutoMountLayer but not by SQLiteIndexedTar.
    #      One problem is that it requires an update to the index metadata information and
    #      the other problem is that the AutoMountLayer would have to ask how deep the recursion
    #      for a particular path is so that it can correctly stop recursive mounting and the
    #      combined recursion depth.
    recursionGroup.add_argument(
        '--recursion-depth', type=int, default=None,
        help='This option takes precedence over --recursive. '
             'Mount archives inside the mounted archives recursively up to the given depth. '
             'A negative value represents infinite depth. '
             'A value of 0 will turn off recursion (same as not specifying --recursive in the first place). '
             'A value of 1 will recursively mount all archives in the given archives but not any deeper. '
             'Note that this only has an effect when creating an index. '
             'If an index already exists, then this option will be effectively ignored. '
             'Recreate the index if you want change the recursive mounting policy anyways.')

    recursionGroup.add_argument(
        '-l', '--lazy', action='store_true', default=False,
        help='When used with recursively bind-mounted folders, TAR files inside the mounted folder will only be '
             'mounted on first access to it.')

    recursionGroup.add_argument(
        '-s', '--strip-recursive-tar-extension', action='store_true',
        help='If true, then recursively mounted TARs named <file>.tar will be mounted at <file>/. '
             'This might lead to folders of the same name being overwritten, so use with care. '
             'The index needs to be (re)created to apply this option!')

    recursionGroup.add_argument(
        '--transform-recursive-mount-point', type=str, nargs=2, metavar=('REGEX_PATTERN', 'REPLACEMENT'),
        help='Specify a regex pattern and a replacement string, which will be applied via Python\'s re module '
             'to the full path of the archive to be recursively mounted. E.g., if there are recursive archives: '
             '/folder/archive.tar.gz, you can substitute \'[.][^/]+$\' to \'\' and it will be mounted to '
             '/folder/archive.tar. Or you can replace \'^.*/([^/]+).tar.gz$\' to \'/\1\' to mount all recursive '
             'folders under the top-level without extensions.')

    # TAR Options

    tarGroup.add_argument(
        '-e', '--encoding', type=str, default=tarfile.ENCODING,
        help='Specify an input encoding used for file names among others in the TAR. '
             'This must be used when, e.g., trying to open a latin1 encoded TAR on an UTF-8 system. '
             'Possible encodings: https://docs.python.org/3/library/codecs.html#standard-encodings')

    tarGroup.add_argument(
        '-i', '--ignore-zeros', action='store_true',
        help='Ignore zeroed blocks in archive. Normally, two consecutive 512-blocks filled with zeroes mean EOF '
             'and ratarmount stops reading after encountering them. This option instructs it to read further and '
             'is useful when reading archives created with the -A option.')

    tarGroup.add_argument(
        '--gnu-incremental', dest='gnu_incremental', action='store_true', default=False,
        help='Will strip octal modification time prefixes from file paths, which appear in GNU incremental backups '
             'created with GNU tar with the --incremental or --listed-incremental options.')

    tarGroup.add_argument(
        '--no-gnu-incremental', dest='gnu_incremental', action='store_false', default=False,
        help='If specified, will never strip octal modification prefixes and will also not do automatic detection.')

    tarGroup.add_argument(
        '--detect-gnu-incremental', dest='gnu_incremental', action='store_const', const=None, default=False,
        help='If specified, will automatically try to detect GNU tar incremental files and, if so, will strip '
             'octal modification prefixes. Note that this is only a heuristic derived by testing 1000-10000 file '
             'entries. If you are sure it is an incremental TAR, use --gnu-incremental instead.')

    tarGroup.add_argument(
        '--resolve-symbolic-links', action='store_true', default=False,
        help='Resolve symbolic links transparently. When enabled, accessing a symbolic link will directly '
             'access the target file or directory instead of showing the link itself. This makes symbolic '
             'links appear as if they were regular files or directories.')

    # Write Overlay Options

    writeGroup.add_argument(
        '-w', '--write-overlay',
        help='Specify an existing folder to be used as a write overlay. The folder itself will be union-mounted '
             'on top such that files in this folder take precedence over all other existing ones. Furthermore, '
             'all file creations and modifications will be forwarded to files in this folder. '
             'Modifying a file inside a TAR will copy that file to the overlay folder and apply the modification '
             'to that writable copy. Deleting files or folders will update the hidden metadata database inside '
             'the overlay folder.')

    writeGroup.add_argument(
        '--commit-overlay', action='store_true', default=False,
        help='Apply deletions and content modifications done in the write overlay to the archive.')

    # Advanced Options

    advancedGroup.add_argument(
        '-o', '--fuse', type=str, default='',
        help='Comma separated FUSE options. See "man mount.fuse" for help. '
             'Example: --fuse "allow_other,entry_timeout=2.8,gid=0". ')

    advancedGroup.add_argument(
        '-f', '--foreground', action='store_true', default=False,
        help='Keeps the python program in foreground so it can print debug '
             'output when the mounted path is accessed.')

    advancedGroup.add_argument(
        '-d', '--debug', type=int, default=1,
        help='Sets the debugging level. Higher means more output. Currently, 3 is the highest.')

    advancedGroup.add_argument(
        '--log-file', type=str, default='',
        help='Specifies a file to redirect all output into. The redirection only takes effect after the mount point '
             'is provided because, without -f, there is no other way to get output after daemonization and forking '
             'into the background.')

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
    advancedGroup.add_argument(
        '-gs', '--gzip-seek-point-spacing', type=float,
        default=math.ceil(DEFAULT_GZIP_SEEK_POINT_SPACING / 1024 / 1024),
        help='This only is applied when the index is first created or recreated with the -c option. '
             'The spacing given in MiB specifies the seek point distance in the uncompressed data. '
             'A distance of 16MiB means that archives smaller than 16MiB in uncompressed size will '
             'not benefit from faster seek times. A seek point takes roughly 32kiB. '
             'So, smaller distances lead to more responsive seeking but may explode the index size!')

    advancedGroup.add_argument(
        '-p', '--prefix', type=str, default='',
        help='[deprecated] Use "-o modules=subdir,subdir=<prefix>" instead. '
             'This standard way utilizes FUSE itself and will also work for other FUSE '
             'applications. So, it is preferable even if a bit more verbose.'
             'The specified path to the folder inside the TAR will be mounted to root. '
             'This can be useful when the archive as created with absolute paths. '
             'E.g., for an archive created with `tar -P cf /var/log/apt/history.log`, '
             '-p /var/log/apt/ can be specified so that the mount target directory '
             '>directly< contains history.log.')

    advancedGroup.add_argument(
        '--password-file', type=str, default='',
        help='Specify a file with newline separated passwords for RAR and ZIP files. '
             'The passwords will be tried out in order of appearance in the file.')

    advancedGroup.add_argument(
        '--use-backend', type=str, action='append',
        help='Specify a backend to be used with higher priority for files which might be opened with multiple '
             'backends. Arguments specified last will have the highest priority. A comma-separated list may be '
             f'specified. Possible backends: {backendNames}')

    advancedGroup.add_argument(
        '--oss-attributions-short', action=PrintOSSAttributionShortAction, nargs=0, default=argparse.SUPPRESS,
        help='Show license identifiers of used libraries.')

    advancedGroup.add_argument(
        '--oss-attributions', action=PrintOSSAttributionAction, nargs=0, default=argparse.SUPPRESS,
        help='Show licenses of used libraries.')

    advancedGroup.add_argument(
        '--disable-union-mount', action='store_true', default=False,
        help='Mounts all specified archives in equally named subfolders under the mount point.')

    advancedGroup.add_argument(
        '--union-mount-cache-max-depth', type=int, default=1024,
        help='Maximum number of folder levels to descend for building the union mount cache.')

    advancedGroup.add_argument(
        '--union-mount-cache-max-entries', type=int, default=100000,
        help='Maximum number of paths before stopping to descend into subfolders when building the union mount cache.')

    advancedGroup.add_argument(
        '--union-mount-cache-timeout', type=float, default=60,
        help='Timeout in seconds before stopping to build the union mount cache.')

    advancedGroup.add_argument(
        '--index-minimum-file-count', type=int, default=1000,
        help='Create indexes for archives with fewer than this limit of files in memory instead of '
             'creating a .index.sqlite file. This is currently not applied for TAR files because the file count '
             'only becomes known after parsing the archive, for which an index is already created.')

    advancedGroup.add_argument(
        '--transform', type=str, nargs=2, metavar=('REGEX_PATTERN', 'REPLACEMENT'),
        help='Specify a regex pattern and a replacement string, which will be applied via Python\'s re module '
             'to the full paths of all archive files.')

    # Positional Arguments

    positionalGroup.add_argument(
        'mount_source', nargs='+',
        help='The path to the TAR archive to be mounted. '
             'If multiple archives and/or folders are specified, then they will be mounted as if the arguments '
             'coming first were updated with the contents of the archives or folders specified thereafter, '
             'i.e., the list of TARs and folders will be union mounted.')
    positionalGroup.add_argument(
        'mount_point', nargs='?',
        help='The path to a folder to mount the TAR contents into. '
             'If no mount path is specified, the TAR will be mounted to a folder of the same name '
             'but without a file extension.')
    # fmt: on

    if 'argcomplete' in sys.modules:
        argcomplete.autocomplete(parser)
    return parser.parse_args(rawArgs)


def cli(rawArgs: Optional[List[str]] = None) -> int:
    """
    Command line interface for ratarmount. Call with args = [ '--help' ] for a description.

    rawArgs: In general, rawArgs is None, meaning sys.argv is used. When used programmatically with a custom
             list of arguments, the first argument should not be the path to the script / the executable,
             i.e., call either cli() or cli(sys.argv[1:])!
    """

    # Manually parse --debug argument in case argument parsing with argparse itself goes wrong.
    tmpArgs = rawArgs or sys.argv
    debug = 1
    for i in range(len(tmpArgs) - 1):
        if tmpArgs[i] in ['-d', '--debug'] and tmpArgs[i + 1].isdecimal():
            try:
                debug = int(tmpArgs[i + 1])
            except ValueError:
                continue

    try:
        args = _parse_args(rawArgs)
        from .actions import process_parsed_arguments

        return process_parsed_arguments(args)
    except (FileNotFoundError, RatarmountError, argparse.ArgumentTypeError, ValueError) as exception:
        print("[Error]", exception)
        if debug >= 3:
            traceback.print_exc()

    return 1

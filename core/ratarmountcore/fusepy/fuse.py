# Copyright (c) 2012 Terence Honles <terence@honles.com> (maintainer)
# Copyright (c) 2008 Giorgos Verigakis <verigak@gmail.com> (author)
#
# Permission to use, copy, modify, and distribute this software for any
# purpose with or without fee is hereby granted, provided that the above
# copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
# ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
# WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
# ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
# OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

# mypy: ignore-errors
# pylint: disable=too-many-public-methods,missing-module-docstring,line-too-long
# pylint: disable=invalid-name,missing-function-docstring,missing-class-docstring,
# pylint: disable=too-few-public-methods,unused-argument,too-many-arguments,too-many-lines

# Note that for ABI forward compatibility and other issues, most C-types should be initialized
# to 0 and the ctypes module does that for us out of the box!
# https://github.com/python/cpython/blob/f8a736b8e14ab839e1193cb1d3955b61c316d048/Lib/test/test_ctypes/test_numbers.py#L95

from __future__ import print_function, absolute_import, division

import ctypes
import errno
import logging
import os
import warnings

from ctypes import (
    CFUNCTYPE,
    POINTER,
    c_char_p,
    c_byte,
    c_size_t,
    c_ssize_t,
    c_int,
    c_uint,
    c_uint64,
    c_void_p,
)
from ctypes.util import find_library
from platform import machine, system
from signal import signal, SIGINT, SIG_DFL, SIGTERM
from stat import S_IFDIR


try:
    from functools import partial
except ImportError:
    # http://docs.python.org/library/functools.html#functools.partial
    def partial(func, *args, **keywords):
        def newfunc(*fargs, **fkeywords):
            newkeywords = keywords.copy()
            newkeywords.update(fkeywords)
            return func(*(args + fargs), **newkeywords)

        newfunc.func = func
        newfunc.args = args
        newfunc.keywords = keywords
        return newfunc


log = logging.getLogger("fuse")
_system = system()
_machine = machine()

if _system == 'Windows' or _system.startswith('CYGWIN'):
    # NOTE:
    #
    # sizeof(long)==4 on Windows 32-bit and 64-bit
    # sizeof(long)==4 on Cygwin 32-bit and ==8 on Cygwin 64-bit
    #
    # We have to fix up c_long and c_ulong so that it matches the
    # Cygwin (and UNIX) sizes when run on Windows.
    import sys

    if sys.maxsize > 0xFFFFFFFF:
        c_win_long = ctypes.c_int64
        c_win_ulong = ctypes.c_uint64
    else:
        c_win_long = ctypes.c_int32
        c_win_ulong = ctypes.c_uint32

if _system == 'Windows' or _system.startswith('CYGWIN'):

    class c_timespec(ctypes.Structure):
        _fields_ = [('tv_sec', c_win_long), ('tv_nsec', c_win_long)]

elif _system == 'OpenBSD':
    c_time_t = ctypes.c_int64

    class c_timespec(ctypes.Structure):
        _fields_ = [('tv_sec', c_time_t), ('tv_nsec', ctypes.c_long)]

else:

    class c_timespec(ctypes.Structure):
        _fields_ = [('tv_sec', ctypes.c_long), ('tv_nsec', ctypes.c_long)]


class c_utimbuf(ctypes.Structure):
    _fields_ = [('actime', c_timespec), ('modtime', c_timespec)]


# Beware that FUSE_LIBRARY_PATH path was unchecked! If it is set to libfuse3.so.3.14.0,
# then it will mount without error, but when trying to access the mount point, will give:
#     Uncaught exception from FUSE operation setxattr, returning errno.EINVAL:
#         'utf-8' codec can't decode byte 0xe8 in position 1: invalid continuation byte
#     Traceback (most recent call last):
#       File "fuse.py", line 820, in _wrapper
#         return func(*args, **kwargs) or 0
#                ^^^^^^^^^^^^^^^^^^^^^
#       File "fuse.py", line 991, in setxattr
#         name.decode(self.encoding),
#         ^^^^^^^^^^^^^^^^^^^^^^^^^^
#     UnicodeDecodeError: 'utf-8' codec can't decode byte 0xe8 in position 1:
#     invalid continuation byte
_libfuse_path = os.environ.get('FUSE_LIBRARY_PATH')
if not _libfuse_path:
    if _system == 'Darwin':
        # libfuse dependency
        _libiconv = ctypes.CDLL(find_library('iconv'), ctypes.RTLD_GLOBAL)

        _libfuse_path = (
            find_library('fuse4x') or find_library('osxfuse') or find_library('fuse') or find_library('fuse-t')
        )
    elif _system == 'Windows':
        # pytype: disable=module-attr
        try:
            import _winreg as reg  # pytype: disable=import-error
        except ImportError:
            import winreg as reg  # pytype: disable=import-error

        def Reg32GetValue(rootkey, keyname, valname):
            key, val = None, None
            try:
                key = reg.OpenKey(
                    rootkey, keyname, 0, reg.KEY_READ | reg.KEY_WOW64_32KEY
                )  # pytype: disable=import-error
                val = str(reg.QueryValueEx(key, valname)[0])
            except WindowsError:  # pylint: disable=undefined-variable  # pytype: disable=name-error
                pass
            finally:
                if key is not None:
                    reg.CloseKey(key)
            return val

        _libfuse_path = Reg32GetValue(reg.HKEY_LOCAL_MACHINE, r"SOFTWARE\WinFsp", r"InstallDir")
        if _libfuse_path:
            arch = "x64" if sys.maxsize > 0xFFFFFFFF else "x86"
            _libfuse_path += f"bin\\winfsp-{arch}.dll"
        # pytype: enable=module-attr
    else:
        _libfuse_path = find_library('fuse')
        if not _libfuse_path:
            _libfuse_path = find_library('fuse3')

if not _libfuse_path:
    raise EnvironmentError('Unable to find libfuse')
_libfuse = ctypes.CDLL(_libfuse_path)

if _system == 'Darwin' and hasattr(_libfuse, 'macfuse_version'):
    _system = 'Darwin-MacFuse'


def get_fuse_version(libfuse):
    version = libfuse.fuse_version()
    if version < 100:
        return version // 10, version % 10
    if version < 1000:
        return version // 100, version % 100
    raise AttributeError(f"Version {version} of found library {_libfuse._name} cannot be parsed!")


fuse_version_major, fuse_version_minor = get_fuse_version(_libfuse)
if fuse_version_major == 2 and fuse_version_minor < 6:
    raise AttributeError(
        f"Found library {_libfuse_path} is too old: {fuse_version_major}.{fuse_version_minor}. "
        "There have been several ABI breaks in each version. Libfuse < 2.6 is not supported!"
    )
if fuse_version_major != 2 and not (fuse_version_major == 3 and _system == 'Linux'):
    raise AttributeError(
        f"Found library {_libfuse_path} has wrong major version: {fuse_version_major}. " "Expected FUSE 2!"
    )


# Check FUSE major version changes by cloning https://github.com/libfuse/libfuse.git
# and check the diff with:
#     git diff -w fuse-2.9.9 fuse-3.0.0 include/fuse.h
# or with comments stripped and diffed:
#    colordiff <( gcc -fpreprocessed -dD -E -P -Wno-all -x c \
#                     <( git show fuse-2.9.9:include/fuse.h ) ) \
#              <( gcc -fpreprocessed -dD -E -P -Wno-all -x c \
#                 <( git show fuse-3.0.2:include/fuse.h ) )
# and repeat for fuse_common.h and possibly over included headers, or check
# the official changelog:
# https://github.com/libfuse/libfuse/blob/master/ChangeLog.rst#libfuse-300-2016-12-08
#
# Header changes summarized:
#  - Added enum fuse_readdir_flags, which is added as last argument to readdir.
#  - Added enum fuse_fill_dir_flags, which is added to the fuse_fill_dir_t
#    function callback argument to readdir.
#  - Removed fuse_operations.getdir and related types fuse_dirh_t.
#    Was already deprecated in favor of readdir.
#  - Added fuse_fill_dir_flags to fuse_dirfil_t callback function pointer that
#    is used for readdir.
#  - Added fuse_config struct, which is the new second parameter of
#    fuse_operations.init.
#  - Added new fuse_file_info struct (fuse_common.h), which is added as
#    additional arguments to:
#    - Added as last argument to: getattr, chmod, chown, truncate, utimens.
#    - This argument already existed for: open, read, write, flush, release, fsync,
#      opendir, readdir, releasedir, fsyncdir, create, ftruncate, fgetattr, lock,
#      ioctl, read_buf, fallocate, poll, write_buf, flock.
#  - Added unsigned int flags to rename.
#  - Removed utime in favor of utimens, which has been added in libFUSE 2.6.
#  - Removed deprecated functions: fuse_fs_fgetattr, fuse_fs_ftruncate,
#    fuse_invalidate, and fuse_is_lib_option.
#  - Removed flags from fuse_operations: flag_nullpath_ok, flag_nopath,
#    flag_utime_omit_ok, flag_reserved. These are now in the new fuse_config struct.
#  - Added version argument to fuse_main_real, but this should not be called directly
#    anyway and instead the fuse_main wrapper, which is unchanged should be called.
#  - Removed first argument struct fuse_chan* from fuse_new.
#  - Removed fuse_chan* return value from fuse_mount and fuse_chan* argument from
#    fuse_unmount (and moved both methods from fuse_common.h into fuse.h).
#  - Added int clone_fd argument to fuse_loop_mt.
#  - Added macro definitions for constants FUSE_CAP_* to fuse_common.h.
#  - Added fuse_apply_conn_info_opts to fuse_common.h, see the official changelog
#    for reasoning.

# Set non-FUSE-specific kernel type definitions.
if _system in ('Darwin', 'Darwin-MacFuse', 'FreeBSD'):
    ENOTSUP = 45

    c_dev_t = ctypes.c_int32
    c_fsblkcnt_t = ctypes.c_ulong
    c_fsfilcnt_t = ctypes.c_ulong
    c_gid_t = ctypes.c_uint32
    c_mode_t = ctypes.c_uint16
    c_off_t = ctypes.c_int64
    c_pid_t = ctypes.c_int32
    c_uid_t = ctypes.c_uint32
    setxattr_t = ctypes.CFUNCTYPE(
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_char_p,
        ctypes.POINTER(ctypes.c_byte),
        ctypes.c_size_t,
        ctypes.c_int,
        ctypes.c_uint32,
    )
    getxattr_t = ctypes.CFUNCTYPE(
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_char_p,
        ctypes.POINTER(ctypes.c_byte),
        ctypes.c_size_t,
        ctypes.c_uint32,
    )
    if _system == 'Darwin':
        _c_stat__fields_ = [
            ('st_dev', c_dev_t),
            ('st_mode', c_mode_t),
            ('st_nlink', ctypes.c_uint16),
            ('st_ino', ctypes.c_uint64),
            ('st_uid', c_uid_t),
            ('st_gid', c_gid_t),
            ('st_rdev', c_dev_t),
            ('st_atimespec', c_timespec),
            ('st_mtimespec', c_timespec),
            ('st_ctimespec', c_timespec),
            ('st_birthtimespec', c_timespec),
            ('st_size', c_off_t),
            ('st_blocks', ctypes.c_int64),
            ('st_blksize', ctypes.c_int32),
            ('st_flags', ctypes.c_int32),
            ('st_gen', ctypes.c_int32),
            ('st_lspare', ctypes.c_int32),
            ('st_qspare', ctypes.c_int64),
        ]
    else:
        _c_stat__fields_ = [
            ('st_dev', c_dev_t),
            ('st_ino', ctypes.c_uint32),
            ('st_mode', c_mode_t),
            ('st_nlink', ctypes.c_uint16),
            ('st_uid', c_uid_t),
            ('st_gid', c_gid_t),
            ('st_rdev', c_dev_t),
            ('st_atimespec', c_timespec),
            ('st_mtimespec', c_timespec),
            ('st_ctimespec', c_timespec),
            ('st_size', c_off_t),
            ('st_blocks', ctypes.c_int64),
            ('st_blksize', ctypes.c_int32),
        ]
elif _system == 'Linux':
    ENOTSUP = 95

    # sys/statvfs.h
    c_fsblkcnt_t = ctypes.c_ulonglong
    c_fsfilcnt_t = ctypes.c_ulonglong

    # https://man7.org/linux/man-pages/man0/sys_types.h.0p.html
    c_dev_t = ctypes.c_ulonglong
    c_gid_t = ctypes.c_uint
    c_mode_t = ctypes.c_uint
    c_off_t = ctypes.c_longlong
    c_pid_t = ctypes.c_int
    c_uid_t = ctypes.c_uint

    # sys/xattr.h
    setxattr_t = ctypes.CFUNCTYPE(
        ctypes.c_int, ctypes.c_char_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_byte), ctypes.c_size_t, ctypes.c_int
    )
    getxattr_t = ctypes.CFUNCTYPE(
        ctypes.c_int, ctypes.c_char_p, ctypes.c_char_p, ctypes.POINTER(ctypes.c_byte), ctypes.c_size_t
    )

    # sys/stat.h
    if _machine == 'x86_64':
        _c_stat__fields_ = [
            ('st_dev', c_dev_t),
            ('st_ino', ctypes.c_ulong),
            ('st_nlink', ctypes.c_ulong),
            ('st_mode', c_mode_t),
            ('st_uid', c_uid_t),
            ('st_gid', c_gid_t),
            ('__pad0', ctypes.c_int),
            ('st_rdev', c_dev_t),
            ('st_size', c_off_t),
            ('st_blksize', ctypes.c_long),
            ('st_blocks', ctypes.c_long),
            ('st_atimespec', c_timespec),
            ('st_mtimespec', c_timespec),
            ('st_ctimespec', c_timespec),
        ]
    elif _machine == 'mips':
        _c_stat__fields_ = [
            ('st_dev', c_dev_t),
            ('__pad1_1', ctypes.c_ulong),
            ('__pad1_2', ctypes.c_ulong),
            ('__pad1_3', ctypes.c_ulong),
            ('st_ino', ctypes.c_ulong),
            ('st_mode', c_mode_t),
            ('st_nlink', ctypes.c_ulong),
            ('st_uid', c_uid_t),
            ('st_gid', c_gid_t),
            ('st_rdev', c_dev_t),
            ('__pad2_1', ctypes.c_ulong),
            ('__pad2_2', ctypes.c_ulong),
            ('st_size', c_off_t),
            ('__pad3', ctypes.c_ulong),
            ('st_atimespec', c_timespec),
            ('__pad4', ctypes.c_ulong),
            ('st_mtimespec', c_timespec),
            ('__pad5', ctypes.c_ulong),
            ('st_ctimespec', c_timespec),
            ('__pad6', ctypes.c_ulong),
            ('st_blksize', ctypes.c_long),
            ('st_blocks', ctypes.c_long),
            ('__pad7_1', ctypes.c_ulong),
            ('__pad7_2', ctypes.c_ulong),
            ('__pad7_3', ctypes.c_ulong),
            ('__pad7_4', ctypes.c_ulong),
            ('__pad7_5', ctypes.c_ulong),
            ('__pad7_6', ctypes.c_ulong),
            ('__pad7_7', ctypes.c_ulong),
            ('__pad7_8', ctypes.c_ulong),
            ('__pad7_9', ctypes.c_ulong),
            ('__pad7_10', ctypes.c_ulong),
            ('__pad7_11', ctypes.c_ulong),
            ('__pad7_12', ctypes.c_ulong),
            ('__pad7_13', ctypes.c_ulong),
            ('__pad7_14', ctypes.c_ulong),
        ]
    elif _machine == 'ppc':
        _c_stat__fields_ = [
            ('st_dev', c_dev_t),
            ('st_ino', ctypes.c_ulonglong),
            ('st_mode', c_mode_t),
            ('st_nlink', ctypes.c_uint),
            ('st_uid', c_uid_t),
            ('st_gid', c_gid_t),
            ('st_rdev', c_dev_t),
            ('__pad2', ctypes.c_ushort),
            ('st_size', c_off_t),
            ('st_blksize', ctypes.c_long),
            ('st_blocks', ctypes.c_longlong),
            ('st_atimespec', c_timespec),
            ('st_mtimespec', c_timespec),
            ('st_ctimespec', c_timespec),
        ]
    elif _machine in ('ppc64', 'ppc64le'):
        _c_stat__fields_ = [
            ('st_dev', c_dev_t),
            ('st_ino', ctypes.c_ulong),
            ('st_nlink', ctypes.c_ulong),
            ('st_mode', c_mode_t),
            ('st_uid', c_uid_t),
            ('st_gid', c_gid_t),
            ('__pad', ctypes.c_uint),
            ('st_rdev', c_dev_t),
            ('st_size', c_off_t),
            ('st_blksize', ctypes.c_long),
            ('st_blocks', ctypes.c_long),
            ('st_atimespec', c_timespec),
            ('st_mtimespec', c_timespec),
            ('st_ctimespec', c_timespec),
        ]
    elif _machine == 'aarch64':
        _c_stat__fields_ = [
            ('st_dev', c_dev_t),
            ('st_ino', ctypes.c_ulong),
            ('st_mode', c_mode_t),
            ('st_nlink', ctypes.c_uint),
            ('st_uid', c_uid_t),
            ('st_gid', c_gid_t),
            ('st_rdev', c_dev_t),
            ('__pad1', ctypes.c_ulong),
            ('st_size', c_off_t),
            ('st_blksize', ctypes.c_int),
            ('__pad2', ctypes.c_int),
            ('st_blocks', ctypes.c_long),
            ('st_atimespec', c_timespec),
            ('st_mtimespec', c_timespec),
            ('st_ctimespec', c_timespec),
        ]
    else:
        # i686, use as fallback for everything else
        _c_stat__fields_ = [
            ('st_dev', c_dev_t),
            ('__pad1', ctypes.c_ushort),
            ('__st_ino', ctypes.c_ulong),
            ('st_mode', c_mode_t),
            ('st_nlink', ctypes.c_uint),
            ('st_uid', c_uid_t),
            ('st_gid', c_gid_t),
            ('st_rdev', c_dev_t),
            ('__pad2', ctypes.c_ushort),
            ('st_size', c_off_t),
            ('st_blksize', ctypes.c_long),
            ('st_blocks', ctypes.c_longlong),
            ('st_atimespec', c_timespec),
            ('st_mtimespec', c_timespec),
            ('st_ctimespec', c_timespec),
            ('st_ino', ctypes.c_ulonglong),
        ]
elif _system == 'Windows' or _system.startswith('CYGWIN'):
    ENOTSUP = 129 if _system == 'Windows' else 134
    c_dev_t = ctypes.c_uint
    c_fsblkcnt_t = c_win_ulong
    c_fsfilcnt_t = c_win_ulong
    c_gid_t = ctypes.c_uint
    c_mode_t = ctypes.c_uint
    c_off_t = ctypes.c_longlong
    c_pid_t = ctypes.c_int
    c_uid_t = ctypes.c_uint
    setxattr_t = ctypes.CFUNCTYPE(
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_char_p,
        ctypes.POINTER(ctypes.c_byte),
        ctypes.c_size_t,
        ctypes.c_int,
    )
    getxattr_t = ctypes.CFUNCTYPE(
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_char_p,
        ctypes.POINTER(ctypes.c_byte),
        ctypes.c_size_t,
    )
    _c_stat__fields_ = [
        ('st_dev', c_dev_t),
        ('st_ino', ctypes.c_ulonglong),
        ('st_mode', c_mode_t),
        ('st_nlink', ctypes.c_ushort),
        ('st_uid', c_uid_t),
        ('st_gid', c_gid_t),
        ('st_rdev', c_dev_t),
        ('st_size', c_off_t),
        ('st_atimespec', c_timespec),
        ('st_mtimespec', c_timespec),
        ('st_ctimespec', c_timespec),
        ('st_blksize', ctypes.c_int),
        ('st_blocks', ctypes.c_longlong),
        ('st_birthtimespec', c_timespec),
    ]
elif _system == 'OpenBSD':
    ENOTSUP = 91
    c_dev_t = ctypes.c_int32
    c_uid_t = ctypes.c_uint32
    c_gid_t = ctypes.c_uint32
    c_mode_t = ctypes.c_uint32
    c_off_t = ctypes.c_int64
    c_pid_t = ctypes.c_int32
    c_ino_t = ctypes.c_uint64
    c_nlink_t = ctypes.c_uint32
    c_blkcnt_t = ctypes.c_int64
    c_blksize_t = ctypes.c_int32
    setxattr_t = ctypes.CFUNCTYPE(
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_char_p,
        ctypes.POINTER(ctypes.c_byte),
        ctypes.c_size_t,
        ctypes.c_int,
    )
    getxattr_t = ctypes.CFUNCTYPE(
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_char_p,
        ctypes.POINTER(ctypes.c_byte),
        ctypes.c_size_t,
    )
    c_fsblkcnt_t = ctypes.c_uint64
    c_fsfilcnt_t = ctypes.c_uint64
    _c_stat__fields_ = [
        ('st_mode', c_mode_t),
        ('st_dev', c_dev_t),
        ('st_ino', c_ino_t),
        ('st_nlink', c_nlink_t),
        ('st_uid', c_uid_t),
        ('st_gid', c_gid_t),
        ('st_rdev', c_dev_t),
        ('st_atimespec', c_timespec),
        ('st_mtimespec', c_timespec),
        ('st_ctimespec', c_timespec),
        ('st_size', c_off_t),
        ('st_blocks', c_blkcnt_t),
        ('st_blksize', c_blksize_t),
        ('st_flags', ctypes.c_uint32),
        ('st_gen', ctypes.c_uint32),
        ('st_birthtimespec', c_timespec),
    ]
else:
    raise NotImplementedError(_system + ' is not supported.')


class c_stat(ctypes.Structure):
    _fields_ = _c_stat__fields_


# https://pubs.opengroup.org/onlinepubs/9699919799/basedefs/sys_statvfs.h.html
if _system == 'FreeBSD':
    c_fsblkcnt_t = ctypes.c_uint64
    c_fsfilcnt_t = ctypes.c_uint64
    setxattr_t = ctypes.CFUNCTYPE(
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_char_p,
        ctypes.POINTER(ctypes.c_byte),
        ctypes.c_size_t,
        ctypes.c_int,
    )

    getxattr_t = ctypes.CFUNCTYPE(
        ctypes.c_int,
        ctypes.c_char_p,
        ctypes.c_char_p,
        ctypes.POINTER(ctypes.c_byte),
        ctypes.c_size_t,
    )

    # https://github.com/freebsd/freebsd-src/blob/b1c3a4d75f4ff74218434a11cdd4e56632e13711/sys/sys/statvfs.h#L57-L68
    class c_statvfs(ctypes.Structure):
        _fields_ = [
            ('f_bavail', c_fsblkcnt_t),
            ('f_bfree', c_fsblkcnt_t),
            ('f_blocks', c_fsblkcnt_t),
            ('f_favail', c_fsfilcnt_t),
            ('f_ffree', c_fsfilcnt_t),
            ('f_files', c_fsfilcnt_t),
            ('f_bsize', ctypes.c_ulong),
            ('f_flag', ctypes.c_ulong),
            ('f_frsize', ctypes.c_ulong),
        ]

elif _system == 'Windows' or _system.startswith('CYGWIN'):

    class c_statvfs(ctypes.Structure):
        _fields_ = [
            ('f_bsize', c_win_ulong),
            ('f_frsize', c_win_ulong),
            ('f_blocks', c_fsblkcnt_t),
            ('f_bfree', c_fsblkcnt_t),
            ('f_bavail', c_fsblkcnt_t),
            ('f_files', c_fsfilcnt_t),
            ('f_ffree', c_fsfilcnt_t),
            ('f_favail', c_fsfilcnt_t),
            ('f_fsid', c_win_ulong),
            ('f_flag', c_win_ulong),
            ('f_namemax', c_win_ulong),
        ]

else:
    # https://sourceware.org/git?p=glibc.git;a=blob;f=bits/statvfs.h;h=ea89d9004d834c81874de00b5e3f5617d3096ccc;hb=HEAD#l33
    class c_statvfs(ctypes.Structure):
        _fields_ = [
            ('f_bsize', ctypes.c_ulong),
            ('f_frsize', ctypes.c_ulong),
            ('f_blocks', c_fsblkcnt_t),
            ('f_bfree', c_fsblkcnt_t),
            ('f_bavail', c_fsblkcnt_t),
            ('f_files', c_fsfilcnt_t),
            ('f_ffree', c_fsfilcnt_t),
            ('f_favail', c_fsfilcnt_t),
            ('f_fsid', ctypes.c_ulong),
            ('f_flag', ctypes.c_ulong),
            ('f_namemax', ctypes.c_ulong),
        ]


if _system == 'Linux':
    # https://github.com/torvalds/linux/blob/20371ba120635d9ab7fc7670497105af8f33eb08/include/uapi/asm-generic/fcntl.h#L195
    class c_flock_t(ctypes.Structure):
        _fields_ = [
            ('l_type', ctypes.c_short),
            ('l_whence', ctypes.c_short),
            ('l_start', c_off_t),
            ('l_len', c_off_t),
            ('l_pid', c_pid_t),
            ('l_sysid', ctypes.c_long),  # not always present
        ]

elif _system == 'OpenBSD':
    # https://github.com/openbsd/src/blob/a465f6177bcfdb2ffa9f98c7ca0780392688fc0d/sys/sys/fcntl.h#L180
    class c_flock_t(ctypes.Structure):
        _fields_ = [
            ('l_start', c_off_t),  # starting offset
            ('l_len', c_off_t),  # len = 0 means until end of file
            ('l_pid', c_pid_t),  # lock owner
            ('l_type', ctypes.c_short),  # lock type: read/write, etc.
            ('l_whence', ctypes.c_short),  # type of l_start
        ]

else:
    c_flock_t = ctypes.c_void_p


# fuse_file_info as defined in fuse_common.h. Changes in FUSE 3:
#  - fh_old was removed
#  - poll_events was added
#  - writepage was added to the bitfield, but the padding was not decreased,
#    so now there are 33 bits in total, which probably lead to some unwanted
#    padding in libfuse 3.0.2. This has been made explicit since libfuse 3.7.0:
#    https://github.com/libfuse/libfuse/commit/1d8e8ca94a3faa635afd1a3bd8d7d26472063a3f
#  - 3.0.0 -> 3.4.2: no change (flags and padding did add up to 33 bits from the beginning)
#  - 3.4.2 -> 3.5.0: cache_readdir added correctly
#  - 3.5.0 -> 3.6.2: no change
#  - 3.6.2 -> 3.7.0: padding 2 was explicitly added but did exist because of alignment
#  - 3.7.0 -> 3.10.5: no change
#  - 3.10.5 -> 3.11.0: noflush added correctly
#  - 3.11.0 -> 3.13.1: no change
#  - 3.13.1 -> 3.14.1: parallel_direct_writes was added in the middle.
#                      Padding was correctly decreased by 1.
#  - 3.14.1 -> 3.16.2: no change
_fuse_int32 = ctypes.c_int32 if (fuse_version_major, fuse_version_minor) >= (3, 17) else ctypes.c_int
_fuse_uint32 = ctypes.c_uint32 if (fuse_version_major, fuse_version_minor) >= (3, 17) else ctypes.c_uint
if fuse_version_major == 2:
    _fuse_file_info_fields_ = [
        ('flags', ctypes.c_int),
        ('fh_old', ctypes.c_ulong),
        ('writepage', ctypes.c_int),
        ('direct_io', ctypes.c_uint, 1),  # Introduced in libfuse 2.4
        ('keep_cache', ctypes.c_uint, 1),  # Introduced in libfuse 2.4
        ('flush', ctypes.c_uint, 1),  # Introduced in libfuse 2.6
        ('nonseekable', ctypes.c_uint, 1),  # Introduced in libfuse 2.8
        ('flock_release', ctypes.c_uint, 1),  # Introduced in libfuse 2.9
        ('padding', ctypes.c_uint, 27),
        ('fh', ctypes.c_uint64),
        ('lock_owner', ctypes.c_uint64),
    ]
elif fuse_version_major == 3:
    _fuse_file_info_fields_ = [('flags', _fuse_int32)]

    # Bit flag types were changed from unsigned int to uint32_t in libfuse 3.17,
    # but as far as I understand this change does not matter because it is only 1 bit.

    _fuse_file_info_fields_bitfield = [
        ('writepage', _fuse_uint32, 1),
        ('direct_io', _fuse_uint32, 1),
        ('keep_cache', _fuse_uint32, 1),
    ]
    # Introduced in 3.15.0 and its placement is an API-incompatible change / bug!
    # https://github.com/libfuse/libfuse/issues/1029
    if fuse_version_minor >= 15 and fuse_version_minor < 17:
        _fuse_file_info_fields_bitfield += [
            ('parallel_direct_writes', _fuse_uint32, 1),
        ]
    _fuse_file_info_fields_bitfield += [
        ('flush', _fuse_uint32, 1),
        ('nonseekable', _fuse_uint32, 1),
        ('flock_release', _fuse_uint32, 1),
    ]
    if fuse_version_minor >= 5:
        _fuse_file_info_fields_bitfield += [('cache_readdir', ctypes.c_uint, 1)]
    if fuse_version_minor >= 11:
        _fuse_file_info_fields_bitfield += [('noflush', ctypes.c_uint, 1)]
    if fuse_version_minor >= 17:
        _fuse_file_info_fields_bitfield += [
            ('parallel_direct_writes', _fuse_uint32, 1),
        ]

    _fuse_file_info_flag_count = sum(x[2] for x in _fuse_file_info_fields_bitfield)
    assert _fuse_file_info_flag_count < ctypes.sizeof(_fuse_uint32) * 8

    _fuse_file_info_fields_ += _fuse_file_info_fields_bitfield
    _fuse_file_info_fields_ += [
        ('padding', _fuse_uint32, ctypes.sizeof(_fuse_uint32) * 8 - _fuse_file_info_flag_count),
        ('padding2', _fuse_uint32),
    ]
    # https://github.com/libfuse/libfuse/pull/1038#discussion_r1775112524
    # https://github.com/libfuse/libfuse/pull/1081
    # This padding did always exist because fh was aligned to an offset modulo 8 B,
    # but libfuse 3.17 made this explicit and I'm not fully sure how ctypes behaves.
    if fuse_version_minor >= 17:
        _fuse_file_info_fields_ += [('padding3', _fuse_uint32)]

    _fuse_file_info_fields_ += [
        ('fh', ctypes.c_uint64),
        ('lock_owner', ctypes.c_uint64),
        ('poll_events', ctypes.c_uint64),
    ]


class fuse_file_info(ctypes.Structure):
    _fields_ = _fuse_file_info_fields_


if ctypes.sizeof(ctypes.c_int) == 4 and (fuse_version_major, fuse_version_minor) >= (3, 17):
    assert ctypes.sizeof(fuse_file_info) == 40


class fuse_context(ctypes.Structure):
    _fields_ = [
        ('fuse', ctypes.c_void_p),
        ('uid', c_uid_t),
        ('gid', c_gid_t),
        ('pid', c_pid_t),
        ('private_data', ctypes.c_void_p),
        # Added in 2.8. Note that this is an ABI break because programs compiled against 2.7
        # will allocate a smaller struct leading to out-of-bound accesses when used for a 2.8
        # shared library! It shouldn't hurt the other way around to have a larger struct than
        # the shared library expects. The newer members will simply be ignored.
        ('umask', c_mode_t),
    ]


_libfuse.fuse_get_context.restype = ctypes.POINTER(fuse_context)


# FUSE_BUF_IS_FD    = (1 << 1),
# FUSE_BUF_FD_SEEK  = (1 << 2),
# FUSE_BUF_FD_RETRY = (1 << 3),
fuse_buf_flags = ctypes.c_int


class fuse_buf(ctypes.Structure):
    _fields_ = [
        ('size', ctypes.c_size_t),
        ('flags', fuse_buf_flags),
        ('mem', ctypes.c_void_p),
        ('fd', ctypes.c_int),
        ('pos', c_off_t),
    ]


class fuse_bufvec(ctypes.Structure):
    _fields_ = [
        ('count', ctypes.c_size_t),
        ('idx', ctypes.c_size_t),
        ('off', ctypes.c_size_t),
        ('buf', ctypes.POINTER(fuse_buf)),
    ]


# fuse_conn_info struct as defined and documented in fuse_common.h
_fuse_conn_info_fields = [
    ('proto_major', ctypes.c_uint),
    ('proto_minor', ctypes.c_uint),
]
if fuse_version_major == 2:
    _fuse_conn_info_fields += [('async_read', _fuse_uint32)]
_fuse_conn_info_fields += [('max_write', _fuse_uint32)]
if fuse_version_major == 3:
    _fuse_conn_info_fields += [('max_read', _fuse_uint32)]
_fuse_conn_info_fields += [
    ('max_readahead', _fuse_uint32),
    ('capable', _fuse_uint32),  # Added in 2.8
    ('want', _fuse_uint32),  # Added in 2.8
    ('max_background', _fuse_uint32),  # Added in 2.9
    ('congestion_threshold', _fuse_uint32),  # Added in 2.9
]
if fuse_version_major == 2:
    _fuse_conn_info_fields += [('reserved', _fuse_uint32 * 23)]
elif fuse_version_major == 3:
    _fuse_conn_info_fields += [('time_gran', _fuse_uint32)]
    if fuse_version_minor < 17:
        _fuse_conn_info_fields += [('reserved', _fuse_uint32 * 22)]
    else:
        _fuse_conn_info_fields += [
            ('max_backing_stack_depth', ctypes.c_uint32),
            ('no_interrupt', ctypes.c_uint32, 1),
            ('padding', ctypes.c_uint32, 31),
            ('capable_ext', ctypes.c_uint64),
            ('want_ext', ctypes.c_uint64),
            ('request_timeout', ctypes.c_uint16),
            ('reserved', ctypes.c_uint16 * 31),
        ]


# https://github.com/libfuse/libfuse/pull/1081/commits/24f5b129c4e1b03ebbd05ac0c7673f306facea1ak
class fuse_conn_info(ctypes.Structure):  # Added in 2.6 (ABI break of "init" from 2.5->2.6)
    _fields_ = _fuse_conn_info_fields


if (fuse_version_major, fuse_version_minor) >= (3, 17):
    assert ctypes.sizeof(fuse_conn_info) == 128

# FUSE 3-only struct for second init argument defined in fuse.h.
# If a FUSE 2 method is loaded but 'init_with_config' overridden,
# then this argument will only be zero-initialized and should be ignored.
# 3.0.0 -> 3.10.5: no change
# 3.10.5 -> 3.11.0: no_rofd_flush was added in the middle causing an ABI break
# 3.11.0 -> 3.13.1: no change
# 3.13.1 -> 3.14.1: parallel_direct_writes was added in the middle causing an ABI break
# 3.14.1 -> 3.16.2: no change
# 3.16.2 -> 3.17.0: Changed all types to uint32_t, added reserved bytes, reverted order to 3.10.
# https://github.com/libfuse/libfuse/pull/1081
_fuse_config_fields_ = [
    ('set_gid', _fuse_int32),
    ('gid', _fuse_uint32),
    ('set_uid', _fuse_int32),
    ('uid', _fuse_uint32),
    ('set_mode', _fuse_int32),
    ('umask', _fuse_uint32),
    ('entry_timeout', ctypes.c_double),
    ('negative_timeout', ctypes.c_double),
    ('attr_timeout', ctypes.c_double),
    ('intr', _fuse_int32),
    ('intr_signal', _fuse_int32),
    ('remember', _fuse_int32),
    ('hard_remove', _fuse_int32),
    ('use_ino', _fuse_int32),
    ('readdir_ino', _fuse_int32),
    ('direct_io', _fuse_int32),
    ('kernel_cache', _fuse_int32),
    ('auto_cache', _fuse_int32),
]
if fuse_version_major == 3:
    # Adding this member in the middle of the struct was an ABI-incompatible change!
    if fuse_version_minor >= 11 and fuse_version_minor < 17:
        _fuse_config_fields_ += [('no_rofd_flush', ctypes.c_int)]

    _fuse_config_fields_ += [
        ('ac_attr_timeout_set', _fuse_int32),
        ('ac_attr_timeout', ctypes.c_double),
        ('nullpath_ok', _fuse_int32),
    ]

    # Another ABI break as discussed here: https://lists.debian.org/debian-devel/2024/03/msg00278.html
    # The break was in 3.14.1 NOT in 3.14.0, but I cannot query the bugfix version.
    # I'd hope that all 3.14.0 installations have been replaced by updates to 3.14.1.
    if fuse_version_minor >= 14 and fuse_version_minor < 17:
        _fuse_config_fields_ += [('parallel_direct_writes', ctypes.c_int)]

    _fuse_config_fields_ += [
        ('show_help', _fuse_int32),
        ('modules', ctypes.c_char_p),
        ('debug', _fuse_int32),
    ]

    if fuse_version_minor >= 17:
        _fuse_config_fields_ += [
            ('fmask', ctypes.c_uint32),
            ('dmask', ctypes.c_uint32),
            ('no_rofd_flush', ctypes.c_int32),
            ('parallel_direct_writes', ctypes.c_int32),
            ('flags', ctypes.c_int32),
            ('reserved', ctypes.c_uint64 * 48),
        ]


class fuse_config(ctypes.Structure):
    _fields_ = _fuse_config_fields_


fuse_pollhandle_p = ctypes.c_void_p  # Not exposed to API


# These are unchanged in FUSE 3 and therefore nice to have separate to reduce duplication.
_fuse_operations_fields_mknod_to_symlink = [
    ('mknod', CFUNCTYPE(c_int, c_char_p, c_mode_t, c_dev_t)),
    ('mkdir', CFUNCTYPE(c_int, c_char_p, c_mode_t)),
    ('unlink', CFUNCTYPE(c_int, c_char_p)),
    ('rmdir', CFUNCTYPE(c_int, c_char_p)),
    ('symlink', CFUNCTYPE(c_int, c_char_p, c_char_p)),
]
_fuse_operations_fields_open_to_removexattr = [
    ('open', CFUNCTYPE(c_int, c_char_p, POINTER(fuse_file_info))),
    ('read', CFUNCTYPE(c_int, c_char_p, POINTER(c_byte), c_size_t, c_off_t, POINTER(fuse_file_info))),
    ('write', CFUNCTYPE(c_int, c_char_p, POINTER(c_byte), c_size_t, c_off_t, POINTER(fuse_file_info))),
    ('statfs', CFUNCTYPE(c_int, c_char_p, POINTER(c_statvfs))),
    ('flush', CFUNCTYPE(c_int, c_char_p, POINTER(fuse_file_info))),
    ('release', CFUNCTYPE(c_int, c_char_p, POINTER(fuse_file_info))),
    ('fsync', CFUNCTYPE(c_int, c_char_p, c_int, POINTER(fuse_file_info))),
    ('setxattr', setxattr_t),
    ('getxattr', getxattr_t),
    ('listxattr', CFUNCTYPE(c_int, c_char_p, POINTER(c_byte), c_size_t)),
    ('removexattr', CFUNCTYPE(c_int, c_char_p, c_char_p)),
]
_fuse_operations_fields_2_9 = [
    (
        'poll',
        CFUNCTYPE(c_int, c_char_p, POINTER(fuse_file_info), fuse_pollhandle_p, POINTER(c_uint)),
    ),
    (
        'write_buf',
        CFUNCTYPE(c_int, c_char_p, POINTER(fuse_bufvec), c_off_t, POINTER(fuse_file_info)),
    ),
    (
        'read_buf',
        CFUNCTYPE(c_int, c_char_p, POINTER(POINTER(fuse_bufvec)), c_size_t, c_off_t, POINTER(fuse_file_info)),
    ),
    ('flock', CFUNCTYPE(c_int, c_char_p, POINTER(fuse_file_info), c_int)),
    (
        'fallocate',
        CFUNCTYPE(c_int, c_char_p, c_int, c_off_t, c_off_t, POINTER(fuse_file_info)),
    ),
]

if fuse_version_major == 2:
    _fuse_operations_fields = (
        [
            ('getattr', CFUNCTYPE(c_int, c_char_p, POINTER(c_stat))),
            ('readlink', CFUNCTYPE(c_int, c_char_p, POINTER(c_byte), c_size_t)),
            ('getdir', c_void_p),  # Deprecated, use readdir
        ]
        + _fuse_operations_fields_mknod_to_symlink
        + [
            ('rename', CFUNCTYPE(c_int, c_char_p, c_char_p)),
            ('link', CFUNCTYPE(c_int, c_char_p, c_char_p)),
            ('chmod', CFUNCTYPE(c_int, c_char_p, c_mode_t)),
            ('chown', CFUNCTYPE(c_int, c_char_p, c_uid_t, c_gid_t)),
            ('truncate', CFUNCTYPE(c_int, c_char_p, c_off_t)),
            ('utime', c_void_p),  # Deprecated, use utimens
        ]
        + _fuse_operations_fields_open_to_removexattr
    )
    if fuse_version_minor >= 3:
        _fuse_operations_fields += [
            ('opendir', CFUNCTYPE(c_int, c_char_p, POINTER(fuse_file_info))),
            (
                'readdir',
                CFUNCTYPE(
                    c_int,
                    c_char_p,
                    c_void_p,
                    CFUNCTYPE(c_int, c_void_p, c_char_p, POINTER(c_stat), c_off_t),
                    c_off_t,
                    POINTER(fuse_file_info),
                ),
            ),
            ('releasedir', CFUNCTYPE(c_int, c_char_p, POINTER(fuse_file_info))),
            ('fsyncdir', CFUNCTYPE(c_int, c_char_p, c_int, POINTER(fuse_file_info))),
            ('init', CFUNCTYPE(c_void_p, POINTER(fuse_conn_info))),
            ('destroy', CFUNCTYPE(c_void_p, c_void_p)),
        ]
    if fuse_version_minor >= 5:
        _fuse_operations_fields += [
            ('access', CFUNCTYPE(c_int, c_char_p, c_int)),
            ('create', CFUNCTYPE(c_int, c_char_p, c_mode_t, POINTER(fuse_file_info))),
            ('ftruncate', CFUNCTYPE(c_int, c_char_p, c_off_t, POINTER(fuse_file_info))),
            ('fgetattr', CFUNCTYPE(c_int, c_char_p, POINTER(c_stat), POINTER(fuse_file_info))),
        ]
    if fuse_version_minor >= 6:
        _fuse_operations_fields += [
            ('lock', CFUNCTYPE(c_int, c_char_p, POINTER(fuse_file_info), c_int, POINTER(c_flock_t))),
            ('utimens', CFUNCTYPE(c_int, c_char_p, POINTER(c_utimbuf))),
            ('bmap', CFUNCTYPE(c_int, c_char_p, c_size_t, POINTER(c_uint64))),
        ]
    if fuse_version_minor >= 8:
        _fuse_operations_fields += [
            ('flag_nullpath_ok', c_uint, 1),
            ('flag_nopath', c_uint, 1),
            ('flag_utime_omit_ok', c_uint, 1),
            ('flag_reserved', c_uint, 29),
            (
                'ioctl',
                CFUNCTYPE(c_int, c_char_p, c_uint, c_void_p, POINTER(fuse_file_info), c_uint, c_void_p),
            ),
        ]
    if fuse_version_minor >= 9:
        _fuse_operations_fields += _fuse_operations_fields_2_9
elif fuse_version_major == 3:
    fuse_fill_dir_flags = ctypes.c_int  # The only flag in libfuse 3.16 is USE_FILL_DIR_PLUS = (1 << 1).
    fuse_fill_dir_t = CFUNCTYPE(c_int, c_void_p, c_char_p, POINTER(c_stat), c_off_t, fuse_fill_dir_flags)

    fuse_readdir_flags = ctypes.c_int  # The only flag in libfuse 3.16 is FUSE_READDIR_PLUS = (1 << 0).

    # Generated bindings with:
    # gcc -fpreprocessed -dD -E -P -Wno-all -x c <( git show fuse-3.16.2:include/fuse.h ) 2>/dev/null |
    #   sed -nr '/struct fuse_operations/,$p' | sed -nr '0,/};/p' | sed -z 's|,\n *|, |g' |
    #   sed -r '
    #       s|const char [*]( ?[a-z_]+)?|ctypes.c_char_p|g;
    #       s|char [*]( ?[a-z_]+)?|ctypes.POINTER(ctypes.c_byte)|g;
    #       s|([( ])([a-z]+)_t( ?[a-z_]+)?|\1c_\2_t|g;
    #       s|([( ])unsigned int( ?[a-z_]+)?|\1ctypes.c_uint|g;
    #       s|([( ])int( ?[a-z_]+)?|\1ctypes.c_int|g;
    #       s|struct (fuse_[a-z_]+) [*][*]( ?[a-z_]+)?|ctypes.POINTER(ctypes.POINTER(\1))|g;
    #       s|struct (fuse_[a-z_]+) [*]( ?[a-z_]+)?|ctypes.POINTER(\1)|g;
    #       s|struct (stat(vfs)?) [*]( ?[a-z_]+)?|ctypes.POINTER(c_\1)|g;
    #       s|struct (flock) [*]( ?[a-z_]+)?|ctypes.POINTER(c_\1)|g;
    #       s|(u?int64)_t [*]( ?[a-z_]+)?|ctypes.POINTER(ctypes.c_\1)|g;
    #       s|void [*]( ?[a-z_]+)?|ctypes.c_void_p|g;
    #       s|const struct timespec tv[[]2[]]|ctypes.POINTER(c_utimbuf)|g;
    #       s|enum ([a-z_]+)|\1|g;
    #       s|^ *||;
    #   ' |
    #   sed -r "s|(^[a-z_.]+) [(][*]([a-z_]+)[)] [(](.*);|('\2', ctypes.CFUNCTYPE(\1, \3),|; s|ctypes[.]||g;"
    # Then fix the remaining problems by using pylint and by comparing with the FUSE 2 version.
    #
    # Removed members: getdir, utime, ftruncate, fgetattr, flag_nullpath_ok, flag_nopath, flag_utime_omit_ok
    #  - The methods were not used by fusepy anyway.
    #  - The flags were not exposed to fusepy callers because the fuse_operations struct is created,
    #    given to fuse_main_real, and then forgotten about in FUSE.__init__.
    # Methods with changed arguments:
    #  - getattr, rename, chmod, chown, truncate, readdir, init, utimens, ioctl
    # fmt: off
    _fuse_operations_fields = [
        ('getattr', CFUNCTYPE(c_int, c_char_p, POINTER(c_stat), POINTER(fuse_file_info))),         # Added file info
        ('readlink', CFUNCTYPE(c_int, c_char_p, POINTER(c_byte), c_size_t)),                       # Same as v2.9
    ] + _fuse_operations_fields_mknod_to_symlink + [
        ('rename', CFUNCTYPE(c_int, c_char_p, c_char_p, c_uint)),                                  # Added flags
        ('link', CFUNCTYPE(c_int, c_char_p, c_char_p)),                                            # Same as v2.9
        ('chmod', CFUNCTYPE(c_int, c_char_p, c_mode_t, POINTER(fuse_file_info))),                  # Added file info
        ('chown', CFUNCTYPE(c_int, c_char_p, c_uid_t, c_gid_t, POINTER(fuse_file_info))),          # Added file info
        ('truncate', CFUNCTYPE(c_int, c_char_p, c_off_t, POINTER(fuse_file_info))),                # Added file info
    ] + _fuse_operations_fields_open_to_removexattr + [
        ('opendir', CFUNCTYPE(c_int, c_char_p, POINTER(fuse_file_info))),                          # Same as v2.9
        ('readdir', CFUNCTYPE(
            c_int, c_char_p, c_void_p, fuse_fill_dir_t, c_off_t, POINTER(fuse_file_info),
            fuse_readdir_flags)),                                                                  # Added flags
        ('releasedir', CFUNCTYPE(c_int, c_char_p, POINTER(fuse_file_info))),                       # Same as v2.9
        ('fsyncdir', CFUNCTYPE(c_int, c_char_p, c_int, POINTER(fuse_file_info))),                  # Same as v2.9
        ('init', CFUNCTYPE(c_void_p, POINTER(fuse_conn_info), POINTER(fuse_config))),              # Added config
        ('destroy', CFUNCTYPE(c_void_p, c_void_p)),                                                # Same as v2.9
        ('access', CFUNCTYPE(c_int, c_char_p, c_int)),                                             # Same as v2.9
        ('create', CFUNCTYPE(c_int, c_char_p, c_mode_t, POINTER(fuse_file_info))),                 # Same as v2.9
        ('lock', CFUNCTYPE(c_int, c_char_p, POINTER(fuse_file_info), c_int, POINTER(c_flock_t))),  # Same as v2.9
        ('utimens', CFUNCTYPE(c_int, c_char_p, POINTER(c_utimbuf), POINTER(fuse_file_info))),      # Added file info
        ('bmap', CFUNCTYPE(c_int, c_char_p, c_size_t, POINTER(c_uint64))),                         # Same as v2.9
        ('ioctl', CFUNCTYPE(                                                                       # Argument type
            c_int, c_char_p, c_int if fuse_version_minor < 5 else c_uint, c_void_p,
            POINTER(fuse_file_info), c_uint, c_void_p)),
    ] + _fuse_operations_fields_2_9 + [
        (
            'copy_file_range',                                                                     # New
            CFUNCTYPE(
                c_ssize_t, c_char_p, POINTER(fuse_file_info), c_off_t, c_char_p,
                POINTER(fuse_file_info), c_off_t, c_size_t, c_int,
            ),
        ),
        ('lseek', CFUNCTYPE(c_off_t, c_char_p, c_off_t, c_int, POINTER(fuse_file_info))),          # New
    ]
    # fmt: on


class fuse_operations(ctypes.Structure):
    _fields_ = _fuse_operations_fields


if _system == "OpenBSD":

    def fuse_main_real(argc, argv, fuse_ops_v, sizeof_fuse_ops, ctx_p):
        return _libfuse.fuse_main(argc, argv, fuse_ops_v, ctx_p)

else:
    fuse_main_real = _libfuse.fuse_main_real


def time_of_timespec(ts, use_ns=False):
    if use_ns:
        return ts.tv_sec * 10**9 + ts.tv_nsec
    return ts.tv_sec + ts.tv_nsec / 1e9


def set_st_attrs(st, attrs, use_ns=False):
    for key, val in attrs.items():
        if key in ('st_atime', 'st_mtime', 'st_ctime', 'st_birthtime'):
            timespec = getattr(st, key + 'spec', None)
            if timespec is None:
                continue

            if use_ns:
                timespec.tv_sec, timespec.tv_nsec = divmod(int(val), 10**9)
            else:
                timespec.tv_sec = int(val)
                timespec.tv_nsec = int((val - timespec.tv_sec) * 1e9)
        elif getattr(st, key, None) is not None:
            setattr(st, key, val)


def fuse_get_context():
    'Returns a (uid, gid, pid) tuple'

    ctxp = _libfuse.fuse_get_context()
    ctx = ctxp.contents
    return ctx.uid, ctx.gid, ctx.pid


def fuse_exit():
    '''
    This will shutdown the FUSE mount and cause the call to FUSE(...) to
    return, similar to sending SIGINT to the process.

    Flags the native FUSE session as terminated and will cause any running FUSE
    event loops to exit on the next opportunity. (see fuse.c::fuse_exit)
    '''
    # OpenBSD doesn't have fuse_exit
    # instead fuse_loop() gracefully catches SIGTERM
    if _system == "OpenBSD":
        os.kill(os.getpid(), SIGTERM)
        return

    fuse_ptr = ctypes.c_void_p(_libfuse.fuse_get_context().contents.fuse)
    _libfuse.fuse_exit(fuse_ptr)


class FuseOSError(OSError):
    def __init__(self, errno):
        super().__init__(errno, os.strerror(errno))


class FUSE:
    '''
    This class is the lower level interface and should not be subclassed under
    normal use. Its methods are called by fuse.

    Assumes API version 2.6 or later.
    '''

    OPTIONS = (
        ('foreground', '-f'),
        ('debug', '-d'),
        ('nothreads', '-s'),
    )

    def __init__(self, operations, mountpoint, raw_fi=False, encoding='utf-8', **kwargs):
        '''
        Setting raw_fi to True will cause FUSE to pass the fuse_file_info
        class as is to Operations, instead of just the fh field.

        This gives you access to direct_io, keep_cache, etc.
        '''

        self.operations = operations
        self.raw_fi = raw_fi
        self.encoding = encoding
        self.__critical_exception = None

        self.use_ns = getattr(operations, 'use_ns', False)
        if not self.use_ns:
            warnings.warn(
                'Time as floating point seconds for utimens is deprecated!\n'
                'To enable time as nanoseconds set the property "use_ns" to '
                'True in your operations class or set your fusepy '
                'requirements to <4.',
                DeprecationWarning,
            )

        args = ['fuse']

        args.extend(flag for arg, flag in self.OPTIONS if kwargs.pop(arg, False))

        kwargs.setdefault('fsname', operations.__class__.__name__)
        args.append('-o')
        args.append(','.join(self._normalize_fuse_options(**kwargs)))
        args.append(mountpoint)

        args = [arg.encode(encoding) for arg in args]
        argv = (ctypes.c_char_p * len(args))(*args)

        fuse_ops = fuse_operations()
        for ent in fuse_operations._fields_:
            name, prototype = ent[:2]

            check_name = name

            # ftruncate()/fgetattr() are implemented in terms of their
            # non-f-prefixed versions in the operations object
            if check_name in ["ftruncate", "fgetattr"]:
                check_name = check_name[1:]

            val = getattr(operations, check_name, None)
            if val is None or getattr(val, 'libfuse_ignore', False):
                continue

            # Function pointer members are tested for using the
            # getattr(operations, name) above but are dynamically
            # invoked using self.operations(name)
            if hasattr(prototype, 'argtypes'):
                val = prototype(partial(self._wrapper, getattr(self, name)))

            setattr(fuse_ops, name, val)

        try:
            old_handler = signal(SIGINT, SIG_DFL)
        except ValueError:
            old_handler = SIG_DFL

        err = fuse_main_real(len(args), argv, ctypes.pointer(fuse_ops), ctypes.sizeof(fuse_ops), None)

        try:
            signal(SIGINT, old_handler)
        except ValueError:
            pass

        del self.operations  # Invoke the destructor
        if self.__critical_exception:
            raise self.__critical_exception
        if err:
            raise RuntimeError(err)

    @staticmethod
    def _normalize_fuse_options(**kargs):
        for key, value in kargs.items():
            if isinstance(value, bool):
                if value is True:
                    yield key
            else:
                yield f'{key}={value}'

    def _wrapper(self, func, *args, **kwargs):
        'Decorator for the methods that follow'

        # Catch exceptions generically so that the whole filesystem does not crash on each fusepy user
        # error. 'init' must not fail because its return code is just stored as private_data field of
        # struct fuse_contex.
        try:
            try:
                return func(*args, **kwargs) or 0

            except OSError as e:
                if func.__name__ == "init":
                    raise e
                if isinstance(e.errno, int) and e.errno > 0:
                    log.debug(
                        "FUSE operation %s raised a %s, returning errno %s.",
                        func.__name__,
                        type(e),
                        e.errno,
                        exc_info=True,
                    )
                    return -e.errno
                log.error(
                    "FUSE operation %s raised an OSError with negative errno %s, returning errno.EINVAL.",
                    func.__name__,
                    e.errno,
                    exc_info=True,
                )
                return -errno.EINVAL

            except Exception as e:
                if func.__name__ == "init":
                    raise e
                log.error(
                    "Uncaught exception from FUSE operation %s, returning errno.EINVAL.",
                    func.__name__,
                    exc_info=True,
                )
                return -errno.EINVAL

        except BaseException as e:
            self.__critical_exception = e
            log.critical(
                "Uncaught critical exception from FUSE operation %s, aborting.",
                func.__name__,
                exc_info=True,
            )
            # the raised exception (even SystemExit) will be caught by FUSE
            # potentially causing SIGSEGV, so tell system to stop/interrupt FUSE
            fuse_exit()
            return -errno.EFAULT

    def _decode_optional_path(self, path):
        # NB: this method is intended for fuse operations that
        #     allow the path argument to be NULL,
        #     *not* as a generic path decoding method
        if path is None:
            return None
        return path.decode(self.encoding)

    if fuse_version_major == 2:

        def getattr(self, path, buf):
            return self.fgetattr(path, buf, None)

    elif fuse_version_major == 3:

        def getattr(self, path, buf, fip):
            return self.fgetattr(path, buf, None)

    def readlink(self, path, buf, bufsize):
        ret = self.operations('readlink', path.decode(self.encoding)).encode(self.encoding)

        # copies a string into the given buffer
        # (null terminated and truncated if necessary)
        data = ctypes.create_string_buffer(ret[: bufsize - 1])
        ctypes.memmove(buf, data, len(data))
        return 0

    def mknod(self, path, mode, dev):
        return self.operations('mknod', path.decode(self.encoding), mode, dev)

    def mkdir(self, path, mode):
        return self.operations('mkdir', path.decode(self.encoding), mode)

    def unlink(self, path):
        return self.operations('unlink', path.decode(self.encoding))

    def rmdir(self, path):
        return self.operations('rmdir', path.decode(self.encoding))

    def symlink(self, source, target):
        'creates a symlink `target -> source` (e.g. ln -s source target)'

        return self.operations('symlink', target.decode(self.encoding), source.decode(self.encoding))

    def _rename(self, old, new):
        return self.operations('rename', old.decode(self.encoding), new.decode(self.encoding))

    if fuse_version_major == 2:
        rename = _rename
    elif fuse_version_major == 3:

        def rename(self, old, new, flags):
            self._rename(old, new)

    def link(self, source, target):
        'creates a hard link `target -> source` (e.g. ln source target)'

        return self.operations('link', target.decode(self.encoding), source.decode(self.encoding))

    if fuse_version_major == 2:

        def chmod(self, path, mode):
            return self.operations('chmod', path.decode(self.encoding), mode)

    elif fuse_version_major == 3:

        def chmod(self, path, mode, fip):
            return self.operations('chmod', path.decode(self.encoding), mode)

    def _chown(self, path, uid, gid):
        # Check if any of the arguments is a -1 that has overflowed
        if c_uid_t(uid + 1).value == 0:
            uid = -1
        if c_gid_t(gid + 1).value == 0:
            gid = -1

        return self.operations('chown', path.decode(self.encoding), uid, gid)

    if fuse_version_major == 2:

        def chown(self, path, uid, gid):
            return self._chown(path, uid, gid)

    elif fuse_version_major == 3:

        def chown(self, path, uid, gid, fip):
            return self._chown(path, uid, gid)

    if fuse_version_major == 2:

        def truncate(self, path, length):
            return self.operations('truncate', path.decode(self.encoding), length)

    elif fuse_version_major == 3:

        def truncate(self, path, length, fip):
            return self.operations('truncate', path.decode(self.encoding), length)

    def open(self, path, fip):
        fi = fip.contents
        if self.raw_fi:
            return self.operations('open', path.decode(self.encoding), fi)
        fi.fh = self.operations('open', path.decode(self.encoding), fi.flags)
        return 0

    def read(self, path, buf, size, offset, fip):
        fh = fip.contents if self.raw_fi else fip.contents.fh
        ret = self.operations('read', self._decode_optional_path(path), size, offset, fh)

        if not ret:
            return 0

        retsize = len(ret)
        assert retsize <= size, f'actual amount read {retsize} greater than expected {size}'

        ctypes.memmove(buf, ret, retsize)
        return retsize

    def write(self, path, buf, size, offset, fip):
        data = ctypes.string_at(buf, size)
        fh = fip.contents if self.raw_fi else fip.contents.fh
        return self.operations('write', self._decode_optional_path(path), data, offset, fh)

    def statfs(self, path, buf):
        stv = buf.contents
        attrs = self.operations('statfs', path.decode(self.encoding))
        for key, val in attrs.items():
            if hasattr(stv, key):
                setattr(stv, key, val)

        return 0

    def flush(self, path, fip):
        fh = fip.contents if self.raw_fi else fip.contents.fh
        return self.operations('flush', self._decode_optional_path(path), fh)

    def release(self, path, fip):
        fh = fip.contents if self.raw_fi else fip.contents.fh
        return self.operations('release', self._decode_optional_path(path), fh)

    def fsync(self, path, datasync, fip):
        fh = fip.contents if self.raw_fi else fip.contents.fh
        return self.operations('fsync', self._decode_optional_path(path), datasync, fh)

    def setxattr(self, path, name, value, size, options, *args):
        return self.operations(
            'setxattr',
            path.decode(self.encoding),
            name.decode(self.encoding),
            ctypes.string_at(value, size),
            options,
            *args,
        )

    def getxattr(self, path, name, value, size, *args):
        ret = self.operations('getxattr', path.decode(self.encoding), name.decode(self.encoding), *args)

        retsize = len(ret)
        # allow size queries
        if not value:
            return retsize

        # do not truncate
        if retsize > size:
            return -errno.ERANGE

        # Does not add trailing 0
        buf = ctypes.create_string_buffer(ret, retsize)
        ctypes.memmove(value, buf, retsize)

        return retsize

    def listxattr(self, path, namebuf, size):
        attrs = self.operations('listxattr', path.decode(self.encoding)) or ''
        ret = '\x00'.join(attrs).encode(self.encoding)
        if len(ret) > 0:
            ret += '\x00'.encode(self.encoding)

        retsize = len(ret)
        # allow size queries
        if not namebuf:
            return retsize

        # do not truncate
        if retsize > size:
            return -errno.ERANGE

        buf = ctypes.create_string_buffer(ret, retsize)
        ctypes.memmove(namebuf, buf, retsize)

        return retsize

    def removexattr(self, path, name):
        return self.operations('removexattr', path.decode(self.encoding), name.decode(self.encoding))

    def opendir(self, path, fip):
        # Ignore raw_fi
        fip.contents.fh = self.operations('opendir', path.decode(self.encoding))
        return 0

    # == About readdir and what should be returned ==
    #
    # Study the implementation in 2.9.9 in fuse.c
    # https://github.com/libfuse/libfuse/blob/fuse_2_9_bugfix/lib/fuse.c
    # Libfuse is split in a high-level and low-level API. We use the former, which calls the latter.
    # The call chain:
    #
    #  1. The fuse_lowlevel_ops.readdir callback is initialized with fuse_lib_readdir.
    #     This still gets an inode as argument from the low-level interface.
    #  2. readdir_fill: If the high-level API callback fuse_operations.readdir is set, then the inode is
    #     converted to a path via get_path -> get_path_common -> try_get_path -> get_node -> get_node_nocheck,
    #     which looks up f->id_table.array[hashid_hash(f, nodeid)], so yeah, basically a std::unorderd_map,
    #     and calls fuse_fs_readdir with the path.
    #  3. fuse_fs_readdir: calls the readdir callback if set, or the getdir callback with a "filler" callback.
    #  4. The filler callback is specified in readdir_fill and is simply fill_dir
    #  5. fill_dir copies the full struct stat argument if given and calls fuse_add_direntry with it.
    #  6. fuse_lowlevel.c:fuse_add_direntry -> fuse_add_dirent
    #  7. fuse_add_dirent basically only copies the inode and the mode!!! NOTHING ELSE:
    #             struct fuse_dirent *dirent = (struct fuse_dirent *) buf;
    #       dirent->ino = stbuf->st_ino;
    #       dirent->off = off;
    #       dirent->namelen = namelen;
    #       dirent->type = (stbuf->st_mode & 0170000) >> 12;
    #       strncpy(dirent->name, name, namelen);
    #     Everything we do to fill the whole stat struct is for naught!
    #     ONLY "stbuf->st_mode & 0170000" IS USED. ONLY 4 BITS.
    #     https://github.com/torvalds/linux/blob/1934261d897467a924e2afd1181a74c1cbfa2c1d/include/uapi/linux/stat.h#L9
    #         #define S_IFMT  00170000
    #         #define S_IFSOCK 0140000
    #         #define S_IFLNK  0120000
    #         #define S_IFREG  0100000
    #         #define S_IFBLK  0060000
    #         #define S_IFDIR  0040000
    #         #define S_IFCHR  0020000
    #         #define S_IFIFO  0010000
    #         #define S_ISUID  0004000
    #         #define S_ISGID  0002000
    #         #define S_ISVTX  0001000
    #      -> I'm not sure whether all of these are actually required. It may also be that file and directory
    #         would suffice.
    #
    # The important thing to know here is that fuse_dirent, the struct defined by the Linux Kernel FUSE API:
    #     https://github.com/torvalds/linux/blob/1934261d897467a924e2afd1181a74c1cbfa2c1d/include/uapi/linux/
    #         fuse.h#L1005-L1010
    #     https://man7.org/linux/man-pages/man4/fuse.4.html
    # Only has members for ino, off, nameln, type, and name. Everything else is cruft added by the libfuse
    # abstraction layer.
    #
    # This changes a bit with FUSE 3, which also adds support for readdir_plus. However, when talking about
    # FUSE 3, we are only talking about a major version change in libfuse, not the Kernel FUSE API, I think.
    #
    # Steps 1-4 are the same as in FUSE 2.9
    # Step 4: The filler callback can be chosen via the new flags argument to readdir:
    #         fuse_fill_dir_t filler = (flags & FUSE_READDIR_PLUS) ? fill_dir_plus : fill_dir;
    # Steps 5-7 are still the same when the FUSE_READDIR_PLUS flag is not set, which is the default!
    # If it is set, then the fill_dir_plus filler callback calls fuse_add_direntry_plus instead of
    # fuse_add_direntry.
    # fuse_add_direntry_plus converts the stat struct in the fuse_attr attr member of the
    # fuse_entry_out entry_out in the fuse_direntplus struct. fuse_attr has 16 members.
    # https://github.com/torvalds/linux/blob/1934261d897467a924e2afd1181a74c1cbfa2c1d/include/uapi/linux/
    #     fuse.h#L263C1-L280C3
    def _readdir(self, path, buf, filler, offset, fip):
        # Ignore raw_fi
        for item in self.operations('readdir', self._decode_optional_path(path), fip.contents.fh):
            if isinstance(item, str):
                name, st, offset = item, None, 0
            else:
                name, attrs, offset = item
                if attrs:
                    st = c_stat()
                    # ONLY THE MODE IS USED BY FUSE! The caller may skip everything else.
                    if 'st_mode' in attrs:
                        setattr(st, 'st_mode', attrs['st_mode'])
                else:
                    st = None

            if fuse_version_major == 2:
                if filler(buf, name.encode(self.encoding), st, offset) != 0:
                    break
            elif fuse_version_major == 3:
                if filler(buf, name.encode(self.encoding), st, offset, 0) != 0:
                    break

        return 0

    if fuse_version_major == 2:

        def readdir(self, path, buf, filler, offset, fip):
            return self._readdir(path, buf, filler, offset, fip)

    elif fuse_version_major == 3:

        def readdir(self, path, buf, filler, offset, fip, flags):
            # TODO if bit 0 (FUSE_READDIR_PLUS) is set in flags, then we might want to gather more metadata
            #      and return it in "filler" with bit 1 (FUSE_FILL_DIR_PLUS) being set.
            # Ignore raw_fi
            return self._readdir(path, buf, filler, offset, fip)

    def releasedir(self, path, fip):
        # Ignore raw_fi
        return self.operations('releasedir', self._decode_optional_path(path), fip.contents.fh)

    def fsyncdir(self, path, datasync, fip):
        # Ignore raw_fi
        return self.operations('fsyncdir', self._decode_optional_path(path), datasync, fip.contents.fh)

    def _init(self, conn, config):
        if hasattr(self.operations, "init_with_config") and not getattr(
            self.operations.init_with_config, "libfuse_ignore", False
        ):
            self.operations.init_with_config(conn, config)
        else:
            self.operations("init", "/")

    if fuse_version_major == 2:

        def init(self, conn):
            self._init(conn, fuse_config())

    else:

        def init(self, conn, config):
            self._init(conn, config)

    def destroy(self, private_data):
        return self.operations('destroy', '/')

    def access(self, path, amode):
        return self.operations('access', path.decode(self.encoding), amode)

    def create(self, path, mode, fip):
        fi = fip.contents
        path = path.decode(self.encoding)

        if self.raw_fi:
            return self.operations('create', path, mode, fi)
        fi.fh = self.operations('create', path, mode, fi.flags)
        return 0

    def ftruncate(self, path, length, fip):
        fh = fip.contents if self.raw_fi else fip.contents.fh
        return self.operations('truncate', self._decode_optional_path(path), length, fh)

    def fgetattr(self, path, buf, fip):
        ctypes.memset(buf, 0, ctypes.sizeof(c_stat))

        st = buf.contents
        if fip:
            fh = fip.contents if self.raw_fi else fip.contents.fh
        else:
            fh = fip

        attrs = self.operations('getattr', self._decode_optional_path(path), fh)
        set_st_attrs(st, attrs, use_ns=self.use_ns)
        return 0

    def lock(self, path, fip, cmd, lock):
        fh = fip.contents if self.raw_fi else fip.contents.fh
        return self.operations('lock', self._decode_optional_path(path), fh, cmd, lock)

    def _utimens(self, path, buf):
        if buf:
            atime = time_of_timespec(buf.contents.actime, use_ns=self.use_ns)
            mtime = time_of_timespec(buf.contents.modtime, use_ns=self.use_ns)
            times = (atime, mtime)
        else:
            times = None

        return self.operations('utimens', path.decode(self.encoding), times)

    if fuse_version_major == 2:
        utimens = _utimens
    elif fuse_version_major == 3:

        def utimens(self, path, buf, fip):
            self._utimens(path, buf)

    def bmap(self, path, blocksize, idx):
        return self.operations('bmap', path.decode(self.encoding), blocksize, idx)

    def ioctl(self, path, cmd, arg, fip, flags, data):
        fh = fip.contents if self.raw_fi else fip.contents.fh
        return self.operations('ioctl', path.decode(self.encoding), cmd, arg, fh, flags, data)

    def poll(self, path, fip, ph, reventsp):
        fh = fip.contents if self.raw_fi else fip.contents.fh
        return self.operations('poll', path.decode(self.encoding), fh, ph, reventsp)

    def write_buf(self, path, buf, offset, fip):
        fh = fip.contents if self.raw_fi else fip.contents.fh
        return self.operations('write_buf', path.decode(self.encoding), buf, offset, fh)

    def read_buf(self, path, bufpp, size, offset, fip):
        fh = fip.contents if self.raw_fi else fip.contents.fh
        return self.operations('read_buf', path.decode(self.encoding), bufpp, size, offset, fh)

    def flock(self, path, fip, op):
        fh = fip.contents if self.raw_fi else fip.contents.fh
        return self.operations('flock', path.decode(self.encoding), fh, op)

    def fallocate(self, path, mode, offset, size, fip):
        fh = fip.contents if self.raw_fi else fip.contents.fh
        return self.operations('fallocate', path.decode(self.encoding), mode, offset, size, fh)


def _nullable_dummy_function(method):
    '''
    Marks the given method as to be ignored by the 'FUSE' class.
    This makes it possible to add methods with the self-documenting function signatures
    while still not giving any actual callbacks to libfuse as long as these methods are
    not overwritten by a method in a subclassed 'Operations' class.
    '''
    method.libfuse_ignore = True
    return method


class Operations:
    '''
    This class should be subclassed and passed as an argument to FUSE on
    initialization. All operations should raise a FuseOSError exception on
    error.

    When in doubt of what an operation should do, check the FUSE header file
    or the corresponding system call man page.

    Any method that is not overwritten will not be set up for libfuse.
    This has the side effect that libfuse can implement fallbacks in case
    callbacks are not implemented. For example 'read' will be used when 'read_buf'
    is not implemented.

    This has the side effect that trace debug output, enabled with -o debug,
    for these FUSE function will not be printed. To enable the debug output,
    it should be overwritten with a method simply raising FuseOSError(errno.ENOSYS).
    '''

    def __call__(self, op, *args):
        if not hasattr(self, op):
            raise FuseOSError(errno.EFAULT)
        return getattr(self, op)(*args)

    @_nullable_dummy_function
    def access(self, path, amode):
        return 0

    @_nullable_dummy_function
    def bmap(self, path, blocksize, idx):
        pass

    @_nullable_dummy_function
    def chmod(self, path, mode):
        raise FuseOSError(errno.EROFS)

    @_nullable_dummy_function
    def chown(self, path, uid, gid):
        raise FuseOSError(errno.EROFS)

    @_nullable_dummy_function
    def create(self, path, mode, fi=None):
        '''
        When raw_fi is False (default case), create should return a
        numerical file handle and the signature of create becomes:
          create(self, path, mode, flags)

        When raw_fi is True the file handle should be set directly by create
        and return 0.
        '''

        raise FuseOSError(errno.EROFS)

    @_nullable_dummy_function
    def destroy(self, path):
        'Called on filesystem destruction. Path is always /'

    @_nullable_dummy_function
    def flush(self, path, fh):
        return 0

    @_nullable_dummy_function
    def fsync(self, path, datasync, fh):
        return 0

    @_nullable_dummy_function
    def fsyncdir(self, path, datasync, fh):
        return 0

    # Either fgetattr or getattr must be non-null or else libfuse 2.6 will segfault
    # with auto-cache enabled. In FUSE 2.9.9, 3.16, setting this to nullptr, should work fine.
    # https://github.com/libfuse/libfuse/blob/0a0db26bd269562676b6251e8347f4b89907ace3/lib/fuse.c#L1483-L1486
    # That particular location seems to have been fixed in 2.8.0 and 2.7.0, but not in 2.6.5.
    # It seems to have been fixed only by accident in feature commit:
    # https://github.com/libfuse/libfuse/commit/3a7c00ec0c156123c47b53ec1cd7ead001fa4dfb
    def getattr(self, path, fh=None):
        '''
        Returns a dictionary with keys identical to the stat C structure of
        stat(2).

        st_atime, st_mtime and st_ctime should be floats.

        NOTE: There is an incompatibility between Linux and Mac OS X
        concerning st_nlink of directories. Mac OS X counts all files inside
        the directory, while Linux counts only the subdirectories.
        '''

        if path != '/':
            raise FuseOSError(errno.ENOENT)
        return {'st_mode': (S_IFDIR | 0o755), 'st_nlink': 2}

    @_nullable_dummy_function
    def getxattr(self, path, name, position=0):
        raise FuseOSError(ENOTSUP)

    @_nullable_dummy_function
    def init(self, path):
        '''
        Called on filesystem initialization. (Path is always /)

        Use it instead of __init__ if you start threads on initialization.
        '''

    @_nullable_dummy_function
    def init_with_config(self, conn_info, config_3):
        '''
        Called on filesystem initialization. Same function as 'init' but with more parameters.
        Only either 'init' or 'init_with_config' should be overridden.
        Use it instead of __init__ if you start threads on initialization.
        Argument config_3 should be ignored when a FUSE 2 library is loaded.
        '''

    @_nullable_dummy_function
    def ioctl(self, path, cmd, arg, fh, flags, data):
        raise FuseOSError(errno.ENOTTY)

    @_nullable_dummy_function
    def link(self, target, source):
        'creates a hard link `target -> source` (e.g. ln source target)'

        raise FuseOSError(errno.EROFS)

    @_nullable_dummy_function
    def listxattr(self, path):
        return []

    @_nullable_dummy_function
    def lock(self, path, fh, cmd, lock):
        raise FuseOSError(errno.ENOSYS)

    @_nullable_dummy_function
    def mkdir(self, path, mode):
        raise FuseOSError(errno.EROFS)

    @_nullable_dummy_function
    def mknod(self, path, mode, dev):
        raise FuseOSError(errno.EROFS)

    @_nullable_dummy_function
    def open(self, path, flags):
        '''
        When raw_fi is False (default case), open should return a numerical
        file handle.

        When raw_fi is True the signature of open becomes:
            open(self, path, fi)

        and the file handle should be set directly.
        '''

        return 0

    @_nullable_dummy_function
    def opendir(self, path):
        'Returns a numerical file handle.'

        return 0

    @_nullable_dummy_function
    def read(self, path, size, offset, fh):
        'Returns a string containing the data requested.'

        raise FuseOSError(errno.EIO)

    @_nullable_dummy_function
    def readdir(self, path, fh):
        '''
        Can return either a list of names, or a list of (name, attrs, offset)
        tuples. attrs is a dict as in getattr.
        '''

        return ['.', '..']

    @_nullable_dummy_function
    def readlink(self, path):
        raise FuseOSError(errno.ENOENT)

    @_nullable_dummy_function
    def release(self, path, fh):
        return 0

    @_nullable_dummy_function
    def releasedir(self, path, fh):
        return 0

    @_nullable_dummy_function
    def removexattr(self, path, name):
        raise FuseOSError(ENOTSUP)

    @_nullable_dummy_function
    def rename(self, old, new):
        raise FuseOSError(errno.EROFS)

    @_nullable_dummy_function
    def rmdir(self, path):
        raise FuseOSError(errno.EROFS)

    @_nullable_dummy_function
    def setxattr(self, path, name, value, options, position=0):
        raise FuseOSError(ENOTSUP)

    @_nullable_dummy_function
    def statfs(self, path):
        '''
        Returns a dictionary with keys identical to the statvfs C structure of
        statvfs(3).

        On Mac OS X f_bsize and f_frsize must be a power of 2
        (minimum 512).
        '''

        return {}

    @_nullable_dummy_function
    def symlink(self, target, source):
        'creates a symlink `target -> source` (e.g. ln -s source target)'

        raise FuseOSError(errno.EROFS)

    @_nullable_dummy_function
    def truncate(self, path, length, fh=None):
        raise FuseOSError(errno.EROFS)

    @_nullable_dummy_function
    def unlink(self, path):
        raise FuseOSError(errno.EROFS)

    @_nullable_dummy_function
    def utimens(self, path, times=None):
        'Times is a (atime, mtime) tuple. If None use current time.'

        return 0

    @_nullable_dummy_function
    def write(self, path, data, offset, fh):
        raise FuseOSError(errno.EROFS)

    @_nullable_dummy_function
    def poll(self, path, fh, ph, reventsp):
        raise FuseOSError(errno.ENOSYS)

    @_nullable_dummy_function
    def write_buf(self, path, buf, offset, fh):
        raise FuseOSError(errno.ENOSYS)

    @_nullable_dummy_function
    def read_buf(self, path, bufpp, size, offset, fh):
        raise FuseOSError(errno.ENOSYS)

    @_nullable_dummy_function
    def flock(self, path, fh, op):
        raise FuseOSError(errno.ENOSYS)

    @_nullable_dummy_function
    def fallocate(self, path, mode, offset, size, fh):
        raise FuseOSError(errno.ENOSYS)


class LoggingMixIn:
    log = logging.getLogger('fuse.log-mixin')

    def __call__(self, op, path, *args):
        self.log.debug('-> %s %s %s', op, path, repr(args))
        ret = '[Unhandled Exception]'
        try:
            ret = getattr(self, op)(path, *args)
            return ret
        except OSError as e:
            ret = str(e)
            raise
        finally:
            self.log.debug('<- %s %s', op, repr(ret))

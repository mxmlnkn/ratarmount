# About this fork

This is basically a fork of [fusepy](https://github.com/fusepy/fusepy) because it did not see any development for over 6 years.
Copying the monolithic file into ratarmount is nice and easy and has no detriments because updates are not expected anyway.
[Refuse](https://github.com/pleiszenburg/refuse/) was an attempt to fork fusepy, but this attempt fizzled out only a month later. Among lots of metadata changes, it contains two bugfixes to the high-level API, which I'll simply redo in this fork.
See also the discussion in [this issue](https://github.com/mxmlnkn/ratarmount/issues/101).
I intend to maintain this fork as long as I maintain ratarmount, which is now over 5 years old.
For now, I have not uploaded to PyPI, so if you want to use this software, simply copy the license and `fuse.py` file into your project.
If there is any demand, I may upload this fork to PyPI.

The main motivations for forking are:

 - [x] FUSE 3 support. Based on the [libfuse changelog](https://github.com/libfuse/libfuse/blob/master/ChangeLog.rst#libfuse-300-2016-12-08), the amount of breaking changes should be fairly small. It should be possible to simply update these ten or so changed structs and functions in the existing fusepy.
 - [x] Translation layer performance. In benchmarks for a simple `find` call for listing all files, some callbacks such as `readdir` turned out to be significantly limited by converting Python dictionaries to ctype structs. The idea would be to expose the ctype structs to the fusepy caller.
   - Much of the performance was lost trying to populate the stat struct even though only the mode member is used by the kernel FUSE API.


# Platforms

While FUSE is (at least in the Unix world) a [Kernel feature](https://man7.org/linux/man-pages/man4/fuse.4.html), several user space libraries exist for easy access.
`libfuse` acts as the reference implementation.

 - [libfuse](https://github.com/libfuse/libfuse) (Linux, FreeBSD) (fuse.h [2](https://github.com/libfuse/libfuse/blob/fuse-2_9_bugfix/include/fuse.h) [3](https://github.com/libfuse/libfuse/blob/master/include/fuse.h))
 - [libfuse](https://github.com/openbsd/src/tree/master/lib/libfuse) (OpenBSD) (fuse.h [2](https://github.com/openbsd/src/blob/master/lib/libfuse/fuse.h))
 - [librefuse](https://github.com/NetBSD/src/tree/netbsd-8/lib/librefuse) (NetBSD) through [PUFFS](https://en.wikipedia.org/wiki/PUFFS_(NetBSD)) (fuse.h [2](https://github.com/NetBSD/src/blob/netbsd-8/lib/librefuse/fuse.h))
 - [FUSE for macOS](https://github.com/osxfuse/osxfuse) (OSX) (fuse.h [2](https://github.com/osxfuse/fuse/blob/master/include/fuse.h))
 - [MacFUSE](https://code.google.com/archive/p/macfuse/) (OSX), no longer maintained
 - [WinFsp](https://github.com/billziss-gh/winfsp) (Windows) (fuse.h [2](https://github.com/winfsp/winfsp/blob/master/inc/fuse/fuse.h) [3](https://github.com/winfsp/winfsp/blob/master/inc/fuse3/fuse.h))
 - [Dokany](https://github.com/dokan-dev/dokany) (Windows) (fuse.h [2](https://github.com/dokan-dev/dokany/blob/master/dokan_fuse/include/fuse.h))
 - [Dokan](https://code.google.com/archive/p/dokan/) (Windows), no longer maintained


# Open issues and PRs upstream

## Possibly valid bugs

 - [x] `#146 Bug: NameError: name 'self' is not defined`
 - [ ] `#144 How to enable O_DIRECT`
 - [x] `#142 FUSE::_wrapper() is a static method, so it shouldn't refer to 'self'`
 - [x] `#130 fixing TypeError: an integer is required when val is None`
 - [x] `#129 setattr(st, key, val): TypeError: an integer is required`
 - [x] `#124 "NameError: global name 'self' is not defined" in static FUSE._wrapper()`
 - [x] `#120 lock operation is passed a pointer to the flock strut`
 - [ ] `#116 Segfault when calling fuse_exit`
 - [ ] `#97 broken exception handling bug`
 - [x] `#81 Irritating default behavior of Operations class - raising FuseOSError(EROFS) where it really should not bug`

## Features

 - [x] `#147 Implement support for poll in the high-level API`
 - [x] `#145 Added fuse-t for Darwin search. See https://www.fuse-t.org/`
 - [x] `#127 Pass flags to create in non raw_fi mode.`
 - [ ] `#104 fix POSIX support for UTIME_OMIT and UTIME_NOW`
 - [x] `#101 Support init options and parameters.`
 - [x] `#100 libfuse versions`
 - [ ] `#70 time precision inside utimens causes rsync misses`
 - [x] `#66 Support init options and parameters`
 - [ ] `#61 performance with large numbers of files`
 - [x] `#28 Implement read_buf() and write_buf()`
 - [ ] `#7 fusepy speed needs`
 - [ ] `#2 Unable to deal with non-UTF-8 filenames`

## Cross-platform issues and feature requests that are out of scope

 - `#143 Expose a file system as case-insensitive`
 - `#141 Windows version?`
 - `#136 RHEL8 RPM package`
 - `#133 Slashes in filenames appear to cause "Input/output error"`
 - `#128 fusepy doesn't work when using 32bit personality`
 - `#117 Module name clash with python-fuse`
 - `#57 Does this support using the dokany fuse wrapper for use on Windows?`
 - [x] `#40 [openbsd] fuse_main_real not existing, there's fuse_main`

## Questions

 - `#138 “nothreads” argument explanation`
 - `#134 Project status?`
 - `#132 fusepy doesn't work when in background mode`
 - `#123 Create/Copy file with content`
 - `#119 Documentation`
 - `#118 Publish a new release`
 - `#115 read not returning 0 to client`
 - `#112 truncate vs ftruncate using python std library`
 - `#105 fuse_get_context() returns 0-filled tuple during release bug needs example`
 - `#98 Next steps/road map for the near future`
 - `#26 ls: ./mnt: Input/output error`

## FUSE-ll out of scope for me personally

 - `#114 [fusell] Allow userdata to be passed to constructor`
 - `#111 [fusell] Allow userdata to be set`
 - `#102 Extensions to fusell.`
 - `#85 bring system support in fusell.py to match fuse.py`

## Tests and documentation

 - `#139 Memory example empty files and ENOATTR`
 - `#127 package the LICENSE file in distributions`
 - `#109 Add test cases for fuse_exit implementation needs tests`
 - `#99 Python versions`
 - `#82 Create CONTRIBUTING.md`
 - `#80 Test infrastructure and suite`
 - `#78 update memory.py with mem.py from kungfuse?`
 - `#59 Include license text in its own file`
 - `#27 link to wiki from readme`

## Performance Improvement Ideas

 - Reduce wrappers:
   - [ ] Always forward path as bytes. This avoids the `_decode_optional_path` call completely.

## Changes for some real major version break

 - [ ] Enable `raw_fi` by default.
 - [ ] Remove file path encoding/decoding by default.
 - [ ] Return ENOSYS by default for almost all `Operations` implementation.
 - [ ] Simply expose `c_stat` to the fusepy user instead of expecting a badly documented dictionary.
       It is platform-dependent, but thanks to POSIX the core members are named identically.
       The order is unspecified by POSIX. What the current approach with `set_st_attrs` adds is silent
       ignoring of unknown keys. This may or may not be what one wants and the same can be achieved by
       testing `c_stat` with `hasattr` before setting values. This style guide should be documented.

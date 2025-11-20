__version__ = '1.2.1'

# Ideas that would require a new major version:
#  - Adhere to PEP 8 for FuseMount arguments and so on.
#  - Consistent interface for flags, e.g., --enable=file-versions, union-mount, gnu-incremental-detection, ignore-zeros,
#       foreground, lazy-mounting ...
#       or: --enable-x, --disable-x
#       or: --x, --no-x <- imo not very readable
#       or: --x=[on|off] [true|false] [1|0] ... seems pretty reasonable and would avoid duplicate help messages!
#         -> would also enale tristates, e.g., --gnu-incremental=[yes|no|detect]
#     -> should be compatible with python-argcomplete!
#     -> I was leaning heavily towards the --feature=on|off until I found out that Python 3.9 has
#        argparse.BooleanOptionalAction, which automatically adds a --no-<feature> variant ...
#        Now, I am conflicted again :(
#  - Disable file versions by default.
#  - Restructure command line to use subcommands: mount, unmount, commit, ...

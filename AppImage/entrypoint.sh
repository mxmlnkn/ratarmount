export LD_LIBRARY_PATH="${APPDIR}/usr/lib/:$LD_LIBRARY_PATH"
export PATH="${APPDIR}/usr/bin/:$PATH"

# The fusepy module directly tries to read this environment variable and uses it.
export FUSE_LIBRARY_PATH="${APPDIR}/usr/lib/libfuse.so.2"

# -u is important or else piping the output to other tools and therefore the tests might fail!
{{ python-executable }} -u -I -m ratarmount "$@"

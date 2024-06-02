export LD_LIBRARY_PATH="${APPDIR}/usr/lib/:$LD_LIBRARY_PATH"
export PATH="${APPDIR}/usr/bin/:$PATH"

# The fusepy module directly tries to read this environment variable and uses it.
export FUSE_LIBRARY_PATH="${APPDIR}/usr/lib/libfuse.so.2"
# Necessary because ctypes.util.find_library does not find the libarchive.so.13 if libarchive.so is missing.
# However, this only seems to be a problem with AppImage (LD_LIBRARY_PATH) not with system-installated versions.
if [[ -f "${APPDIR}/usr/lib/libarchive.so.13" ]]; then
    export LIBARCHIVE="${APPDIR}/usr/lib/libarchive.so.13"
fi

# -u is important or else piping the output to other tools and therefore the tests might fail!
{{ python-executable }} -u -I -m ratarmount "$@"

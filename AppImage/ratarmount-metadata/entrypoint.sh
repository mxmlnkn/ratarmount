export LD_LIBRARY_PATH="${APPDIR}/usr/lib/:$LD_LIBRARY_PATH"
export PATH="${APPDIR}/usr/bin/:$PATH"

# The fusepy module directly tries to read this environment variable and uses it.
export FUSE_LIBRARY_PATH="${APPDIR}/usr/lib/libfuse.so.2"
# Necessary because ctypes.util.find_library does not find the libarchive.so.13 if libarchive.so is missing.
# However, this only seems to be a problem with AppImage (LD_LIBRARY_PATH) not with system-installated versions.
if [[ -f "${APPDIR}/usr/lib/libarchive.so.13" ]]; then
    export LIBARCHIVE="${APPDIR}/usr/lib/libarchive.so.13"
fi

# Avoid calling a subprocess bash after changing LD_LIBRARY_PATH in order to avoid the warning:
#     libtinfo.so.6: no version information available
# -u is important or else piping the output to other tools and therefore the tests might fail!
ARGS=( -u -I -m ratarmount "$@" )

# Also avoid calling external processes like "head" or "grep" because they would have to be bundled.
# Try to use bash built-in's only:
#     https://www.gnu.org/software/bash/manual/html_node/Bash-Builtins.html
#     https://www.gnu.org/software/bash/manual/html_node/Bourne-Shell-Builtins.html
#     https://www.gnu.org/software/bash/manual/html_node/The-Set-Builtin.html
read -r firstLine 2>/dev/null < {{ python-executable }}
if [[ "$firstLine" =~ ^#\!.*bash$ ]]; then
    # Sourcing the file avoids the shebang being interpreted and another bash process being started.
    # However, this means that this Python wrapper script, which checks $0 to get its location, is wrong
    # and needs to be patched to use BASH_SOURCE.
    BASH_ARGV0={{ python-executable }}
    . {{ python-executable }} "${ARGS[@]}"
else
    {{ python-executable }} "${ARGS[@]}"
fi

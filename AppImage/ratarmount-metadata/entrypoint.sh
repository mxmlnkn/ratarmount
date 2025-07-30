#!/bin/sh
# Works in bash as well as dash 0.5.12 on Ubuntu 24.04. Should be POSIX-compliant.

# Workaround for https://github.com/niess/python-appimage/issues/92
# If running from an extracted image, then export APPDIR
if [ -z "${APPIMAGE}" ]; then
    self="$(readlink -f -- "$0")"
    export APPDIR="${self%/*}"
fi

# The fusepy module directly tries to read this environment variable and uses it.
export FUSE_LIBRARY_PATH="${APPDIR}/usr/lib/libfuse.so.2"
# Necessary because ctypes.util.find_library does not find the libarchive.so.13 if libarchive.so is missing.
# However, this only seems to be a problem with AppImage (LD_LIBRARY_PATH) not with system-installated versions.
if [ -f "${APPDIR}/usr/lib/libarchive.so.13" ]; then
    export LIBARCHIVE="${APPDIR}/usr/lib/libarchive.so.13"
fi

# Avoid calling external processes like "head" or "grep" because they would have to be bundled!
# Even though they are part of POSIX, I think systems without grep installed exist.
# Try to use bash built-in's only:
#     https://www.gnu.org/software/bash/manual/html_node/Bash-Builtins.html
#     https://www.gnu.org/software/bash/manual/html_node/Bourne-Shell-Builtins.html
#     https://www.gnu.org/software/bash/manual/html_node/The-Set-Builtin.html

# Export Tcl/Tk, the Python-native GUI framework. Currently not bundled in the AppImage because it is not used.
#export TCL_LIBRARY="${APPDIR}/usr/share/tcltk/tcl8.6"
#export TK_LIBRARY="${APPDIR}/usr/share/tcltk/tk8.6"
#export TKPATH="${TK_LIBRARY}"

# Export SSL certificate
export SSL_CERT_FILE="${APPDIR}/opt/_internal/certs.pem"

export LD_LIBRARY_PATH="${APPDIR}/usr/lib/:$LD_LIBRARY_PATH"
# Avoid calling a subprocess bash after changing LD_LIBRARY_PATH in order to avoid the warning:
#     libtinfo.so.6: no version information available
export PATH="${APPDIR}/usr/bin/:$PATH"

# Exec avoids another yet another process or shell process being started.
# Directly call the Python3 binary to avoid another indirection: https://github.com/niess/python-appimage/issues/90
# Note that python-appimage bundles yet another bash(!) script inside /usr/bin/python3 to call the real one!
# -u is important or else piping the output to other tools and therefore the tests might fail!
# -I is isolated mode. Implies -E, -P and -s. All PYTHON* environment variables are ignored, too.
# -E Ignore environment variables like PYTHONPATH and PYTHONHOME that modify the behavior of the interpreter.
# -P Don't automatically prepend a potentially unsafe path to sys.path such as the current directory,
#    the script's directory or an empty string.
# -s Don't add user site directory to sys.path.
for PYTHONHOME in "$APPDIR"/opt/python*; do
    for APPIMAGE_COMMAND in "$PYTHONHOME"/bin/python3*; do
        if [ -x "$APPIMAGE_COMMAND" ] && [ ! -L "$APPIMAGE_COMMAND" ]; then
            # Should point to the python3 binary. Will be set to sys.executable inside the encodings/__init__.py hack!
            export APPIMAGE_COMMAND
            exec "$APPIMAGE_COMMAND" -u -I -m ratarmount "$@"
        fi
    done
done

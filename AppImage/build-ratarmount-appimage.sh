#!/usr/bin/env bash

# E.g., run this script inside the manylinux2014 container and mount the whole ratarmount git root:
#   docker run -v$PWD:/project -it quay.io/pypa/manylinux2014_x86_64 bash
#   cd /project/AppImage && ./build-ratarmount-appimage.sh

function commandExists()
{
    command -v "$1" > /dev/null 2>&1
}

function installSystemRequirements()
{
    export PATH="/opt/python/cp39-cp39/bin:$PATH"
    python3 -m pip install python-appimage
    yum -y install epel-release
    # We need to isntall development dependencies to build Python packages from source and we also need
    # to install libraries such as libarchive in order to copy them into the AppImage.
    yum install -y fuse fakeroot patchelf fuse-libs libsqlite3x strace desktop-file-utils libzstd-devel libarchive lzop
}

function installAppImageTools()
{
    local platform=$( uname --hardware-platform )

    toolName='appimagetool'
    if [[ ! -x $toolName ]]; then
        curl -L -o "$toolName" \
            "https://github.com/AppImage/AppImageKit/releases/download/continuous/appimagetool-$platform.AppImage"
        chmod u+x "$toolName"
    fi

    toolName='linuxdeploy'
    if [[ ! -x "$toolName" ]]; then
        curl -L -o "$toolName" \
            "https://github.com/linuxdeploy/linuxdeploy/releases/download/continuous/linuxdeploy-$platform.AppImage"
        chmod u+x "$toolName"
    fi

    export PATH="$PWD:$PATH"
}

function installAppImagePythonPackages()
{
    # Unfortunately, building from source only works with the same manylinux container from which the
    # AppImage python binary has been stripped out from! Else, it seems to have hardcoded the path to Python.h
    # and that path will not exist when running the python binary directly on a different host system.
    # The compilation call will contain this: -I/opt/_internal/cpython-3.9.15/include/python3.9
    # Even though it should be like this: -I<path-to-appdir>/opt/_internal/cpython-3.9.15/include/python3.9
    if [[ -n "$USE_CUTTING_EDGE_BACKENDS" ]]; then
        "$APP_PYTHON_BIN" -I -m pip install --no-cache-dir \
            'git+https://github.com/mxmlnkn/indexed_bzip2.git@master#egginfo=indexed_bzip2&subdirectory=python/indexed_bzip2'
        "$APP_PYTHON_BIN" -I -m pip install --no-cache-dir \
            'git+https://github.com/mxmlnkn/indexed_bzip2.git@master#egginfo=rapidgzip&subdirectory=python/rapidgzip'
    else
        "$APP_PYTHON_BIN" -I -m pip install --no-cache-dir indexed_bzip2
        "$APP_PYTHON_BIN" -I -m pip install --no-cache-dir rapidgzip
    fi
    "$APP_PYTHON_BIN" -I -m pip install --no-cache-dir ../core
    "$APP_PYTHON_BIN" -I -m pip install --no-cache-dir ..
}

function installAppImageSystemLibraries()
{
    # Note that manylinux2014 already has libsqlite3.so.0 inside /usr/lib.
    # It is also a good idea to run ldd on the .so files to see what they depend on in turn.
    # ldd /usr/lib64/libarchive.so.13
    #    libcrypto.so.10 => /lib64/libcrypto.so.10
    #    # https://savannah.nongnu.org/projects/acl
    #    # Commands for Manipulating POSIX Access Control Lists
    #    libacl.so.1 => /lib64/libacl.so.1
    #    # https://savannah.nongnu.org/projects/attr
    #    # Commands for Manipulating Filesystem Extended Attributes
    #    libattr.so.1 => /lib64/libattr.so.1
    #    # Compression backend libarries. Definitely required.
    #    liblzo2.so.2 => /lib64/liblzo2.so.2
    #    liblzma.so.5 => /lib64/liblzma.so.5
    #    libbz2.so.1 => /lib64/libbz2.so.1
    #    libxml2.so.2 => /lib64/libxml2.so.2    # For xar, which has an XML TOC.
    #    libz.so.1 => /lib64/libz.so.1
    # Ubiquitous libraries that probably do not and should not be bundled into the AppImage:
    #    linux-vdso.so.1 =>
    #    libm.so.6 => /lib64/libm.so.6
    #    libdl.so.2 => /lib64/libdl.so.2
    #    libc.so.6 => /lib64/libc.so.6
    #    libpthread.so.0 => /lib64/libpthread.so.0
    #    /lib64/ld-linux-x86-64.so.2
    # linuxdeploy automatically bundles transitive dependencies! I only need to specify libarchive.so manually
    # because it is dynamically loaded by python-libarchive-c, which linuxdeploy does notice automatically.
    local libraries=( $( find /lib64/ -name 'libcrypto.so*' ) )
    if commandExists repoquery; then
        libraries+=( $( repoquery -l fuse-libs | 'grep' 'lib64.*[.]so' ) )
        libraries+=( $( repoquery -l libarchive | 'grep' 'lib64.*[.]so' ) )
    elif commandExists dnf; then
        libraries+=( $( dnf repoquery -l fuse-libs | 'grep' 'lib64.*[.]so' ) )
        libraries+=( $( dnf repoquery -l libarchive | 'grep' 'lib64.*[.]so' ) )
    elif commandExists dpkg; then
        libraries+=( $( dpkg -L libfuse2 | 'grep' '/lib.*[.]so' ) )
        libraries+=( $( dpkg -L libarchive | 'grep' '/lib.*[.]so' ) )
    else
        echo -e "\e[31mCannot gather FUSE libs into AppImage without (dnf) repoquery.\e[0m"
    fi

    echo "Bundle libraries:"
    printf '    %s\n' "${libraries[@]}"

    if [[ "${#libraries[@]}" -gt 0 ]]; then
        'cp' -a "${libraries[@]}" "$APP_DIR"/usr/lib/
    fi

    APPIMAGE_EXTRACT_AND_RUN=1 linuxdeploy --appdir="$APP_DIR" "${libraries[@]/#/--library=}" \
        --executable=$( which fusermount ) --executable=$( which lzop )
}

function trimAppImage()
{
    APP_PYTHON_BASE="${APP_DIR}/opt/python${APP_PYTHON_VERSION}"
    APP_PYTHON_LIB="${APP_PYTHON_BASE}/lib/python${APP_PYTHON_VERSION}"
    "$APP_PYTHON_BIN" -s -m pip uninstall -y build setuptools wheel pip

    'rm' -rf \
        "$APP_PYTHON_LIB/site-packages/indexed_gzip/tests" \
        "$APP_PYTHON_LIB/site-packages/indexed_gzip/"*.c \
        "$APP_PYTHON_LIB/site-packages/indexed_gzip/"*.h \
        "$APP_PYTHON_LIB/site-packages/indexed_gzip/"*.pxd

    #"$APP_PYTHON_LIB/email"  # imported by urllib, importlib, site-packages/packaging/metadata.py
    'rm' -rf \
           "$APP_PYTHON_BASE/include" \
           "$APP_DIR/usr/share/tcltk" \
           "$APP_DIR/usr/lib/libpng"* \
           "$APP_PYTHON_LIB/dbm" \
           "$APP_PYTHON_LIB/imaplib.py" \
           "$APP_PYTHON_LIB/mailbox.py" \
           "$APP_PYTHON_LIB/smtplib.py" \
           "$APP_PYTHON_LIB/smtpd.py" \
           "$APP_PYTHON_LIB/ensurepip" \
           "$APP_PYTHON_LIB/html" \
           "$APP_PYTHON_LIB/idlelib" \
           "$APP_PYTHON_LIB/pickletools.py" \
           "$APP_PYTHON_LIB/pydoc_data" \
           "$APP_PYTHON_LIB/pydoc.py" \
           "$APP_PYTHON_LIB/tkinter" \
           "$APP_PYTHON_LIB/turtledemo" \
           "$APP_PYTHON_LIB/turtle.py" \
           "$APP_PYTHON_LIB/unittest" \
           "$APP_PYTHON_LIB/wsgiref" \
           "$APP_PYTHON_LIB/xmlrpc" \
           "$APP_PYTHON_LIB/zoneinfo" \
           "$APP_PYTHON_LIB/__phello__"
    # Remove unused readline and everything using it
    'rm' -rf \
           "$APP_DIR/usr/lib/libreadline"* \
           "$APP_PYTHON_LIB/cmd.py" \
           "$APP_PYTHON_LIB/code.py" \
           "$APP_PYTHON_LIB/pdb.py" \
           "$APP_PYTHON_LIB/profile.py" \
           "$APP_PYTHON_LIB/pstats.py" \
           "$APP_PYTHON_LIB/rlcompleter.py" \
           "$APP_PYTHON_LIB/lib-dynload/readline.cpython-311-x86_64-linux-gnu.so"
    # Remove libraries deprecated since 3.11 and to be removed in 3.13:
    # https://docs.python.org/3.13/whatsnew/3.13.html#whatsnew313-pep594
    'rm' -rf \
           "$APP_PYTHON_LIB/aifc.py" \
           "$APP_PYTHON_LIB/lib-dynlod/audioop"* \
           "$APP_PYTHON_LIB/chunk.py" \
           "$APP_PYTHON_LIB/cgi.py" \
           "$APP_PYTHON_LIB/cgitb.py" \
           "$APP_PYTHON_LIB/crypt.py" \
           "$APP_PYTHON_LIB/lib-dynload/crypt"* \
           "$APP_PYTHON_LIB/imghdr.py" \
           "$APP_PYTHON_LIB/mailcap.py" \
           "$APP_PYTHON_LIB/msilib.py" \
           "$APP_PYTHON_LIB/lib-dynlib/nis"* \
           "$APP_PYTHON_LIB/nntplib.py" \
           "$APP_PYTHON_LIB/lib-dynlib/ossaudiodev"* \
           "$APP_PYTHON_LIB/pipes.py" \
           "$APP_PYTHON_LIB/sndhdr.py" \
           "$APP_PYTHON_LIB/lib-dynlib/spwd"* \
           "$APP_PYTHON_LIB/sunau.py" \
           "$APP_PYTHON_LIB/telnetlib.py" \
           "$APP_PYTHON_LIB/uu.py" \
           "$APP_PYTHON_LIB/xdrlib.py" \
           "$APP_PYTHON_LIB/lib2to3"
    find "$APP_DIR/usr/lib/" -name 'libtk*.so' -delete
    find "$APP_DIR/usr/lib/" -name 'libtcl*.so' -delete
    find "$APP_DIR" -type d -empty -print0 | xargs -0 rmdir
    find "$APP_DIR" -type d -empty -print0 | xargs -0 rmdir
    find "$APP_DIR" -name '__pycache__' -print0 | xargs -0 rm -r
    find "$APP_PYTHON_LIB/site-packages/" -name '*.so' -size +1M -print0 | xargs -0 strip --strip-debug
}


# Main entry

cd -- "$( dirname -- "$BASH_SOURCE" )"

# AUDITWHEEL_ARCH is set inside the manylinux container automatically
if [[ -n $AUDITWHEEL_ARCH ]]; then
    APPIMAGE_ARCH=$AUDITWHEEL_ARCH
else
    APPIMAGE_ARCH=$( uname -m )
fi

if [[ -n $AUDITWHEEL_PLAT ]]; then
    APPIMAGE_PLATFORM=$AUDITWHEEL_PLAT
else
    # This is used for python-appimage and requires a manylinux version!
    APPIMAGE_PLATFORM="manylinux2014_$APPIMAGE_ARCH"
fi

APP_BASE="ratarmount-$APPIMAGE_PLATFORM"
APP_DIR="$APP_BASE.AppDir"
if [[ -z $APP_PYTHON_VERSION ]]; then
    APP_PYTHON_VERSION=3.12
fi
APP_PYTHON_BIN="$APP_DIR/opt/python$APP_PYTHON_VERSION/bin/python$APP_PYTHON_VERSION"

if [[ -n $AUDITWHEEL_ARCH ]]; then
    echo "Install System Build Tools"
    # If manylinux container is implied as the current host system, install requirements.
    installSystemRequirements
fi

echo "Install AppImage Tooling"
installAppImageTools

echo "Build Base Python AppImage With Ratarmount Metadata"
python3 -m pip install --upgrade python_appimage
python3 -m python_appimage build app -l "$APPIMAGE_PLATFORM" -p "$APP_PYTHON_VERSION" ratarmount-metadata/ ||
    exit 1

echo "Extract AppImage to AppDir for Further Modification"
./"ratarmount-$APPIMAGE_ARCH.AppImage" --appimage-extract > /dev/null
'rm' -rf "$APP_DIR" ./"ratarmount-$APPIMAGE_ARCH.AppImage"
mv squashfs-root/ "$APP_DIR"

echo "Install Ratarmount into AppDir"
installAppImagePythonPackages

echo "Bundle System Dependencies into AppDir"
installAppImageSystemLibraries

echo "Clean up Unnecessary Files from AppDir"
trimAppImage

echo "Create AppImage from Modified AppDir"
# times and sizes for ratarmount.AppImage --help on T14: --comp gzip: 1.6s, 12.50 MB, --comp xz: 3.0s, 12.88 MB
APPIMAGE_EXTRACT_AND_RUN=1 ARCH="$APPIMAGE_ARCH" appimagetool --comp gzip --no-appstream "$APP_BASE".App{Dir,Image}

chmod u+x "$APP_BASE.AppImage"
version=$( ./"$APP_BASE.AppImage" --version | sed -n -E 's|ratarmount ([0-9.]+)|\1|p' &>/dev/null )
if [[ -z "$version" ]]; then
    version=$( sed -n -E "s|.*__version__ = '([0-9.]+).*'|\1|p" ../ratarmount.py )
fi
if [[ -n "$version" ]]; then
    'mv' -- "$APP_BASE.AppImage" "ratarmount-$version-$APPIMAGE_ARCH.AppImage"
fi

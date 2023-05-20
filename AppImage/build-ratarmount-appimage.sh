#!/usr/bin/env bash

# E.g., run this cript inside the manylinux2014 container and mount the whole ratarmount git root:
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
    yum install -y fuse fakeroot patchelf fuse-libs libsqlite3x strace desktop-file-utils
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
            'git+https://github.com/mxmlnkn/indexed_bzip2.git@master#egginfo=pragzip&subdirectory=python/pragzip'
    else
        "$APP_PYTHON_BIN" -I -m pip install --no-cache-dir indexed_bzip2
        "$APP_PYTHON_BIN" -I -m pip install --no-cache-dir pragzip
    fi
    "$APP_PYTHON_BIN" -I -m pip install --no-cache-dir ../core
    "$APP_PYTHON_BIN" -I -m pip install --no-cache-dir ..
}

function installAppImageSystemLibraries()
{
    # Note that manylinux2014 already has libsqlite3.so.0 inside /usr/lib.
    local libraries=()
    if commandExists repoquery; then
        libraries=( $( repoquery -l fuse-libs | 'grep' 'lib64.*[.]so' ) )
    elif commandExists dnf; then
        libraries=( $( dnf repoquery -l fuse-libs | 'grep' 'lib64.*[.]so' ) )
    elif commandExists dpkg; then
        libraries=( $( dpkg -L libfuse2 | 'grep' '/lib.*[.]so' ) )
    else
        echo -e "\e[31mCannot gather FUSE libs into AppImage without (dnf) repoquery.\e[0m"
    fi

    if [[ "${#libraries[@]}" -gt 0 ]]; then
        'cp' -a "${libraries[@]}" "$APP_DIR"/usr/lib/
    fi

    APPIMAGE_EXTRACT_AND_RUN=1 linuxdeploy --appdir="$APP_DIR" "${libraries[@]/#/--library=}" \
        --executable=/usr/bin/fusermount
}

function trimAppImage()
{
    APP_PYTHON_BASE="${APP_DIR}/opt/python${APP_PYTHON_VERSION}"
    APP_PYTHON_LIB="${APP_PYTHON_BASE}/lib/python${APP_PYTHON_VERSION}"
    "$APP_PYTHON_BIN" -s -m pip uninstall -y build setuptools wheel pip
    'rm' -rf "$APP_PYTHON_LIB/site-packages/indexed_gzip/tests" \
           "$APP_PYTHON_BASE/include" \
           "$APP_DIR/usr/share/tcltk" \
           "$APP_PYTHON_LIB/ensurepip" \
           "$APP_PYTHON_LIB/lib2to3" \
           "$APP_PYTHON_LIB/tkinter" \
           "$APP_PYTHON_LIB/unittest"
    find "$APP_DIR/usr/lib/" -name 'libtk8*.so' -delete
    find "$APP_DIR/usr/lib/" -name 'libtcl8*.so' -delete
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
    APP_PYTHON_VERSION=3.11
fi
APP_PYTHON_BIN="$APP_DIR/opt/python$APP_PYTHON_VERSION/bin/python$APP_PYTHON_VERSION"

echo "Install System Build Tools"
if [[ -n $AUDITWHEEL_ARCH ]]; then
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
./"ratarmount-$APPIMAGE_ARCH.AppImage" --appimage-extract
'rm' -rf "$APP_DIR"
mv squashfs-root/ "$APP_DIR"

echo "Install Ratarmount into AppDir"
installAppImagePythonPackages

echo "Bundle System Dependencies into AppDir"
installAppImageSystemLibraries

echo "Clean up Unnecessary Files from AppDir"
trimAppImage

echo "Create AppImage from Modified AppDir"
APPIMAGE_EXTRACT_AND_RUN=1 ARCH="$APPIMAGE_ARCH" appimagetool --no-appstream "$APP_BASE".App{Dir,Image}

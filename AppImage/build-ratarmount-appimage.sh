#!/usr/bin/env bash

# E.g., run this script inside the manylinux2014 container and mount the whole ratarmount git root:
#   docker run -v$PWD:/project -it quay.io/pypa/manylinux2014_x86_64 bash
#   cd /project/AppImage && ./build-ratarmount-appimage.sh
# Should be built in the same manylinux container as used for the AppImage, or else the libarchive
# from the surrounding system is mixed with an incompatible liblzma from the Python AppImage, resulting in:
#     OSError: /tmp/.mount_ratarmlSdCvH/usr/lib/liblzma.so.5: version `XZ_5.2' not found
#     (required by /tmp/.mount_ratarmlSdCvH/usr/lib/libarchive.so.13)
# Then again, this error can be fixed by calling linxdeploy explicitly with liblzma.so.


function commandExists()
{
    command -v "$1" > /dev/null 2>&1
}

function installSystemRequirements()
{
    export PATH="/opt/python/cp39-cp39/bin:$PATH"
    python3 -m pip install python-appimage
    yum -y install epel-release
    # We need to install development dependencies to build Python packages from source and we also need
    # to install libraries such as libarchive in order to copy them into the AppImage.
    yum install -y fuse fakeroot patchelf fuse-libs libsqlite3x strace desktop-file-utils libzstd-devel \
        libarchive libarchive-devel lzop lzo lzo-devel
}

function installAppImageTools()
{
    local platform=$( uname --hardware-platform )

    toolName='appimagetool'
    if [[ ! -x $toolName ]]; then
        curl -L -o "$toolName" \
            "https://github.com/AppImage/appimagetool/releases/download/continuous/appimagetool-$platform.AppImage"
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
            'git+https://github.com/mxmlnkn/indexed_bzip2.git@master#egginfo=rapidgzip&subdirectory=python/rapidgzip'
    else
        "$APP_PYTHON_BIN" -I -m pip install --no-cache-dir rapidgzip
    fi

    # https://github.com/nathanhi/pyfatfs/issues/41
    "$APP_PYTHON_BIN" -I -m pip install --no-cache-dir \
        'git+https://github.com/mxmlnkn/pyfatfs.git@master#egginfo=pyfatfs'

    "$APP_PYTHON_BIN" -I -m pip install --no-cache-dir ../core
    "$APP_PYTHON_BIN" -I -m pip install --no-cache-dir ..[full]

    # These lines are only to document the individual package sizes. They are all installed with [full] above.
    # ratarmount-0.10.0-manylinux2014_x86_64.AppImage (the first one!) was 13.6 MB
    # ratarmount-v0.11.3-manylinux2014_x86_64.AppImage was 13.6 MB
    # ratarmount-0.12.0-manylinux2014_x86_64.AppImage was 26.3 MB thanks to an error with the trime-down script.
    # ratarmount-0.15.0-x86_64.AppImage was 14.8 MB
    # ratarmount-0.15.1-x86_64.AppImage was 13.3 MB (manylinux_2014)
    # ratarmount-0.15.2-x86_64.AppImage was 11.7 MB (manylinux_2_28)
    # At this point, with pyfatfs, the AppImage is/was 13.0 MB. Extracts to 45.1 MB
    # This bloats the AppImage to 23.7 MB, which is still ok, I guess. Extracts to 83.1 MB
    #    "$APP_PYTHON_BIN" -I -m pip install --no-cache-dir requests aiohttp sshfs smbprotocol pygit2<1.15 fsspec
    # This bloats the AppImage to 38.5 MB :/. Extracts to 121.0 MB
    #    "$APP_PYTHON_BIN" -I -m pip install --no-cache-dir s3fs gcsfs adlfs dropboxdrivefs

    # These are untested but small enough that we can just install them for now. Maybe they even work.
    "$APP_PYTHON_BIN" -I -m pip install --no-cache-dir gcsfs adlfs dropboxdrivefs

    # Need to install it manually because it is disabled for Python >=3.12 because of:
    # https://github.com/nathanhi/pyfatfs/issues/41
    # And we need to apply a patch for that.
    "$APP_PYTHON_BIN" -I -m pip install --no-cache-dir pyfatfs
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
    local yumCommand=''
    if commandExists repoquery; then
        yumCommand='repoquery'
    elif commandExists dnf; then
        yumCommand='dnf repoquery'
    elif commandExists yum; then
        yumCommand='yum'
    elif commandExists dpkg; then
        libraries+=( $( dpkg -L libfuse2 | 'grep' '/lib.*[.]so' ) )
        libraries+=( $( dpkg -L libarchive13 | 'grep' '/lib.*[.]so' ) )
        libraries+=( $( dpkg -L libarchive-dev | 'grep' '/lib.*[.]so' ) )
        libraries+=( $( dpkg -L lzo | 'grep' '/lib.*[.]so' ) )
        libraries+=( $( dpkg -L liblzma5 | 'grep' '/lib.*[.]so' ) )
    else
        echo -e "\e[31mCannot gather FUSE libs into AppImage without (dnf) repoquery.\e[0m"
    fi

    if [[ -n "$yumCommand" ]]; then
        libraries+=( $( $yumCommand -l fuse-libs | 'grep' 'lib64.*[.]so' ) )
        libraries+=( $( $yumCommand -l libarchive | 'grep' 'lib64.*[.]so' ) )
        libraries+=( $( $yumCommand -l libarchive-devel | 'grep' 'lib64.*[.]so' ) )
        libraries+=( $( $yumCommand -l lzo | 'grep' 'lib64.*[.]so' ) )
        libraries+=( $( $yumCommand -l xz-devel | 'grep' 'lib64.*[.]so' ) )
    fi

    # For some reason, the simple libarchive.so file without any version suffix is only installed with the development
    # packages! For yet another reason ctypes.util.find_library does not find libarchive.so.13 if libarchive.so
    # does not # exist in the AppDir. However, when only libarchive.so.13 exists in the system location, it DOES
    # find it even when libarchive.so does not exist -.-. It's really weird.
    # https://github.com/Changaco/python-libarchive-c/issues/128

    echo "Bundle libraries:"
    printf '    %s\n' "${libraries[@]}"

    if [[ "${#libraries[@]}" -gt 0 ]]; then
        existingLibraries=()
        for library in "${libraries[@]}"; do
            if [[ -e "$library" ]]; then
                existingLibraries+=( "$library" )
            fi
        done
        libraries=( "${existingLibraries[@]}" )
        'cp' -a "${libraries[@]}" "$APP_DIR"/usr/lib/
    fi

    APPIMAGE_EXTRACT_AND_RUN=1 linuxdeploy --appdir="$APP_DIR" "${libraries[@]/#/--library=}" \
        --executable="$( which fusermount )" --executable="$( which lzop )"
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
    #"$APP_PYTHON_LIB/html"   # Needed by botocore
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
    # aifc, audioop, chunk, cgi, cgitb, crypt, imghdr, mailcap, msilib, nis, nntplib,
    # ossaudiodev, pipes, sndhdr, spwd, sunau, telnetlib, uu, xdrlib, lib2to3
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
# BASH_SOURCE only exists since bash 3.0. This is a fallback.
# https://stackoverflow.com/questions/35006457/choosing-between-0-and-bash-source
if [[ -z "$BASH_SOURCE" && -f "$0" ]]; then cd -- "$( dirname -- "$0" )"; fi

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
# times and sizes for ratarmount.AppImage --help on Ryzen 3900X: --comp gzip: 1.2s, 15.09 MB, --comp xz: 2.2s, 14.59 MB
# times for ratarmount --help without AppImage on Ryzen 3900X: 0.155s
APPIMAGE_EXTRACT_AND_RUN=1 ARCH="$APPIMAGE_ARCH" appimagetool \
    --comp zstd --mksquashfs-opt -Xcompression-level --mksquashfs-opt 22 \
    --mksquashfs-opt -b --mksquashfs-opt 256K --no-appstream "$APP_BASE".App{Dir,Image}

# Zstd benchmarks (new appimagetool implementations seem to be unable to use anything else -.-)
#   Installed:               0.123 | 0.1467 +- 0.0005 | 0.165
#   Extracted AppRun:        0.298 | 0.396  +- 0.0016 | 0.448  Surprisingly slow
#   No comp : Size: 78922048 0.714 | 0.7453 +- 0.0008 | 0.802  Kinda surprising that uncompressed has this overhead
#   Level  1: Size: 30843200 0.951 | 1.24   +- 0.004  | 1.346
#   Level  2: Size: 29430080 1.033 | 1.294  +- 0.004  | 1.393  v 5% slower
#   Level  3: Size: 28348736 1.018 | 1.25   +- 0.004  | 1.417
#   Level  4: Size: 28184896 1.075 | 1.276  +- 0.003  | 1.397
#   Level  5: Size: 27468096 0.968 | 1.242  +- 0.005  | 1.435
#   Level  6: Size: 27050304 1.063 | 1.261  +- 0.0029 | 1.409
#   Level  7: Size: 26939712 1.019 | 1.243  +- 0.003  | 1.371
#   Level  8: Size: 26849600 1.032 | 1.221  +- 0.005  | 1.425
#   Level  9: Size: 26837312 1.063 | 1.251  +- 0.003  | 1.359
#   Level 10: Size: 26784064 1.001 | 1.214  +- 0.003  | 1.346
#   Level 11: Size: 26747200 1.063 | 1.2446 +- 0.0027 | 1.345
#   Level 12: Size: 26747200 1.045 | 1.224  +- 0.004  | 1.367
#   Level 13: Size: 26681664 1.138 | 1.2473 +- 0.0030 | 1.377
#   Level 14: Size: 26644800 1.102 | 1.23   +- 0.004  | 1.378
#   Level 15: Size: 26632512 0.999 | 1.2543 +- 0.0026 | 1.356
#   Level 16: Size: 25538880 1.187 | 1.3058 +- 0.0023 | 1.41   v 10% slower
#   Level 17: Size: 25055552 1.099 | 1.275  +- 0.004  | 1.452
#   Level 18: Size: 23929152 1.102 | 1.336  +- 0.004  | 1.51
#   Level 19: Size: 23896384 1.231 | 1.3236 +- 0.0024 | 1.452
#   Level 20: Size: 23896384 1.112 | 1.306  +- 0.004  | 1.435
#   Level 21: Size: 23892288 1.093 | 1.366  +- 0.004  | 1.506
#   Level 22: Size: 23892288 1.17  | 1.365  +- 0.003  | 1.478
#   gzip    : Size: 27899072 0.834 | 1.0063 +- 0.0025 | 1.089  v 25-35% faster than zstd!
#   ratarmount-0.15.0-x86_64.AppImage
#             Size: 14804160 0.878 | 0.9857 +- 0.0021 | 1.06
#
# - The old gzip compression is almost 28% faster! But
# - Levels with no size difference: 21/22, 19/20, 11/12
# - The improvement of level 22 over level 19 or even level 18 is also minuscule.
# - Level 17->18 was the last larger size improvement (-4.5%).
# - Level 18->19: -0.14%
# - Levels 2-15 are mostly equally fast looking at the minimum, average, and maximum
#   The fastest speed for Level 1 could be said to be 10% faster but it only affects the average by 5%
#   Levels 16-22 are roughly 10% slower for the minimum, average, and maximum
# - Compression level doesn't seem to implact decompression time much, so simply use the highest setting.
# - Only block sizes between 4 KiB and 1 MiB are allowed:
#   mksquashfs: -b block size not power of two or not between 4096 and 1Mbyte
#   I don't know what kind of default settings appimagetool uses. It seems to result in one single block,
#   else I can't explain why it takes 3-6x as long to mount and hast 5% smaller size even compared to the
#   largest block size of 1 MiB!
#
#   Level 15: Block Size:   4K Size: 33308992 0.295 | 0.3698 +- 0.0016 | 0.433 -> slower and larger! not worth it
#   Level 15: Block Size:  16K Size: 30462272 0.236 | 0.2909 +- 0.0013 | 0.33
#   Level 15: Block Size:  32K Size: 29618496 0.241 | 0.3074 +- 0.0012 | 0.343
#   Level 15: Block Size:  64K Size: 28856640 0.246 | 0.3124 +- 0.0012 | 0.35
#   Level 15: Block Size: 128K Size: 27685184 0.286 | 0.3534 +- 0.0016 | 0.412
#   Level 15: Block Size: 256K Size: 26841408 0.335 | 0.4066 +- 0.0014 | 0.456
#   Level 15: Block Size: 512K Size: 28815680 0.362 | 0.4342 +- 0.0014 | 0.50  -> larger and slower!?
#   Level 15: Block Size:   1M Size: 28164416 0.476 | 0.5389 +- 0.0016 | 0.654
#
#   Level 22: Block Size:   4K Size: 33190208 0.30  | 0.3803 +- 0.0016 | 0.453
#   Level 22: Block Size:  16K Size: 30310720 0.257 | 0.3048 +- 0.0009 | 0.337
#   Level 22: Block Size:  32K Size: 29299008 0.245 | 0.3172 +- 0.0012 | 0.353
#   Level 22: Block Size:  64K Size: 28569920 0.258 | 0.3204 +- 0.0013 | 0.363
#   Level 22: Block Size: 128K Size: 27414848 0.267 | 0.327  +- 0.0014 | 0.393
#   Level 22: Block Size: 256K Size: 26616128 0.327 | 0.3952 +- 0.0014 | 0.47  -> seems like an okayish tradeoff
#   Level 22: Block Size: 512K Size: 25882944 0.416 | 0.4762 +- 0.0016 | 0.537
#   Level 22: Block Size:   1M Size: 25313600 0.565 | 0.6092 +- 0.0013 | 0.677 -> still faster than gzip or any zstd!
#
# - 256K block size doesn't even have much of a speed difference between level 15 and 22.
#
# result="$APP_BASE.uncompressed.AppImage"
# appimagetool --mksquashfs-opt -noI --mksquashfs-opt -noId --mksquashfs-opt -noD --mksquashfs-opt -noF \
#     --mksquashfs-opt -noX  --no-appstream "$APP_BASE.AppDir" "$result" &>/dev/null
# for level in $( seq 1 22 ); do
#     result="$APP_BASE.zstd.$level.AppImage"
#     appimagetool --comp zstd --mksquashfs-opt -Xcompression-level --mksquashfs-opt "$level" --no-appstream \
#         "$APP_BASE.AppDir" "$result" &>/dev/null
#     times=$( for i in $( seq 25 ); do ( time "./$result" --help ) 2>&1 | sed -nr 's/^real.*0m([0-9.]+)s/\1/p'; done )
#     printf "Level %i: Size: %i %s\n" "$level" "$( stat -c %s "$result" )" "$( uncertainValue $times )"
# done
# for level in 15 22; do
# for blockSize in 4K 16K 32K 64K 128K 256K 512K 1M; do
#     result="$APP_BASE.zstd.$level.blocksize.$blockSize.AppImage"
#     appimagetool --comp zstd --mksquashfs-opt -Xcompression-level --mksquashfs-opt "$level" --no-appstream \
#         --mksquashfs-opt -b --mksquashfs-opt "$blockSize" "$APP_BASE.AppDir" "$result" &>/dev/null
#     times=$( for i in $( seq 25 ); do ( time "./$result" --help ) 2>&1 | sed -nr 's/^real.*0m([0-9.]+)s/\1/p'; done )
#     printf "Level %i: Block Size: %s Size: %i %s\n" "$level" "$blockSize" "$( stat -c %s "$result" )" \
#            "$( uncertainValue $times )"
# done
# done

chmod u+x "$APP_BASE.AppImage"
version=$( ./"$APP_BASE.AppImage" --version | sed -n -E 's|ratarmount ([0-9.]+)|\1|p' &>/dev/null )
if [[ -z "$version" ]]; then
    version=$( sed -n -E "s|.*__version__ = '([0-9.]+).*'|\1|p" ../ratarmount/version.py )
fi
if [[ -n "$version" ]]; then
    'mv' -- "$APP_BASE.AppImage" "ratarmount-$version-$APPIMAGE_ARCH.AppImage"
fi

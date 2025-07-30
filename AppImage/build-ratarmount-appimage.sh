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
    yum install -y fuse fakeroot patchelf fuse-libs libsqlite3x strace desktop-file-utils libzstd-devel
    if [[ "$APPIMAGE_VARIANT" != 'slim' ]]; then
        yum install -y libarchive libarchive-devel lzop lzo lzo-devel
    fi
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
        "$APP_PYTHON_BIN" -I -m pip install --no-cache-dir rapidgzip || exit 1
    fi

    # Install first, because it exactly pins cachetools to 6.0.0, nothing else allowed, instead of doing
    # more relaxed matching. It works with major version 5 as well as 6, so it is fine to force a downgrade
    # to make it work with: # https://github.com/googleapis/google-auth-library-python/blob/main/setup.py
    # which requires "cachetools>=2.0.0,<6.0" and is required by gcsfs.
    "$APP_PYTHON_BIN" -I -m pip install --no-cache-dir ext4 &>/dev/null
    "$APP_PYTHON_BIN" -I -m pip install --no-cache-dir cachetools==5 &>/dev/null

    # https://github.com/nathanhi/pyfatfs/issues/41
    "$APP_PYTHON_BIN" -I -m pip install --no-cache-dir \
        'git+https://github.com/mxmlnkn/pyfatfs.git@master#egginfo=pyfatfs'

    "$APP_PYTHON_BIN" -I -m pip install --no-cache-dir ../core || exit 1

    if [[ "$APPIMAGE_VARIANT" == 'slim' ]]; then
        # Especially do not install fsspec-backends, which quadtruple the AppImage size.
        "$APP_PYTHON_BIN" -I -m pip install --no-cache-dir .. || exit 1
        "$APP_PYTHON_BIN" -I -m pip uninstall -y --no-cache-dir libarchive-c
        return
    fi

    "$APP_PYTHON_BIN" -I -m pip install --no-cache-dir ..[full,sqlar] || exit 1

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
    # The move from Python 3.12 to Python 3.13 bloads the AppImage by another 50% from 40 MB to 60 MB :(
    # I think it is time for a slim version, especially when adding a Qt GUI in the future.
    # The problem is, even without fsspec backensd, the AppImage still comes out as 36 MB!
    # The main contributors to size are:
    #   - libicudata.so (~40 MB), which for some reason was not bundled for ratarmount-0.15.0.
    #     Find out why, e.g., by looking for it with 'readelf -d elfbin' for all shared libraries.
    #     -> used by libicuuc.so.74 by libxml2 by libarchive. libarchive will not work without this :(
    #        I guess the slim version also has to work without libarchive.
    #   - rapidgzip, which has a rather large binary for some reason (lookup tables?).
    #     Maybe use indexed_gzip for the slim AppImage until I have fixed the size (and GIL) problems :(.
    #   - Another large contributour is python-zstandard needed by PySquashFSImage!
    #   - 0.15.2 also had a rapidgzip .so file that was only 10 MB instead of 40 MB! strip --strip-debug helps!
    #     Stripping the binary reduces the AppImage from 20 MB to 13 MB. I guess for an AppImage version called
    #     slim, it is fine to trade debugability for a smaller size.
    # These are untested but small enough that we can just install them for now. Maybe they even work.
    "$APP_PYTHON_BIN" -I -m pip install --no-cache-dir gcsfs adlfs dropboxdrivefs
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
    # because it is dynamically loaded by python-libarchive-c, which linuxdeploy does not notice automatically.
    local libraries package packages yumCommand=''
    libraries=( $( find /lib64/ -name 'libcrypto.so*' ) )

    if commandExists repoquery; then
        yumCommand='repoquery'
    elif commandExists dnf; then
        yumCommand='dnf repoquery'
    elif commandExists yum; then
        yumCommand='yum'
    elif commandExists dpkg; then
        packages=(libfuse2)
        if [[ "$APPIMAGE_VARIANT" != 'slim' ]]; then
            packages+=(libarchive13 libarchive-dev lzo liblzma5)
        fi

        # On Ubuntu 24.04.2 LTS for ARM, installing libfuse2 does install libfuse2t64 (for 64-bit time_t support!).
        # Unfortunately, dpkg -L does not know about this alias -.-.
        for package in "${packages[@]}"; do
            if dpkg -L "$package" &>/dev/null; then
                libraries+=( $( dpkg -L "$package" | 'grep' '/lib.*[.]so' ) )
            elif dpkg -L "${package}t64" &>/dev/null; then
                libraries+=( $( dpkg -L "${package}t64" | 'grep' '/lib.*[.]so' ) )
            else
                echo "Failed to find $package"'!'
                exit 1
            fi
        done
    else
        echo -e "\e[31mCannot gather FUSE libs into AppImage without (dnf) repoquery.\e[0m"
    fi

    if [[ -n "$yumCommand" ]]; then
        packages=(fuse-libs)
        if [[ "$APPIMAGE_VARIANT" != 'slim' ]]; then
            packages+=(libarchive libarchive-devel lzo xz-devel)
        fi

        for package in "${packages[@]}"; do
            libraries+=( $( $yumCommand -l "$package" | 'grep' 'lib64.*[.]so' ) )
        done
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

    if [[ "$APPIMAGE_VARIANT" != 'slim' ]]; then
        APPIMAGE_EXTRACT_AND_RUN=1 linuxdeploy --appdir="$APP_DIR" "${libraries[@]/#/--library=}" \
            --executable="$( which fusermount )" --executable="$( which lzop )"
    fi
}

function trimAppImage()
{
    APP_PYTHON_BASE="${APP_DIR}/opt/python${APP_PYTHON_VERSION}"
    APP_PYTHON_LIB="${APP_PYTHON_BASE}/lib/python${APP_PYTHON_VERSION}"
    "$APP_PYTHON_BIN" -s -m pip uninstall -y build setuptools wheel pip

    # site-packages/test   https://github.com/fsspec/dropboxdrivefs/issues/23
    # site-packages/tests  https://github.com/skelsec/unicrypto/issues/9
    'rm' -rf \
        "$APP_PYTHON_LIB/site-packages/indexed_gzip/tests" \
        "$APP_PYTHON_LIB/site-packages/indexed_gzip/"*.c \
        "$APP_PYTHON_LIB/site-packages/indexed_gzip/"*.h \
        "$APP_PYTHON_LIB/site-packages/indexed_gzip/"*.pxd \
        "$APP_PYTHON_LIB/site-packages/test" \
        "$APP_PYTHON_LIB/site-packages/tests" \
        "$APP_PYTHON_LIB/site-packages/crcmod/test.py" \
        "$APP_PYTHON_LIB/site-packages/sniffio/_tests" \
        "$APP_PYTHON_LIB/site-packages/fsspec/tests" \
        "$APP_PYTHON_LIB/site-packages/adlfs/tests" \
        "$APP_PYTHON_LIB/site-packages/aioitertools/tests" \
        "$APP_PYTHON_LIB/lib-dynload/_test"* \
        "$APP_PYTHON_LIB/config-"*

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

    find "$APP_PYTHON_BASE/bin/" -type f -not -name 'python*' -delete

    find "$APP_DIR/usr/lib/" -name 'libtk*.so' -delete
    find "$APP_DIR/usr/lib/" -name 'libtcl*.so' -delete

    # May be worth thinking about not deleting them when I have a GUI. But even then, all systems that may want
    # to use a GUI should have these installed with X11, I think, and I had some bad issues with bundled X11
    # libraries not being protocol/binary compatible with the installed X11 server and segfaulting.
    rm -rf \
        "$APP_PYTHON_LIB/lib-dynload/"*tkinter* \
        "$APP_DIR/usr/lib/"lib{X,freetype,fontconfig}*.so*

    # find "$APP_DIR" -type f -name '*.so*' -exec bash -c 'readelf -d "$0" | grep libicu && echo "$0"' {} \;
    #  <- required by libicuuc.so.74 (I think) <- required by libxml2.so.2 <- libarchive.so
    # Are there shared libraries not being needed?
    # Check with: find "$APP_DIR" -type f -name '*.so*' -exec bash -c 'echo $0; readelf -d "$0" |
    #     sed -nr "s|.*Shared library: \[([^]]*)\]|  \1|p" |
    #     grep -v -E "lib(c|m|pthread|stdc[+][+]|gcc_s|z|dl)[.]so"' {} \;
    #find "$APP_DIR/usr/lib/" -name 'libicu*.so*' -delete

    if [ "$APPIMAGE_VARIANT" == 'slim' ]; then
        find "$APP_DIR" -name '__pycache__' -print0 | xargs -0 rm -r

        find "$APP_PYTHON_LIB/site-packages/" -name '*.so' -size +128k -print0 | xargs -0 strip --strip-debug
        find "${APP_DIR}/" -name '*.so' -size +128k -print0 | xargs -0 strip --strip-debug
    else
        # Add compiled bytecode for faster speedup. https://docs.python.org/3/glossary.html#term-bytecode states:
        # > This “intermediate language” is said to run on a virtual machine that executes the machine code
        # > corresponding to each bytecode. Do note that bytecodes are not expected to work between different
        # > Python virtual machines, nor to be stable between Python releases.
        # Ergo, as long as we create the bytecode with the bundled Python interpreter, then it seems fine to me.
        # And even if not, I would assume mismatching bytecode to be ignored as it often is accidentally bundled
        # or simply left over after an update.
        # Some solutions such as PyInstaller seem to always bundle the bytecode and even offer to remove the original
        # source code: https://pyinstaller.org/en/stable/operating-mode.html#hiding-the-source-code
        #
        # Benchmarks with ratarmount 1.1.1:
        #
        # Repack it to show that the repacking itself is not the cause of the speedup.
        #   ./ratarmount-1.1.1-slim-x86_64.AppImage --appimage-extract
        #   ARCH=x86_64 appimagetool --comp zstd --mksquashfs-opt -Xcompression-level \
        #       --mksquashfs-opt 22 --mksquashfs-opt -b --mksquashfs-opt 256K --no-appstream \
        #       squashfs-root ratarmount-repacked.AppImage
        #   time ./ratarmount-repacked.AppImage                       # 1.056s 0.969s 0.953s 1.020s 1.008s
        #   stat -c %s ./ratarmount-repacked.AppImage                 # 13048312
        #
        # With bytecode:
        #   squashfs-root/usr/bin/python3 -I -m compileall squashfs-root/opt/python3*/lib/
        #   appimagetool ...
        #   time ./ratarmount-compiled.AppImage                       # 0.365s 0.401s 0.392s 0.342s 0.431s
        #   stat -c %s ./ratarmount-compiled.AppImage                 # 17615352
        #  -> 2-3x faster, +35 % size (13.0 -> 17.6) MB
        #
        # Repeat with full version and actual mounting, i.e., expensive fsspec is imported.
        #   time ./ratarmount-full-repacked.AppImage mounted mounted  # 3.273s 3.222s 3.255s 3.392s 3.258s
        #   stat -c %s ./ratarmount-full-repacked.AppImage            # 63678968
        # With bytecode:
        #   time ./ratarmount-full-compiled.AppImage mounted mounted  # 1.361s 1.107s 1.237s 1.102s 1.314s
        #   stat -c %s ./ratarmount-full-compiled.AppImage            # 76659192
        #  -> 2-3x faster, + 20.3% size (63.7 -> 76.7) MB
        #
        # Tradeoffs when compiling only a subset of .py files with:
        # find squashfs-root/ -iname '*.py' -size +128k -print0 | xargs -0 squashfs-root/usr/bin/python -I -m py_compile
        # full version:
        #   Cutoff  Image Size  Startup Times
        #   0       76659192    0.570s 0.605s 0.623s 0.567s 0.554s
        #   16K     72911352    0.838s 0.833s 0.774s 0.755s 0.705s
        #   64K     67549688    1.035s 1.082s 1.048s 1.030s 1.018s
        #   128K    65522168    1.202s 1.206s 1.167s 1.214s 1.187s
        #   inf     63678968    1.398s 1.436s 1.447s 1.378s 1.373s
        # slim version:
        #   Cutoff  Image Size  Startup Times
        #   0       17611256    0.422s 0.390s 0.420s 0.381s 0.343s
        #   16K     16497144    0.541s 0.518s 0.504s 0.568s 0.543s
        #   64K     14502392    0.702s 0.713s 0.787s 0.746s 0.754s
        #   128K    13482488    0.892s 0.903s 0.889s 0.889s 0.894s
        #   inf     13048312    1.022s 1.021s 1.052s 1.020s 1.011s
        #  -> Note that slim only has 2 files larger than 128 KiB: _pydecimal.py and typing_extensions.py
        #
        # du -b $( find "$APP_PYTHON_LIB" -iname '*.py' ) | sort -nr | head -20
        #   227283  "$APP_PYTHON_LIB"/_pydecimal.py
        #   157408  "$APP_PYTHON_LIB"/site-packages/typing_extensions.py
        #   127125  "$APP_PYTHON_LIB"/inspect.py
        #   118836  "$APP_PYTHON_LIB"/typing.py
        #   112444  "$APP_PYTHON_LIB"/tarfile.py
        #   111120  "$APP_PYTHON_LIB"/email/_header_value_parser.py
        #   109175  "$APP_PYTHON_LIB"/site-packages/Cryptodome/SelfTest/PublicKey/test_import_ECC.py -> unused?
        #   106749  "$APP_PYTHON_LIB"/doctest.py                                                     -> unused?
        #   105004  "$APP_PYTHON_LIB"/site-packages/rarfile.py
        #   103515  "$APP_PYTHON_LIB"/urllib/request.py
        #   101155  "$APP_PYTHON_LIB"/argparse.py
        #   96395   "$APP_PYTHON_LIB"/site-packages/Cryptodome/Util/number.py
        #   93593   "$APP_PYTHON_LIB"/_pyio.py
        #   92087   "$APP_PYTHON_LIB"/_pydatetime.py
        #   88895   "$APP_PYTHON_LIB"/site-packages/psutil/tests/test_linux.py                       -> unused?
        #   88747   "$APP_PYTHON_LIB"/subprocess.py
        #   88339   "$APP_PYTHON_LIB"/zipfile/__init__.py
        #   86668   "$APP_PYTHON_LIB"/site-packages/psutil/__init__.py                               -> unused?
        #   86028   "$APP_PYTHON_LIB"/site-packages/psutil/_pslinux.py
        #   83437   "$APP_PYTHON_LIB"/logging/__init__.py
        "$APP_PYTHON_BIN" -s -m compileall "$APP_PYTHON_LIB"
    fi

    find "$APP_DIR" -type d -empty -print0 | xargs -0 -I{} rmdir -- {}
    find "$APP_DIR" -type d -empty -print0 | xargs -0 -I{} rmdir -p -- {}
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
    APP_PYTHON_VERSION=3.13
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
    'mv' -- "$APP_BASE.AppImage" "ratarmount-$version-$APPIMAGE_VARIANT-$APPIMAGE_ARCH.AppImage"
fi

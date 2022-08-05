#!/usr/bin/bash
appname=ratarmount
arch=$AUDITWHEEL_ARCH
platform=${AUDITWHEEL_PLAT%_$arch}
appbase=$appname-$platform_$arch
appdir=$appbase.AppDir
python_tag=cp39-cp39
python_libarchive_ext_url=https://github.com/Vadiml1024/python-libarchive/releases/download/V3.6.1-extended-36/python_libarchive_ext-3.6.1-cp39-cp39-manylinux_2_17_x86_64.manylinux2014_x86_64.whl

echo Install System Build Tools
ln -s python3.9 /usr/local/bin/python3
export PATH="/usr/local/bin:$PATH"
python3 -m pip install python-appimage
yum -y install epel-release
yum install -y dnf fuse fakeroot patchelf fuse-libs libsqlite3x strace desktop-file-utils

echo Install AppImage Tooling
curl -L -o /usr/bin/appimagetool 'https://github.com/AppImage/AppImageKit/releases/download/13/appimagetool-x86_64.AppImage'
chmod u+x /usr/bin/appimagetool
curl -L -o /usr/bin/linuxdeploy 'https://github.com/linuxdeploy/linuxdeploy/releases/download/continuous/linuxdeploy-x86_64.AppImage'
chmod u+x /usr/bin/linuxdeploy

echo Create Base Python AppImage platform: $platform arch: $arch
python3 -m python_appimage build local -p $(which python3)
pyversion_string=( $(python3 -V) ); pyfullversion=${pyversion_string[1]}
python_base_image="python$pyfullversion-$python_tag-${platform}_${arch}.AppImage"
pyver=${pyfullversion%.[0-9]*} # remove the last part of the version number
mv python$pyfullversion-$arch.AppImage $python_base_image

echo Build Base Python AppImage With Ratarmount Metadata
python3 -m python_appimage build app -b $python_base_image -n ratarmount-$platform AppImage/

echo Extract AppImage to AppDir for Further Modification

./${appbase}.AppImage --appimage-extract
mv squashfs-root/ "$appdir"

echo Install Ratarmount into AppDir
apppython=$appdir/opt/python3.9/bin/python3.9
"$apppython" -I -m pip install --no-cache-dir  "$python_libarchive_ext_url"
"$apppython" -I -m pip install --no-cache-dir ./core
"$apppython" -I -m pip install --no-cache-dir .

echo Bundle System Dependencies into AppDir
# Note that manylinux2014 already has libsqlite3.so.0 inside /usr/lib.
cp -a $( dnf repoquery -l fuse-libs | 'grep' 'lib64.*[.]so' ) "$appdir"/usr/lib/
APPIMAGE_EXTRACT_AND_RUN=1 linuxdeploy --appdir="$appdir" \
            --library=/usr/lib64/libfuse.so.2 \
            --library=/usr/lib64/libulockmgr.so.1 \
            --executable=/usr/bin/fusermount \
            --executable=/usr/bin/ulockmgr_server
#(cd "$appdir"/usr/lib  && rm -f libc.so.6 libc.so &&  ln -s libc-2.28.so libc.so.6 && ln -s libc-so.6 libc.so)
echo Clean up Unnecessary Files from AppDir
"$apppython" -s -m pip uninstall -y build setuptools wheel pip
rm -rf "$appdir/opt/python3.9/lib/python3.9/site-packages/indexed_gzip/tests" \
               "$appdir/opt/python3.9/include" \
               "$appdir/usr/share/tcltk" \
               "$appdir/usr/lib/libtk8.5.so" \
               "$appdir/usr/lib/libtcl8.5.so" \
               "$appdir/opt/python3.9/lib/python3.9/ensurepip" \
               "$appdir/opt/python3.9/lib/python3.9/lib2to3" \
               "$appdir/opt/python3.9/lib/python3.9/tkinter" \
               "$appdir/opt/python3.9/lib/python3.9/unittest"
find "$appdir" -type d -empty -print0 | xargs -0 rmdir
find "$appdir" -type d -empty -print0 | xargs -0 rmdir
find "$appdir" -name '__pycache__' -print0 | xargs -0 rm -r

echo Create AppImage from Modified AppDir
APPIMAGE_EXTRACT_AND_RUN=1 ARCH=x86_64 appimagetool --no-appstream "$appbase".App{Dir,Image}


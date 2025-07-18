# AppImages

The AppImages can be downloaded, made executable with `chmod u+x *.AppImage`, and executed directly to start ratarmount.

There are also usability helper for AppImages such as [AppImageLauncher](https://github.com/TheAssassin/AppImageLauncher) and [AM](https://github.com/ivan-hc/AM).

If the AppImage is too slow to start up or to reduce memory overhead for the AppImage itself, the Appimage can also be manually unpacked and installed:

 - Extract with `ratarmount-*.AppImage --appimage-extract`
 - The extracted AppImage can be started by executing `squashfs-root/AppRun`.
 - Rename and move the resulting generic `squashfs-root` folder wherever you want, e.g., `/opt/ratarmount-<version>`.
 - Add a link to `/opt/ratarmount-<version>/AppRun` into some folder that is in your `PATH` variable, e.g., with:
   `ln -s /opt/ratarmount-<version>/AppRun ~/.local/bin/ratarmount`

## Slim Version

Supports: 7z, ASAR, bzip2, EXT4, FAT, gzip, RAR, SQLAR, TAR, XZ, ZIP, zlib, ZStandard

## Normal / Full Version

Supports:

 - all slim formats
 - libarchive (ar, CAB, cpio, grzip, ISO9660, lrzip, LZ4, lzip, LZMA, lzop, RPM, UU, WARC, XAR, Z), SquashFS
 - all remote protocols (ftp://, git://, http://, ssh://, ...)


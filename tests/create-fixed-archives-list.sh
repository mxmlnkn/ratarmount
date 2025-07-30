#!/usr/bin/env bash

tests=(
    # Not done with pytest because it takes more than 10x longer for some reason.
    # Without pytest, mounting takes 4.5 s and md5sum takes 17.5 s.
    cb5d4faf665db396dc34df1689ef1da8 tests/sparse-file-larger-than-8GiB-followed-by-normal-file.tar.zst sparse
    c157a79031e1c40f85931829bc5fc552 tests/sparse-file-larger-than-8GiB-followed-by-normal-file.tar.zst foo
)
pytestedTests=()


if python3 -c 'import libarchive' &>/dev/null; then
pytestedTests+=(
    2709a3348eb2c52302a7606ecf5860bc tests/nested-with-symlink.7z                 foo/fighter/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/nested-with-symlink.7z                 foo/fighter/saucer
    2b87e29fca6ee7f1df6c1a76cb58e101 tests/nested-with-symlink.7z                 foo/lighter.tar/fighter/bar
    2709a3348eb2c52302a7606ecf5860bc tests/zip.7z                                 natsu.zip/ufo
    10d6977ec2ab378e60339323c24f9308 tests/zip.7z                                 natsu.zip/foo
    2709a3348eb2c52302a7606ecf5860bc tests/file-in-non-existing-folder.7z         foo2/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.7z                      foo/fighter/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.7z                      foo/jet/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/double-compressed-nested-tar.tar.7z.7z nested-tar.tar.7z/nested-tar.tar/foo/fighter/ufo

    19696f24a91fc4e8950026f9c801a0d0 tests/simple.lzma                            simple
    19696f24a91fc4e8950026f9c801a0d0 tests/simple.lrz                             simple
    19696f24a91fc4e8950026f9c801a0d0 tests/simple.lz4                             simple
    19696f24a91fc4e8950026f9c801a0d0 tests/simple.lzip                            simple
    19696f24a91fc4e8950026f9c801a0d0 tests/simple.lzo                             simple
    19696f24a91fc4e8950026f9c801a0d0 tests/simple.Z                               simple

    d3b07384d113edec49eaa6238ad5ff00 tests/single-file.ar                         bar
    d3b07384d113edec49eaa6238ad5ff00 tests/single-file.cab                        bar
    d3b07384d113edec49eaa6238ad5ff00 tests/single-file.iso.bz2                    single-file.iso/bar
    d3b07384d113edec49eaa6238ad5ff00 tests/single-file.xar                        bar
    d3b07384d113edec49eaa6238ad5ff00 tests/single-file.bin.cpio                   bar
    d3b07384d113edec49eaa6238ad5ff00 tests/single-file.crc.cpio                   bar
    d3b07384d113edec49eaa6238ad5ff00 tests/single-file.hpbin.cpio                 bar
    d3b07384d113edec49eaa6238ad5ff00 tests/single-file.hpodc.cpio                 bar
    d3b07384d113edec49eaa6238ad5ff00 tests/single-file.newc.cpio                  bar
    d3b07384d113edec49eaa6238ad5ff00 tests/single-file.odc.cpio                   bar
    # The contents of files and file hierarchy of WARC is subject to change.
    4aecced75ff52fdd39bb52dae192258f tests/hello-world.warc                       warc-specifications/primers/web-archive-formats/hello-world.txt
)
fi


# TODO Some bug with rarfile throwing: Failed the read enough data: req=304 got=51 and then seek(0) not working?
if ! uname | 'grep' -q -i darwin; then
tests+=(
    2709a3348eb2c52302a7606ecf5860bc tests/nested-with-symlink.rar                foo/fighter/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/nested-with-symlink.rar                foo/fighter/saucer
    2b87e29fca6ee7f1df6c1a76cb58e101 tests/nested-with-symlink.rar                foo/lighter.tar/fighter/bar
)
fi

# zipfile returns unseekable file object with Python 3.6. Therefore, I disabled it completely there.
if [[ -n "$python3MinorVersion" && "$python3MinorVersion" -gt 6 ]]; then
if ! uname | 'grep' -q -i darwin; then
tests+=(
    2709a3348eb2c52302a7606ecf5860bc tests/zip.rar                                natsu.zip/ufo
    10d6977ec2ab378e60339323c24f9308 tests/zip.rar                                natsu.zip/foo
    49b996b16f59ab6c87dea31e227f8798 tests/rar-misrecognized-as-zip.rar           bag.zip/README.md
    49b996b16f59ab6c87dea31e227f8798 tests/rar-misrecognized-as-zip.rar           bag.zip/CHANGELOG.md
    49b996b16f59ab6c87dea31e227f8798 tests/rar-misrecognized-as-zip.rar           bag1.zip/CHANGELOG.md
)
fi
pytestedTests+=(
    2709a3348eb2c52302a7606ecf5860bc tests/file-in-non-existing-folder.zip        foo2/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/rar.zip                                natsu.rar/ufo
    10d6977ec2ab378e60339323c24f9308 tests/rar.zip                                natsu.rar/foo
    2709a3348eb2c52302a7606ecf5860bc tests/nested-with-symlink.zip                foo/fighter/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/nested-with-symlink.zip                foo/fighter/saucer
    2b87e29fca6ee7f1df6c1a76cb58e101 tests/nested-with-symlink.zip                foo/lighter.tar/fighter/bar
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.zip                     foo/fighter/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.zip                     foo/jet/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/mockup-self-extracting.zip             ufo

    2709a3348eb2c52302a7606ecf5860bc tests/denormal-paths.zip                     ufo
    d3b07384d113edec49eaa6238ad5ff00 tests/denormal-paths.zip                     root/bar
    c157a79031e1c40f85931829bc5fc552 tests/denormal-paths.zip                     foo
)
fi

# pyfatfs depends on PyFilesystem2, which only works for Python < 3.12 because of the removed pkg_resources.
# https://github.com/nathanhi/pyfatfs/issues/41
if python3 -c 'import pyfatfs' &>/dev/null; then
tests+=(
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.fat12.bz2               folder-symlink.fat12/foo/fighter/ufo
)
fi

tests+=(
    f47c75614087a8dd938ba4acff252494 tests/simple-file-split.001                  simple-file-split
    f47c75614087a8dd938ba4acff252494 tests/simple-file-split.002                  simple-file-split
    d3b07384d113edec49eaa6238ad5ff00 tests/single-file-split.tar.001              bar
    d3b07384d113edec49eaa6238ad5ff00 'tests/#not-a-good-name! Ör, is it?.tar'     bar

    b026324c6904b2a9cb4b88d6d61c81d1 tests/2k-recursive-tars.tar.bz2              mimi/00001.tar/foo
    3059b91c3562cd29457192eb3c3fe376 tests/2k-recursive-tars.tar.bz2              mimi/01234.tar.versions/1
    8f30b20831bade7a2236edf09a55af60 tests/2k-recursive-tars.tar.bz2              mimi/01333.tar/foo
    f95f8943f6dcf7b3c1c8c2cab5455f8b tests/2k-recursive-tars.tar.bz2              mimi/02000.tar/foo
    c157a79031e1c40f85931829bc5fc552 tests/2k-recursive-tars.tar.bz2              mimi/foo
)

# https://github.com/indygreg/python-zstandard/issues/238
if [[ -n "$python3MinorVersion" && "$python3MinorVersion" -lt 14 ]]; then
pytestedTests+=(
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.zstd.squashfs           foo/fighter/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.zstd.squashfs           foo/jet/ufo
)
fi

if [[ -n "$python3MinorVersion" && "$python3MinorVersion" -ge 9 ]]; then
pytestedTests+=(
    # Directly testing the .ext.bz2 does not work with --ignore-zeros
    # because the nested TAR gets detected before trying EXT4.
    2709a3348eb2c52302a7606ecf5860bc tests/nested-tar-1M.ext4                     foo/fighter/ufo
    2b87e29fca6ee7f1df6c1a76cb58e101 tests/nested-tar-1M.ext4                     foo/lighter.tar/fighter/bar
    2709a3348eb2c52302a7606ecf5860bc tests/nested-tar-10M.ext4                    foo/fighter/ufo
    2b87e29fca6ee7f1df6c1a76cb58e101 tests/nested-tar-10M.ext4                    foo/lighter.tar/fighter/bar

)
fi

pytestedTests+=(
    2709a3348eb2c52302a7606ecf5860bc tests/nested-tar.asar                        foo/fighter/ufo
    2b87e29fca6ee7f1df6c1a76cb58e101 tests/nested-tar.asar                        foo/lighter.tar/fighter/bar

    2709a3348eb2c52302a7606ecf5860bc tests/nested-tar.sqlar                       foo/fighter/ufo
    2b87e29fca6ee7f1df6c1a76cb58e101 tests/nested-tar.sqlar                       foo/lighter.tar/fighter/bar
    2709a3348eb2c52302a7606ecf5860bc tests/nested-tar-trailing-slash.sqlar        foo/fighter/ufo
    2b87e29fca6ee7f1df6c1a76cb58e101 tests/nested-tar-trailing-slash.sqlar        foo/lighter.tar/fighter/bar
    2709a3348eb2c52302a7606ecf5860bc tests/nested-tar-denormal.sqlar              foo/fighter/ufo
    2b87e29fca6ee7f1df6c1a76cb58e101 tests/nested-tar-denormal.sqlar              foo/lighter.tar/fighter/bar
    2709a3348eb2c52302a7606ecf5860bc tests/nested-tar-compressed.sqlar            foo/fighter/ufo
    2b87e29fca6ee7f1df6c1a76cb58e101 tests/nested-tar-compressed.sqlar            foo/lighter.tar/fighter/bar

    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.gzip.squashfs           foo/fighter/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.lz4.squashfs            foo/fighter/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.lzma.squashfs           foo/fighter/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.lzo.squashfs            foo/fighter/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.no-compression.squashfs foo/fighter/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.xz.squashfs             foo/fighter/ufo

    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.gzip.squashfs           foo/jet/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.lz4.squashfs            foo/jet/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.lzma.squashfs           foo/jet/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.lzo.squashfs            foo/jet/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.no-compression.squashfs foo/jet/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.xz.squashfs             foo/jet/ufo

    2709a3348eb2c52302a7606ecf5860bc tests/file-in-non-existing-folder.rar        foo2/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.rar                     foo/fighter/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.rar                     foo/jet/ufo

    2709a3348eb2c52302a7606ecf5860bc tests/updated-file-implicitly-with-folder.tar foo/fighter
    2709a3348eb2c52302a7606ecf5860bc tests/updated-file-implicitly-with-folder.tar foo.versions/2/fighter
    c157a79031e1c40f85931829bc5fc552 tests/updated-file-implicitly-with-folder.tar foo.versions/1
    2709a3348eb2c52302a7606ecf5860bc tests/updated-file-implicitly-with-folder.tar bar/par/sora/natsu
    2709a3348eb2c52302a7606ecf5860bc tests/updated-file-implicitly-with-folder.tar bar/par/sora.versions/2/natsu
    cd85c6a5e5053c04f95e1df301c80755 tests/updated-file-implicitly-with-folder.tar bar/par/sora.versions/1

    d3b07384d113edec49eaa6238ad5ff00 tests/single-file.tar                        bar

    d3b07384d113edec49eaa6238ad5ff00 tests/single-file-with-leading-dot-slash.tar bar
    2b87e29fca6ee7f1df6c1a76cb58e101 tests/folder-with-leading-dot-slash.tar      foo/bar
    2709a3348eb2c52302a7606ecf5860bc tests/folder-with-leading-dot-slash.tar      foo/fighter/ufo

    2709a3348eb2c52302a7606ecf5860bc tests/denormal-paths.rar                     ufo
    d3b07384d113edec49eaa6238ad5ff00 tests/denormal-paths.rar                     root/bar
    c157a79031e1c40f85931829bc5fc552 tests/denormal-paths.rar                     foo

    2709a3348eb2c52302a7606ecf5860bc tests/denormal-paths.tar                     ufo
    d3b07384d113edec49eaa6238ad5ff00 tests/denormal-paths.tar                     root/bar
    c157a79031e1c40f85931829bc5fc552 tests/denormal-paths.tar                     foo

    2709a3348eb2c52302a7606ecf5860bc tests/single-nested-file.tar                 foo/fighter/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/single-nested-folder.tar               foo/fighter/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/nested-tar.tar                         foo/fighter/ufo
    2b87e29fca6ee7f1df6c1a76cb58e101 tests/nested-tar.tar                         foo/lighter.tar/fighter/bar
    # https://github.com/libarchive/libarchive/issues/2692
    #2709a3348eb2c52302a7606ecf5860bc tests/nested-tar.skippable-frame.lz4         foo/fighter/ufo
    #2b87e29fca6ee7f1df6c1a76cb58e101 tests/nested-tar.skippable-frame.lz4         foo/lighter.tar/fighter/bar
    2709a3348eb2c52302a7606ecf5860bc tests/nested-tar.tar.pzstd                   foo/fighter/ufo
    2b87e29fca6ee7f1df6c1a76cb58e101 tests/nested-tar.tar.pzstd                   foo/lighter.tar/fighter/bar
    2709a3348eb2c52302a7606ecf5860bc tests/directly-nested-tar.tar                fighter/ufo
    2b87e29fca6ee7f1df6c1a76cb58e101 tests/directly-nested-tar.tar                lighter.tar/fighter/bar
    2709a3348eb2c52302a7606ecf5860bc tests/nested-tar-with-overlapping-name.tar   foo/fighter/ufo
    2b87e29fca6ee7f1df6c1a76cb58e101 tests/nested-tar-with-overlapping-name.tar   foo/fighter.tar/fighter/bar
    2709a3348eb2c52302a7606ecf5860bc tests/hardlink.tar                           hardlink/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/hardlink.tar                           hardlink/natsu
    b3de7534cbc8b8a7270c996235d0c2da tests/concatenated.tar                       foo/fighter
    2709a3348eb2c52302a7606ecf5860bc tests/concatenated.tar                       foo/bar
    2709a3348eb2c52302a7606ecf5860bc tests/nested-symlinks.tar                    foo/foo
    2709a3348eb2c52302a7606ecf5860bc tests/nested-symlinks.tar                    foo/fighter/foo

    b3de7534cbc8b8a7270c996235d0c2da tests/updated-file.tar                       foo/fighter/ufo
    b3de7534cbc8b8a7270c996235d0c2da tests/updated-file.tar                       foo/fighter/ufo.versions/3
    9a12be5ebb21d497bd1024d159f2cc5f tests/updated-file.tar                       foo/fighter/ufo.versions/2
    2709a3348eb2c52302a7606ecf5860bc tests/updated-file.tar                       foo/fighter/ufo.versions/1

    9a12be5ebb21d497bd1024d159f2cc5f tests/updated-folder-with-file.tar           foo
    b3de7534cbc8b8a7270c996235d0c2da tests/updated-folder-with-file.tar           foo.versions/1/fighter
    b3de7534cbc8b8a7270c996235d0c2da tests/updated-folder-with-file.tar           foo.versions/1/fighter.versions/2
    2709a3348eb2c52302a7606ecf5860bc tests/updated-folder-with-file.tar           foo.versions/1/fighter.versions/1
    b3de7534cbc8b8a7270c996235d0c2da tests/updated-folder-with-file.tar           foo.versions/2/fighter
    b3de7534cbc8b8a7270c996235d0c2da tests/updated-folder-with-file.tar           foo.versions/2/fighter.versions/2
    2709a3348eb2c52302a7606ecf5860bc tests/updated-folder-with-file.tar           foo.versions/2/fighter.versions/1
    9a12be5ebb21d497bd1024d159f2cc5f tests/updated-folder-with-file.tar           foo.versions/3

    b3de7534cbc8b8a7270c996235d0c2da tests/updated-file-with-folder.tar           foo/fighter
    b3de7534cbc8b8a7270c996235d0c2da tests/updated-file-with-folder.tar           foo/fighter.versions/1
    9a12be5ebb21d497bd1024d159f2cc5f tests/updated-file-with-folder.tar           foo.versions/1
    b3de7534cbc8b8a7270c996235d0c2da tests/updated-file-with-folder.tar           foo.versions/2/fighter
    b3de7534cbc8b8a7270c996235d0c2da tests/updated-file-with-folder.tar           foo.versions/2/fighter.versions/1

    19696f24a91fc4e8950026f9c801a0d0 tests/simple.bz2                             simple
    19696f24a91fc4e8950026f9c801a0d0 tests/simple.gz                              simple
    19696f24a91fc4e8950026f9c801a0d0 tests/simple.xz                              simple
    19696f24a91fc4e8950026f9c801a0d0 tests/simple.zlib                            simple
    19696f24a91fc4e8950026f9c801a0d0 tests/simple.zst                             simple
    2709a3348eb2c52302a7606ecf5860bc tests/file-existing-as-non-link-and-link.tar foo/fighter/ufo
    d3b07384d113edec49eaa6238ad5ff00 tests/two-self-links-to-existing-file.tar    bar

    c9172d469a8faf82fe598c0ce978fcea tests/base64.gz                              base64

    2709a3348eb2c52302a7606ecf5860bc tests/nested-directly-compressed.tar.bz2     directly-compressed/ufo.bz2/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/nested-directly-compressed.tar.bz2     directly-compressed/ufo.gz/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/nested-directly-compressed.tar.bz2     directly-compressed/ufo.xz/ufo

    c157a79031e1c40f85931829bc5fc552 tests/absolute-file-incremental.tar          14130612002/tmp/foo
    8ddd8be4b179a529afa5f2ffae4b9858 tests/incremental-backup.level.0.tar         root-file.txt
    5bbf5a52328e7439ae6e719dfe712200 tests/incremental-backup.level.0.tar         foo/1
    c193497a1a06b2c72230e6146ff47080 tests/incremental-backup.level.0.tar         foo/2
    febe6995bad457991331348f7b9c85fa tests/incremental-backup.level.0.tar         foo/3
    3d45efe945446cd53a944972bf60810c tests/incremental-backup.level.1.tar         foo/3
    5bbf5a52328e7439ae6e719dfe712200 tests/incremental-backup.level.1.tar         foo/moved
    c157a79031e1c40f85931829bc5fc552 tests/single-file-incremental-mockup.tar     14130613451/foo
    c157a79031e1c40f85931829bc5fc552 tests/single-file-incremental-long-name-mockup.tar 14130613451/000000000100000000020000000003000000000400000000050000000006000000000700000000080000000009000000000A000000000B000000000C
    c157a79031e1c40f85931829bc5fc552 tests/single-file-incremental-long-name.tar 000000000100000000020000000003000000000400000000050000000006000000000700000000080000000009000000000A000000000B000000000C

    832c78afcb9832e1a21c18212fc6c38b tests/gnu-sparse-files.tar                   01.sparse1.bin
    832c78afcb9832e1a21c18212fc6c38b tests/gnu-sparse-files.tar                   02.normal1.bin
    832c78afcb9832e1a21c18212fc6c38b tests/gnu-sparse-files.tar                   03.sparse1.bin
)

'cp' 'tests/single-file.tar' 'tests/#not-a-good-name! Ör, is it?.tar'
zstd -d -k -f tests/sparse-file-larger-than-8GiB-followed-by-normal-file.tar.zst.zst

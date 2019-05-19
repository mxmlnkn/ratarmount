#!/bin/bash

cd -- "$( dirname -- "${BASH_SOURCE[0]}" )"

echoerr() { echo "$@" 1>&2; }

error=0
checkFileInTAR()
{
    local archive="$1"
    local fileInTar="$2"
    local correctChecksum="$3"

    local mountFolder="$( mktemp -d )"

    # try with index recreation
    python3 ratarmount.py -c --recursive "$archive" "$mountFolder" &>/dev/null
    checksum="$( md5sum "$mountFolder/$fileInTar" 2>/dev/null | sed 's| .*||' )"
    if test "$checksum" != "$correctChecksum"; then
        echoerr "File sum of '$fileInTar' in mounted TAR '$archive' does not match"'!'
        echoerr 'TEST FAILED!'
        return 1
    fi
    fusermount -u "$mountFolder" &>/dev/null

    # retry without forcing index recreation
    python3 ratarmount.py --recursive "$archive" "$mountFolder" &>/dev/null
    checksum="$( md5sum "$mountFolder/$fileInTar" 2>/dev/null | sed 's| .*||' )"
    if test "$checksum" != "$correctChecksum"; then
        echoerr "File sum of '$fileInTar' in mounted TAR '$archive' does not match"'!'
        echoerr 'TEST FAILED!'
        return 1
    fi
    fusermount -u "$mountFolder" &>/dev/null

    echoerr "Tested succesfully '$fileInTar' in '$archive' for checksum $correctChecksum"

    return 0
}

rm -f tests/*.index.pickle

checkFileInTAR tests/single-file.tar bar d3b07384d113edec49eaa6238ad5ff00
checkFileInTAR tests/single-file-with-leading-dot-slash.tar bar d3b07384d113edec49eaa6238ad5ff00
checkFileInTAR tests/folder-with-leading-dot-slash.tar foo/bar 2b87e29fca6ee7f1df6c1a76cb58e101
checkFileInTAR tests/folder-with-leading-dot-slash.tar foo/fighter/ufo 2709a3348eb2c52302a7606ecf5860bc
checkFileInTAR tests/single-nested-file.tar foo/fighter/ufo 2709a3348eb2c52302a7606ecf5860bc
checkFileInTAR tests/single-nested-folder.tar foo/fighter/ufo 2709a3348eb2c52302a7606ecf5860bc

checkFileInTAR tests/nested-tar.tar foo/fighter/ufo 2709a3348eb2c52302a7606ecf5860bc
checkFileInTAR tests/nested-tar.tar foo/lighter/fighter/bar 2b87e29fca6ee7f1df6c1a76cb58e101

checkFileInTAR tests/nested-tar-with-overlapping-name.tar foo/fighter/ufo 2709a3348eb2c52302a7606ecf5860bc
checkFileInTAR tests/nested-tar-with-overlapping-name.tar foo/fighter.tar/fighter/bar 2b87e29fca6ee7f1df6c1a76cb58e101

rm -f tests/*.index.pickle

exit $error

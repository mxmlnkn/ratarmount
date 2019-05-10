#!/bin/bash

error=0
checkFileInTAR()
{
    mountFolder="$( mktemp -d )"

    # try with index recreation
    python3 ratarmount.py -c "$1" "$mountFolder" &>/dev/null
    checksum="$( md5sum "$mountFolder/$2" 2>/dev/null | sed 's| .*||' )"
    if test "$checksum" != "$3"; then
        echo "File sum of '$2' in mounted TAR '$1'"' does not match! It seems there was a mounting error!'
        return 1
    fi
    fusermount -u "$mountFolder" &>/dev/null

    # retry without forcing index recreation
    python3 ratarmount.py "$1" "$mountFolder" &>/dev/null
    checksum="$( md5sum "$mountFolder/$2" 2>/dev/null | sed 's| .*||' )"
    if test "$checksum" != "$3"; then
        echo "File sum of '$2' in mounted TAR '$1'"' does not match! It seems there was a mounting error!'
        return 1
    fi
    fusermount -u "$mountFolder" &>/dev/null

    return 0
}

checkFileInTAR tests/single-file.tar bar d3b07384d113edec49eaa6238ad5ff00
checkFileInTAR tests/single-file-with-leading-dot-slash.tar bar d3b07384d113edec49eaa6238ad5ff00
checkFileInTAR tests/folder-with-leading-dot-slash.tar foo/bar 2b87e29fca6ee7f1df6c1a76cb58e101
checkFileInTAR tests/folder-with-leading-dot-slash.tar foo/fighter/ufo 2709a3348eb2c52302a7606ecf5860bc

exit $error

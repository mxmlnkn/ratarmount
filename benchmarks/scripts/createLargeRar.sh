#!/usr/bin/env bash

set -e

echoerr() { echo "$@" 1>&2; }


function createLargeRar()
{
    local folder iFolder firstSubFolder iFile

    # Creates an archive with many files with long names making file names the most memory consuming part of the index.
    if [[ ! "$nFolders" -eq "$nFolders" ]]; then
        echoerr "Argument 1 must be number to specify the number of folders containing each 1k files but is: $nFolders"
        return 1
    fi

    echoerr "Creating a archive with $(( nFolders * nFilesPerFolder )) files..."
    folder="$( mktemp -d -p "$( pwd )" )"

    iFolder=0
    firstSubFolder="$folder/$( printf "%0${nameLength}d" "$iFolder" )"
    mkdir -p -- "$firstSubFolder"

    for (( iFile = 0; iFile < nFilesPerFolder; ++iFile )); do
        base64 /dev/urandom | head -c "$nBytesPerFile" > "$firstSubFolder/$( printf "%0${nameLength}d" "$iFile" )"
    done

    for (( iFolder = 1; iFolder < nFolders; ++iFolder )); do
        subFolder="$folder/$( printf "%0${nameLength}d" "$iFolder" )"
        ln -s -- "$firstSubFolder" "$subFolder"
    done

    file="$nFolders-folders-with-$nFilesPerFolder-files-${nBytesPerFile}B-files.rar"
    ( cd -- "$folder"  && rar a "../$file" -r . )

    #file="$nFolders-folders-with-$nFilesPerFolder-files-${nBytesPerFile}B-files.qo+.rar"
    #( cd -- "$folder"  && rar a -qo+ "../$file" -r . )

    #file="$nFolders-folders-with-$nFilesPerFolder-files-${nBytesPerFile}B-files.qo-.rar"
    #( cd -- "$folder"  && rar a -qo- "../$file" -r . )

    'rm' -rf -- "$folder"
}


mountFolder=$( mktemp -d )

nameLength=32
nFilesPerFolder=1000
extendedBenchmarks=1

nFolders=100
nBytesPerFile=$(( 64 * 1024 ))

createLargeRar

rmdir "$mountFolder"


python3 -c 'import sys
import time
import rarfile

t0 = time.time()
f = rarfile.RarFile(sys.argv[1])
t1 = time.time()
print(f"Opening the RAR took: {t1-t0:.3f} s")
print("File Count:", len(f.infolist()))  # This is alway instant. Seems to get initialized during open
t2 = time.time()
print(f"Getting infolist took: {t2-t1:.3f} s")
' "$file"

# Creating the RAR is VERY slow. Takes many minutes for the 100 folders case.
# 10-folders-with-1000-files-65536B-files.rar 487 MiB -> 0.224
# 100-folders-with-1000-files-65536B-files.rar 4.8 GiB -> 4.821 2.242 2.311 2.317
# 100-folders-with-1000-files-65536B-files.qo+.rar 4.8 GiB -> 16.309 2.332 2.361 2.276
# 100-folders-with-1000-files-65536B-files.qo-.rar 4.8 GiB -> 4.072 2.283 2.245
# -qo[+-] option for the quick open service block seems to not impact open-performance at all after the file is cached.
# Unfortunately, these benchmarks also imply that opening a RAR file (and also a zip file) will always have the overhead
# for opening the file even if an index exists. I'm not even sure whether adding the index for the ZIP did any good for
# the user requesting it.
# In order to profit from an index, I would have to implement my own RAR/ZIP layer that is at least able to read the
# local records given a record/header offset.

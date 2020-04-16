#!/bin/bash

cd -- "$( dirname -- "${BASH_SOURCE[0]}" )"
cd ..

echoerr() { echo "$@" 1>&2; }

error=0

checkStat()
{
    local file="$1"
    if ! stat -- "$file" &>/dev/null; then
        echoerr -e "\e[37mCan't stat file or folder '$file'"'!'" Getting:"
        stat -- "$file"
        echoerr -e '\e[0m'
        return 1
    fi
}

verifyCheckSum()
{
    local mountFolder="$1"
    local fileInTar="$2"
    local archive="$3"
    local correctChecksum="$4"

    checksum="$( md5sum "$mountFolder/$fileInTar" 2>/dev/null | sed 's| .*||' )"
    if test "$checksum" != "$correctChecksum"; then
        echoerr -e "\e[37mFile sum of '$fileInTar' in mounted TAR '$archive' does not match when creating index"'!\e[0m'
        return 1
    fi
}

funmount()
{
    local mountFolder="$1"
    if mountpoint "$mountFolder" &>/dev/null; then
        fusermount -u "$mountFolder"
        while mountpoint "$mountFolder" &>/dev/null; do
            sleep 0.2s
        done
    fi
}

returnError()
{
    echoerr -e "\e[37m$*\e[0m"
    echoerr -e '\e[31mTEST FAILED!\e[0m'
    exit 1
}

checkFileInTAR()
{
    local type="$1"; shift
    local archive="$1"; shift
    local fileInTar="$1"; shift
    local correctChecksum="$1"

    local mountFolder="$( mktemp -d )"

    funmount "$mountFolder"

    # try with index recreation
    local cmd=( python3 ratarmount.py -c --recursive --serialization-backend "$type" "$archive" "$mountFolder" )
    "${cmd[@]}" &>/dev/null
    checkStat "$mountFolder" || returnError "${cmd[*]}"
    checkStat "$mountFolder/$fileInTar" || returnError "${cmd[*]}"
    verifyCheckSum "$mountFolder" "$fileInTar" "$archive" "$correctChecksum" || returnError "${cmd[*]}"
    funmount "$mountFolder"

    # retry without forcing index recreation
    local cmd=( python3 ratarmount.py --recursive --serialization-backend "$type" "$archive" "$mountFolder" )
    "${cmd[@]}" &>/dev/null
    checkStat "$mountFolder" || returnError "${cmd[*]}"
    checkStat "$mountFolder/$fileInTar" || returnError "${cmd[*]}"
    verifyCheckSum "$mountFolder" "$fileInTar" "$archive" "$correctChecksum" || returnError "${cmd[*]}"
    funmount "$mountFolder"

    rmdir "$mountFolder"

    echoerr "Tested succesfully '$fileInTar' in '$archive' for checksum $correctChecksum"

    return 0
}

checkFileInTARPrefix()
{
    # Prefixing support only works for SQLite backend, therefore it does not take a backend/type parameter
    local prefix="$1"; shift
    local archive="$1"; shift
    local fileInTar="$1"; shift
    local correctChecksum="$1"

    local mountFolder="$( mktemp -d )"

    funmount "$mountFolder"

    # try with index recreation
    local cmd=( python3 ratarmount.py -c --recursive --prefix "$prefix" "$archive" "$mountFolder" )
    "${cmd[@]}" &>/dev/null
    checkStat "$mountFolder" || returnError "${cmd[*]}"
    checkStat "$mountFolder/$fileInTar" || returnError "${cmd[*]}"
    verifyCheckSum "$mountFolder" "$fileInTar" "$archive" "$correctChecksum" || returnError "${cmd[*]}"
    funmount "$mountFolder"

    rmdir "$mountFolder"

    echoerr "Tested succesfully '$fileInTar' in '$archive' for checksum $correctChecksum"

    return 0
}

checkLinkInTAR()
{
    local archive="$1"; shift
    local fileInTar="$1"; shift
    local correctLinkTarget="$1"

    local mountFolder="$( mktemp -d )"

    funmount "$mountFolder"

    # try with index recreation
    local cmd=( python3 ratarmount.py -c --recursive "$archive" "$mountFolder" )
    "${cmd[@]}" &>/dev/null
    checkStat "$mountFolder" || returnError "${cmd[*]}"
    checkStat "$mountFolder/$fileInTar" || returnError "${cmd[*]}"
    if [[ $( readlink -- "$mountFolder/$fileInTar" ) != $correctLinkTarget ]]; then
        echoerr -e "\e[37mLink target of '$fileInTar' in mounted TAR '$archive' does not match"'!\e[0m'
        returnError "${cmd[*]}"
    fi
    funmount "$mountFolder"

    rmdir "$mountFolder"

    echoerr "Tested succesfully '$fileInTar' in '$archive' for link target $correctLinkTarget"

    return 0
}

createLargeTar()
(
    # creates a TAR with many files with long names making file names out to be the most memory consuming
    # part of the metadata required for the TAR index
    # https://www.gnu.org/software/tar/manual/html_section/tar_67.html
    # https://unix.stackexchange.com/questions/32795/what-is-the-maximum-allowed-filename-and-folder-size-with-ecryptfs
    #  -> some common file name limitations:
    #     . max 99 for GNU v7 (not the default tar archive)
    #     . Linux systems have 256 max file name length (and 4096 max path length)
    fileNameDataSizeInMB="$1"
    if ! test "$fileNameDataSizeInMB" -eq "$fileNameDataSizeInMB"; then
        echoerr "Argument 1 must be number in 1MiB to be used but is: $fileNameDataSizeInMB"
        return 1
    fi

    echoerr "Creating a tar with ${fileNameDataSizeInMB} MiB in file name meta data..."
    tarFolder="$( mktemp -d )"
    subFolder='A'

    # first create a TAR with files of length 96 characters with max 1024 files per folder to amount to ~1MiB of data
    # using subfolders alleviates the burden on listings and such things
    nameLength=96
    maxFilesPerFolder=1024
    for (( i = 0; i < 1024 * 1024 / nameLength; ++i )); do
        if test "$(( i % 1024 ))" -eq 0; then
            subFolder="$(( i / maxFilesPerFolder ))"
            mkdir -p "$tarFolder/$subFolder"
        fi
        touch "$tarFolder/$subFolder/$( printf '%096d' "$i" )"
    done

    tarFile1MiB='tests/large-tar-with-1-MiB-metadata.tar'
    tar -c -C "$tarFolder" -f "$tarFile1MiB" --owner=user --group=group .
    'rm' -rf -- "$tarFolder"

    if test "$fileNameDataSizeInMB" -eq 1; then
        printf '%s' "$tarFile1MiB"
        return 0
    fi

    echoerr "Done creating 1MiB TAR. Will now copy-paste it $fileNameDataSizeInMB times into a larger TAR."

    # Now, instead of spamming the host system with billions of files, make use of the recursive mounting of ratarmount
    # to increase the memory footprint by copy-pasting the TAR with 1MiB metadata n times

    largeTarFolder="$( mktemp -d )"

    for (( i = 0; i < fileNameDataSizeInMB; ++i )); do
        cp "$tarFile1MiB" "$largeTarFolder/$( printf '%05d' "$i" ).tar"
    done

    tarFileNMiB="tests/large-tar-with-$fileNameDataSizeInMB-MiB-metadata.tar"
    tar -c -C "$largeTarFolder" -f "$tarFileNMiB" --owner=user --group=group .
    'rm' -rf -- "$largeTarFolder"

    printf '%s' "$tarFileNMiB"
    return 0
)

memoryUsage()
{
    # monitors rss memory usage of given process ID and writes it into the given file

    if test $# -ne 2; then
        echoerr "Required two arguments: <pid> <time series file>"
    fi

    local pidToMonitor="$1"
    local timeSeriesFile="$2"

    echo '# seconds size resident share text lib data dirty' > "$timeSeriesFile"
    echo "# pageSize=$( getconf PAGESIZE )" >> "$timeSeriesFile"
    echo '# all values are measured in pages' >> "$timeSeriesFile"

    while printf '%s ' "$( date +%s.%N )" >> "$timeSeriesFile" &&
          cat "/proc/$pidToMonitor/statm" 2>/dev/null >> "$timeSeriesFile"
    do sleep 0.05s; done
}

testLargeTar()
{
    local fileNameDataSizeInMB="$1"
    local serializationLibrary="$2"

    local extraArgs=()
    if test -n "$serializationLibrary"; then
        extraArgs=( '--serialization-backend' "$serializationLibrary" )
    fi

    local largeTar="tests/large-tar-with-$fileNameDataSizeInMB-MiB-metadata.tar"
    if ! test -f "$largeTar"; then
        largeTar="$( createLargeTar $fileNameDataSizeInMB )"
    fi

    # clear up mount folder if already in use
    local mountFolder="$( mktemp -d )"
    if mountpoint "$mountFolder" &>/dev/null; then
        fusermount -u "$mountFolder"
        while mountpoint "$mountFolder" &>/dev/null; do sleep 0.2s; done
    fi

    # benchmark creating the index

    python3 ./ratarmount.py -c -f --recursive "${extraArgs[@]}" "$largeTar" "$mountFolder" &
    local ratarmountPid="$!"
    #trap "kill $ratarmountPid" SIGINT SIGTERM # for some reason makes the program unclosable ...

    local timeSeriesFile="benchmark-memory-${fileNameDataSizeInMB}-MiB-saving.dat"
    memoryUsage "$ratarmountPid" "$timeSeriesFile" &
    local memoryUsagePid="$!"

    while ! mountpoint -q "$mountFolder"; do sleep 1s; done
    fusermount -u "$mountFolder"
    wait "$memoryUsagePid"
    wait "$ratarmountPid"

    # do again but this time benchmark loading the created index

    python3 ./ratarmount.py -f --recursive "${extraArgs[@]}" "$largeTar" "$mountFolder" &
    local ratarmountPid="$!"

    local timeSeriesFile="benchmark-memory-${fileNameDataSizeInMB}-MiB-loading.dat"
    memoryUsage "$ratarmountPid" "$timeSeriesFile" &
    local memoryUsagePid="$!"

    while ! mountpoint -q "$mountFolder"; do sleep 1s; done
    fusermount -u "$mountFolder"
    wait "$memoryUsagePid"
    wait "$ratarmountPid"

    # cleanup

    rmdir "$mountFolder"

    echo "$timeSeriesFile"
}

getPeakMemoryFromFile()
{
    python3 -c "import sys, numpy as np
data = np.genfromtxt( sys.argv[1], skip_footer = 1 ).transpose()
print( int( np.max( data[1] ) ), int( np.max( data[2] ) ) )" "$1"
}

benchmarkSerialization()
{
    local benchmarksFolder=benchmarks/data
    local logFile="$benchmarksFolder/serializationBenchmark.dat"
    touch "$logFile"
    echo '# tarMiB indexCreationTime serializationTime serializedSize deserializationTime peakVmSizeCreation peakRssSizeCreation peakVmSizeLoading peakRssSizeLoading' >> "$logFile"
    mkdir -p -- "$benchmarksFolder"

    local type mib compression compressions
    for mib in 1 8 64 256; do
        for type in sqlite custom pickle2 pickle3 cbor msgpack rapidjson ujson simplejson; do
            compressions=( '' '.gz' '.lz4' )
            if [[ "$type" == 'sqlite' ]]; then compressions=( '' ); fi
            for compression in "${compressions[@]}"; do
                echoerr "Benchmarking ${type}.${compression} ..."

                printf '%i ' "$mib" >> "$logFile"

                testLargeTar "$mib" "${type}${compression}" | sed -n -r '
                    s|Creating offset dictionary for /[^:]*.tar took ([0-9.]+)s|\1|p;
                    s|Writing out TAR.* took ([0-9.]+)s and is sized ([0-9]+) B|\1 \2|p;
                    s|Loading offset dictionary.* took ([0-9.]+)s|\1|p;
                ' | sed -z 's|\n| |g' >> "$logFile"

                # not nice but hard to do differently as the pipe opens testLargeTar in a subshell and tee
                # redirects it directly to tty, so we can't store an output!
                local timeSeriesFile="benchmark-memory-${mib}-MiB-saving.dat"
                printf '%s %s ' $( getPeakMemoryFromFile "$timeSeriesFile" ) >> "$logFile"
                'mv' "$timeSeriesFile" "$benchmarksFolder/${type}${compression}-$timeSeriesFile"

                local timeSeriesFile="benchmark-memory-${mib}-MiB-loading.dat"
                printf '%s %s ' $( getPeakMemoryFromFile "$timeSeriesFile" ) >> "$logFile"
                'mv' "$timeSeriesFile" "$benchmarksFolder/${type}${compression}-$timeSeriesFile"

                echo " # ${type}${compression}" >> "$logFile"
            done
        done
    done
}




python3 tests/tests.py || returnError "tests/tests.py"

pylint --disable=C0326,C0103 ratarmount.py > pylint.log

rm -f tests/*.index.*

for type in sqlite custom pickle2 pickle3 cbor msgpack rapidjson ujson simplejson; do
    compressions=( '' '.gz' '.lz4' )
    if [[ "$type" == 'sqlite' ]]; then compressions=( '' ); fi
    for compression in "${compressions[@]}"; do
        echoerr "=== Testing Serialization Backend: ${type}${compression} ==="

        checkLinkInTAR tests/symlinks.tar foo ../foo
        checkLinkInTAR tests/symlinks.tar python /usr/bin/python

        tests=(
            d3b07384d113edec49eaa6238ad5ff00 tests/single-file.tar                        bar
            d3b07384d113edec49eaa6238ad5ff00 tests/single-file-with-leading-dot-slash.tar bar
            2b87e29fca6ee7f1df6c1a76cb58e101 tests/folder-with-leading-dot-slash.tar      foo/bar
            2709a3348eb2c52302a7606ecf5860bc tests/folder-with-leading-dot-slash.tar      foo/fighter/ufo
            2709a3348eb2c52302a7606ecf5860bc tests/single-nested-file.tar                 foo/fighter/ufo
            2709a3348eb2c52302a7606ecf5860bc tests/single-nested-folder.tar               foo/fighter/ufo
            2709a3348eb2c52302a7606ecf5860bc tests/nested-tar.tar                         foo/fighter/ufo
            2b87e29fca6ee7f1df6c1a76cb58e101 tests/nested-tar.tar                         foo/lighter.tar/fighter/bar
            2709a3348eb2c52302a7606ecf5860bc tests/nested-tar-with-overlapping-name.tar   foo/fighter/ufo
            2b87e29fca6ee7f1df6c1a76cb58e101 tests/nested-tar-with-overlapping-name.tar   foo/fighter.tar/fighter/bar
            2709a3348eb2c52302a7606ecf5860bc tests/hardlink.tar                           hardlink/ufo
            2709a3348eb2c52302a7606ecf5860bc tests/hardlink.tar                           hardlink/natsu
        )

        # Sparse file support is not backported to the old serializers
        if [[ $type == 'sqlite' ]]; then
            tests+=(
                832c78afcb9832e1a21c18212fc6c38b tests/gnu-sparse-files.tar                   01.sparse1.bin
                832c78afcb9832e1a21c18212fc6c38b tests/gnu-sparse-files.tar                   02.normal1.bin
                832c78afcb9832e1a21c18212fc6c38b tests/gnu-sparse-files.tar                   03.sparse1.bin
            )
        fi

        for (( iTest = 0; iTest < ${#tests[@]}; iTest += 3 )); do
            checkFileInTAR "${type}${compression}" "${tests[iTest+1]}" "${tests[iTest+2]}" "${tests[iTest]}"
            # For SQLite backend, check with BZip2 compression
            if [[ $type == 'sqlite' ]]; then
                tmpBz2=$( mktemp --suffix='.tar.bz2' )
                bzip2 --keep --stdout "${tests[iTest+1]}" > "$tmpBz2"
                checkFileInTAR "${type}${compression}" "$tmpBz2" "${tests[iTest+2]}" "${tests[iTest]}"
                'rm' -- "$tmpBz2"

                tmpGz=$( mktemp --suffix='.tar.gz' )
                gzip --keep --stdout "${tests[iTest+1]}" > "$tmpGz"
                checkFileInTAR "${type}${compression}" "$tmpGz" "${tests[iTest+2]}" "${tests[iTest]}"
                'rm' -- "$tmpGz"
            fi
        done
    done

    if [[ "$type" == 'sqlite' ]]; then
        checkFileInTARPrefix '' tests/single-nested-file.tar foo/fighter/ufo 2709a3348eb2c52302a7606ecf5860bc
        checkFileInTARPrefix foo tests/single-nested-file.tar fighter/ufo 2709a3348eb2c52302a7606ecf5860bc
        checkFileInTARPrefix foo/fighter tests/single-nested-file.tar ufo 2709a3348eb2c52302a7606ecf5860bc
    fi
done

#benchmarkSerialization # takes quite long, and a benchmark is not a test ...

rm -f tests/*.index.*
rmdir tests/*/

echo -e '\e[32mAll tests ran succesfully.\e[0m'

exit $error

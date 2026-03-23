#!/usr/bin/env bash

set -e

cd -- "$( dirname -- "${BASH_SOURCE[0]}" )/.." || { echo 'Failed to cd to git root!'; exit 1; }

source tests/common.sh


createLargeTar()
(
    # creates a TAR with many files with long names making file names out to be the most memory consuming
    # part of the metadata required for the TAR index
    # https://www.gnu.org/software/tar/manual/html_section/tar_67.html
    # https://unix.stackexchange.com/questions/32795/what-is-the-maximum-allowed-filename-and-folder-size-with-ecryptfs
    #  -> some common file name limitations:
    #     . max 99 for GNU v7 (not the default tar archive)
    #     . Linux systems have 256 max file name length (and 4096 max path length)
    fileNameDataSizeInMB=$1
    if ! test "$fileNameDataSizeInMB" -eq "$fileNameDataSizeInMB"; then
        echoerr "Argument 1 must be number in 1MiB to be used but is: $fileNameDataSizeInMB"
        return 1
    fi

    echoerr "Creating a tar with ${fileNameDataSizeInMB} MiB in file name meta data..."
    tarFolder="$( mktemp -d --suffix .test.ratarmount )"
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

    largeTarFolder="$( mktemp -d --suffix .test.ratarmount )" || return 1

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

    local largeTar="tests/large-tar-with-$fileNameDataSizeInMB-MiB-metadata.tar"
    if ! test -f "$largeTar"; then
        largeTar="$( createLargeTar "$fileNameDataSizeInMB" )"
    fi

    rm -f ratarmount.{stdout,stderr}.log

    # clear up mount folder if already in use
    local mountFolder
    mountFolder="$( mktemp -d --suffix .test.ratarmount )" || returnError "$LINENO" 'Failed to create temporary directory'
    MOUNT_POINTS_TO_CLEANUP+=( "$mountFolder" )

    # benchmark creating the index

    $RATARMOUNT_CMD -P "$parallelization" -c -f --recursive "$largeTar" "$mountFolder" &
    local ratarmountPid="$!"
    #trap "kill $ratarmountPid" SIGINT SIGTERM # for some reason makes the program unclosable ...

    local timeSeriesFile="benchmark-memory-${fileNameDataSizeInMB}-MiB-saving.dat"
    memoryUsage "$ratarmountPid" "$timeSeriesFile" &
    local memoryUsagePid="$!"

    waitForMountpoint "$mountFolder" || returnError 'Waiting for mountpoint timed out!'
    $RATARMOUNT_CMD -u "$mountFolder"
    wait "$memoryUsagePid"
    wait "$ratarmountPid"

    # do again but this time benchmark loading the created index

    $RATARMOUNT_CMD -P "$parallelization" -f --recursive "$largeTar" "$mountFolder" &
    local ratarmountPid="$!"

    local timeSeriesFile="benchmark-memory-${fileNameDataSizeInMB}-MiB-loading.dat"
    memoryUsage "$ratarmountPid" "$timeSeriesFile" &
    local memoryUsagePid="$!"

    waitForMountpoint "$mountFolder" || returnError 'Waiting for mountpoint timed out!'
    $RATARMOUNT_CMD -u "$mountFolder"
    wait "$memoryUsagePid"
    wait "$ratarmountPid"

    # cleanup

    safeRmdir "$mountFolder"

    echo "$timeSeriesFile"
}

getPeakMemoryFromFile()
{
    python3 -c "import sys, numpy as np
data = np.genfromtxt( sys.argv[1], skip_footer = 1 ).transpose()
print( int( np.max( data[1] ) ), int( np.max( data[2] ) ) )" "$1"
}


benchmarkDecoderBackends()
{
    local tmpFolder
    while read -r file; do
        TMP_FILES_TO_CLEANUP+=( "$file" )
        tmpFolder=$( dirname -- "$file" )
        compression=$( file --mime-type -- "$file" | sed 's|.*[/-]||;' )
        if [[ "$compression" == tar ]]; then continue; fi

        printf '% 5s : ' "$compression"
        case "$compression" in
            bzip2)
                python3 -m timeit 'from rapidgzip import IndexedBzip2File as IBF; IBF( '"'$file'"' ).read();'
                printf '% 5s : ' "pbz2"
                python3 -m timeit 'from rapidgzip import IndexedBzip2File as IBF; IBF( '"'$file'"', parallelization = 0 ).read();'
                ;;
            gzip)
                python3 -m timeit 'from indexed_gzip import IndexedGzipFile as IBF; IBF( '"'$file'"' ).read();'
                ;;
            xz)
                python3 -m timeit 'import xz; xz.open( '"'$file'"' ).read();'
                ;;
            zstd)
                python3 -m timeit 'from indexed_zstd import IndexedZstdFile as IBF; IBF( '"'$file'"' ).read();'
                ;;
        esac
    done < <( recompressFile 'tests/2k-recursive-tars.tar.bz2' )

    cleanup
    safeRmdir "$tmpFolder"
}


# Linting only to be done locally because in CI it is in separate steps
if [[ -z "$CI" ]]; then
    COLUMNS=98 $RATARMOUNT_CMD --help | sed '/# Metadata Index Cache/,$d' > tests/ratarmount-help.txt

    bash tests/run-style-checkers.sh
fi


# We need to run these tests without pytest because, for some reason,
# pytest slows the zip decryption fix down from 0.1 s to 1.1 s?!
if [[ $TEST_EXTERNAL_COMMAND -eq 0 ]]; then
    python3 core/tests/test_ZipMountSource.py
fi

bash tests/run-complex-usage-tests.sh
bash tests/run-fixed-archive-tests.sh
bash tests/run-remote-backend-tests.sh


if [[ $TEST_EXTERNAL_COMMAND -eq 0 ]]; then
    benchmarkDecoderBackends
fi


echo -e '\e[32mAll tests ran successfully.\e[0m'

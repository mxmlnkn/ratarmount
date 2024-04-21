#!/usr/bin/env bash

# E.g., call with:
# fname=benchmark-archivemount-$( date +%Y-%m-%dT%H-%M )
# rm tar-with*; bash benchmarkMounting.sh 2>"$fname.err" | tee "$fname.out"

#set -x
set -e

echoerr() { echo "$@" 1>&2; }


# Kinda obsolete parallelized bash version
# For 1MiB frames, the frequent process spawning for dd and and zstd cost the most of the time!
# Therefore, use a process pool in python to fix that speed issue.
function createMultiFrameZstd()
(
    frameSize=$1
    file=$2
    if [[ ! -f "$file" ]]; then echo "Could not find file '$file'." 1>&2; return 1; fi
    fileSize=$( stat -c %s -- "$file" )

    if [[ ! $frameSize =~ ^[0-9]+$ ]]; then
        echo "Frame size '$frameSize' is not a valid number." 1>&2
        return 1
    fi

    # Create a temporary file. I avoid simply piping to zstd
    # because it wouldn't store the uncompressed size.
    if [[ -d /dev/shm ]]; then tmpFolder=$( mktemp -d --tmpdir=/dev/shm ); fi
    if [[ -z $tmpFolder ]]; then tmpFolder=$( mktemp -d ); fi
    if [[ -z $tmpFolder ]]; then
        echo "Could not create a temporary folder for the frames." 1>&2
        return 1
    fi

    echo "Compress into $file.zst" 1>&2
    true > "$file.zst"
    nCores=$( nproc )  # nproc is actually pretty slow when used in a loop!
    for (( offset = 0; offset < fileSize; )); do
        # Do it chunk-wise because we are compressing the frames with temporary files in limited /dev/shm!
        printf '' > "$tmpFolder/offsets"
        for (( part = 0; part < 2 * nCores; ++part )); do
            echo "$part $offset" >> "$tmpFolder/offsets"
            (( offset += frameSize ))
            if (( offset >= fileSize )); then
                break
            fi
        done
        cat "$tmpFolder/offsets" | xargs -P 0 -I {} bash -c '
            file=$1
            frameSize=$2
            tmpFolder=$3
            part=${4% *}
            offset=${4#* }

            time dd if="$file" of="$tmpFolder/$part" bs=$(( 1024*1024 )) \
               iflag=skip_bytes,count_bytes skip="$offset" count="$frameSize" # 2>/dev/null
            zstd --rm -q -- "$tmpFolder/$part"
        ' bash "$file" "$frameSize" "$tmpFolder" {}

        compressedFrames=( $( sed -n -E 's|([0-9]+) .*|'"$tmpFolder"'/\1.zst|p' "$tmpFolder/offsets" ) )
        cat "${compressedFrames[@]}" >> "$file.zst"
        'rm' "${compressedFrames[@]}"
    done

    ls -la "$tmpFolder"
    'rm' -r -- "$tmpFolder"
)


cat <<EOF > createMultiFrameZstd.py
import concurrent.futures
import os
import sys

import zstandard


def compressZstd(data):
    return zstandard.ZstdCompressor().compress(data)


if __name__ == '__main__':
    filePath = sys.argv[1]
    frameSize = int(sys.argv[2])
    parallelization = os.cpu_count() * 2

    with open(filePath, 'rb') as file, open(
        filePath + ".zst", 'wb'
    ) as compressedFile, concurrent.futures.ThreadPoolExecutor(parallelization) as pool:
        results = []
        while True:
            toCompress = file.read(frameSize)
            if not toCompress:
                break
            results.append(pool.submit(compressZstd, toCompress))
            while len(results) >= parallelization:
                compressedData = results.pop(0).result()
                compressedFile.write(compressedData)

        while results:
            compressedFile.write(results.pop(0).result())
EOF

function createMultiFrameZstd()
{
    python3 createMultiFrameZstd.py "$@"
}

function benchmarkCommand()
{
    local commandToBenchmark=( "$@" )

    echoerr "Running: ${commandToBenchmark[*]} ..."

    if [[ "${commandToBenchmark[0]}" == 'fuse-archive' ]]; then
        tmpFile=$( mktemp )
        # We need to keep fuse-archive in foreground until it has finished mounting to get a useful max RSS estimate!
        { /bin/time -f '%e s %M kiB max rss' "${commandToBenchmark[@]}" -f; } 2>&1 1>/dev/null |
            'grep' 'max rss' > "$tmpFile" &
        pid=$!

        local mountPoint
        mountPoint=${commandToBenchmark[${#commandToBenchmark[@]} -1]}
        while ! command mountpoint -q "$mountPoint"; do sleep 0.1; done
        stat -- "$mountPoint"  &>/dev/null
        fusermount -u "$mountPoint"
        for (( i = 0; i < 30; ++i )); do
            if ! mountpoint -q "$mountPoint" && ! ps -p $pid &> /dev/null; then
                break
            fi
            sleep 1s
            echoerr "Waiting for mountpoint"
        done # throw error after timeout?
        rss=$( cat -- "$tmpFile" | sed -r 's|.* s ([0-9]+) kiB.*|\1|' )
        rm "$tmpFile"

        # This also remounted so that we get the mount point!
        duration=$( { /bin/time -f '%e s %M kiB max rss' \
                          bash -c 'fuse-archive "$@" && stat "${@: -1}"' "${commandToBenchmark[@]}"; } 2>&1 1>/dev/null |
                          'grep' 'max rss' )

        echoerr "Command took ${duration%% s *} s and $rss kiB"
    else
        duration=$( { /bin/time -f '%e s %M kiB max rss' \
                          "${commandToBenchmark[@]}"; } 2>&1 1>/dev/null |
                          'grep' 'max rss' )
        rss=$( printf '%s' "$duration" | sed -r 's|.* s ([0-9]+) kiB.*|\1|' )

        echoerr "Command took $duration"  # duration includes RSS, which is helpful to print here
    fi

    # Check for file path arguments
    for arg in "${commandToBenchmark[@]}"; do
        if [[ -f "$arg" ]]; then
            fileSize=$( stat --format=%s -- "$arg" )
        fi
    done

    echo "$cmd;\"${commandToBenchmark[*]}\";tar$compression;$nameLength;$nFolders;$nFilesPerFolder;$nBytesPerFile;${duration%% s *};$rss;$fileSize;\"$( date --iso-8601=seconds )\"" >> "$dataFile"
}


function benchmarkFunction()
{
    local commandToBenchmark=( "$@" )

    echoerr "Running: ${commandToBenchmark[*]} ..."

    # In contrast to time, /bin/time will not know bash functions!
    export -f "${commandToBenchmark[0]}"

    duration=$( { /bin/time -f '%e s %M kiB max rss' \
                      bash -c '"$@"' bash "${commandToBenchmark[@]}"; } 2>&1 |
                      'grep' 'max rss' )
    rss=$( printf '%s' "$duration" | sed -r 's|.* s ([0-9]+) kiB.*|\1|' )

    echoerr "Command took $duration"  # duration includes RSS, which is helpful to print here

    # Check for file path arguments
    for arg in "${commandToBenchmark[@]}"; do
        if [[ -f "$arg" ]]; then
            fileSize=$( stat --format=%s -- "$arg" )
        fi
    done

    echo "$cmd;\"${commandToBenchmark[*]}\";tar$compression;$nameLength;$nFolders;$nFilesPerFolder;$nBytesPerFile;${duration%% s *};$rss;$fileSize;\"$( date --iso-8601=seconds )\"" >> "$dataFile"
}


function createLargeTar()
{
    local tarFolder iFolder firstSubFolder iFile tarFile

    # creates a TAR with many files with long names making file names out to be the most memory consuming
    # part of the metadata required for the TAR index
    if [[ ! "$nFolders" -eq "$nFolders" ]]; then
        echoerr "Argument 1 must be number to specify the number of folders containing each 1k files but is: $nFolders"
        return 1
    fi

    echoerr "Creating a tar with $(( nFolders * nFilesPerFolder )) files..."
    tarFolder="$( mktemp -d -p "$( pwd )" )"

    iFolder=0
    firstSubFolder="$tarFolder/$( printf "%0${nameLength}d" "$iFolder" )"
    mkdir -p -- "$firstSubFolder"

    for (( iFile = 0; iFile < nFilesPerFolder; ++iFile )); do
        base64 /dev/urandom | head -c "$nBytesPerFile" > "$firstSubFolder/$( printf "%0${nameLength}d" "$iFile" )"
    done

    for (( iFolder = 1; iFolder < nFolders; ++iFolder )); do
        subFolder="$tarFolder/$( printf "%0${nameLength}d" "$iFolder" )"
        ln -s -- "$firstSubFolder" "$subFolder"
    done

    tarFile="tar-with-$nFolders-folders-with-$nFilesPerFolder-files-${nBytesPerFile}B-files.tar"
    # Transform and remove leading dot because fuse-archive had problems with that
    # https://github.com/google/fuse-archive/issues/2
    benchmarkCommand tar --hard-dereference --dereference --sort=name -c --transform='s|^[.]/||' -C "$tarFolder" -f "$tarFile" --owner=user --group=group .
    'rm' -rf -- "$tarFolder"
}


function benchmark()
{
    # Run and time mounting of TAR file. Do not unmount because we need it for further benchmarks!
    find . -maxdepth 1 -name "tar-with*.index.sqlite" -delete
    benchmarkCommand $cmd "${tarFile}${compression}" "$mountFolder"

    sleep 0.1s
    for (( i = 0; i < 30; ++i )); do
        if mountpoint -q "$mountFolder"; then
            break
        fi
        sleep 1s
        echoerr "Waiting for mountpoint"
    done # throw error after timeout?

    if [[ $nFolders == 1 ]]; then
        nFoldersTests=1
        nFilesTest=10
    else
        nFoldersTests=10
        nFilesTest=1
    fi
    for (( iFolder = 0; iFolder < nFoldersTests; iFolder += 1 )); do
    for (( iFile = 0; iFile < nFilesTest; )); do
        testFile=$( printf "%0${nameLength}d" "$(( iFolder * nFolders / nFoldersTests ))" )/$( printf "%0${nameLength}d" "$(( iFile * nFilesPerFolder / nFilesTest ))" )

        benchmarkCommand cat "$mountFolder/$testFile"
        catDuration=$duration  # set by benchmarkCommand!

        benchmarkCommand stat "$mountFolder/$testFile"

        # Only benchmark ~3 files if the test takes too long
        # The archivemount test with the 90GB TAR takes >2h per cat ...!
        (( ++iFile ))
        if [[ "${catDuration%.*}" -gt 600 ]]; then
            # Beware that the result of the calculated value is implicitly converted to 0 (value != 0) or 1 exit code!
            (( iFile += nFilesTest / 3 ))
        fi
    done
    done

    benchmarkCommand find "$mountFolder"
    #benchmarkCommand find "$mountFolder" -type f -exec crc32 {} \;

    fusermount -u "$mountFolder"

    echoerr
}


function printSystemInfo()
{
    cmds=(
        # System
        "uname -r -v -m -o"

        # Used compression tools
        "pigz --version"
        "lbzip2 --version"
        "zstd --version"

        # Related compression tools
        "bzip2 --version"
        "gzip --version"

        # Tools used in benchmarks
        "cat --version"
        "tar --version"
        "find --version"

        # archivemount
        "fusermount --version"
        "dpkg -l *libfuse*"
        "archivemount --version"
        "dpkg -l *libarchive*"

        # Ratarmount
        "ratarmount --version"
        "pip show ratarmount"
        "pip show indexed_bzip2"
        "pip show indexed_gzip"
        "pip show indexed_zstd"
    )

    for cmd in "${cmds[@]}"; do
        printf '==> %s <==' "$cmd"
        echo
        $cmd
        echo
    done
}


#printSystemInfo


mountFolder=$( mktemp -d )

dataFile="benchmark-archivemount-$( date +%Y-%m-%dT%H-%M ).dat"
echo '# tool command compression nameLength nFolders nFilesPerFolder nBytesPerFile duration/s peakRssMemory/kiB fileSize/B startTime' > "$dataFile"

nameLength=32
nFilesPerFolder=1000
extendedBenchmarks=1

for nFolders in 1 10 100 300 1000 2000; do
for nBytesPerFile in 0 $(( 64 * 1024 )); do
tarFile="tar-with-$nFolders-folders-with-$nFilesPerFolder-files-${nBytesPerFile}B-files.tar"
for compression in '' '.bz2' '.gz' '.xz' '.zst'; do
    echoerr ""
    echoerr "Test with tar$compression archive containing $nFolders folders with each $nFilesPerFolder files with each $nBytesPerFile bytes"

    cmd=  # clear for benchmarkCommand method because it is not relevant for these tests
    if [[ ! -f $tarFile ]]; then
        createLargeTar
    fi

    if [[ ! -f $tarFile$compression ]]; then
        case "$compression" in
            '.bz2') benchmarkCommand lbzip2 --keep "$tarFile"; ;;
            '.gz' ) benchmarkCommand pigz --keep "$tarFile"; ;;
            # Use same block sizes for zstd as for xz for a fair comparison!
            '.xz' ) benchmarkCommand xz -T 0 --block-size=$(( 1024*1024 )) --keep "$tarFile"; ;;
            '.zst') benchmarkFunction createMultiFrameZstd "$tarFile" "$(( 1024*1024 ))"; ;;
        esac
    fi

    for cmd in "ratarmount -P $( nproc )"; do
        benchmark
    done

    if [[ $extendedBenchmarks -eq 1 ]]; then
        for cmd in archivemount fuse-archive; do
            benchmark
        done

        # Benchmark decompression and listing with other tools for comparison
        benchmarkCommand tar tvlf "${tarFile}${compression}"

        case "$compression" in
            '.bz2')
                benchmarkCommand lbzip2 --force --keep --decompress --stdout "${tarFile}${compression}"
                benchmarkCommand bzip2 --force --keep --decompress --stdout "${tarFile}${compression}"
                ;;
            '.gz' )
                benchmarkCommand pigz --force --keep --decompress --stdout "${tarFile}${compression}"
                benchmarkCommand gzip --force --keep --decompress --stdout "${tarFile}${compression}"
                ;;
            '.xz' )
                benchmarkCommand xz -T 0 --block-size=$(( 1024*1024 )) --force --keep --decompress --stdout "${tarFile}${compression}"
                ;;
            '.zst')
                benchmarkCommand zstd --force --keep --decompress --stdout "${tarFile}${compression}"
                ;;
        esac

        # Benchmark single-core version of anything that is parallelized
        cmd="ratarmount -P 1"
        benchmark
    fi

    # I don't have enough free space on my SSD to keep 4x 100GB large files around
    if [[ -n "$compression" &&
          ( $( stat --format=%s -- ${tarFile}${compression} ) -gt $(( 1*1024*1024*1024 )) ) ]]
    then
        'rm' "${tarFile}${compression}"
    fi
done  # compression

find . -maxdepth 1 -name "tar-with-$nFolders-folders-with-$nFilesPerFolder-files-${nBytesPerFile}B-files.tar*" -delete

done  # nBytesPerFile
done  # nFolders

rmdir "$mountFolder"

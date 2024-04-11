#!/usr/bin/env bash

set -e

echoerr() { echo "$@" 1>&2; }


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
    local cmdToPrint commandToBenchmark

    commandToBenchmark=( "$@" )

    echoerr "Running: ${commandToBenchmark[*]} ..."

    if [[ ( "${commandToBenchmark[0]}" == 'archivemount' ) ||
          ( "${commandToBenchmark[0]}" == 'fuse-archive' ) ||
          ( "${commandToBenchmark[0]}" == 'ratarmount' ) ]]; then
        # We need to keep fuse-archive in foreground because it is hard to find the process ID
        # of the daemonized forked background process.
        commandToBenchmark=( "${commandToBenchmark[0]}" -f "${commandToBenchmark[@]:1}" )
        "${commandToBenchmark[@]}" 2>&1 1>/dev/null &
        mountPid=$!

        local mountPoint
        mountPoint=${commandToBenchmark[${#commandToBenchmark[@]} -1]}
        while ! command mountpoint -q "$mountPoint"; do sleep 0.1; done
        stat -- "$mountPoint"  &>/dev/null
    else
        duration=$( { /bin/time -f '%e s %M kiB max rss' \
                          "${commandToBenchmark[@]}"; } 2>&1 1>/dev/null |
                          'grep' 'max rss' )

        echoerr "Command took $duration"  # duration includes RSS, which is helpful to print here

        if [[ "${commandToBenchmark[0]}" == 'cat' ]]; then
            # Pretty sure the memory value is in kiB not "kB" like printed out.
            # https://unix.stackexchange.com/questions/199482/does-proc-pid-status-always-use-kb
            rss=$( sed -n -r 's|^VmHWM:[ \t]*([0-9]+) kB.*|\1|p' /proc/$mountPid/status )
        else
            rss=$( printf '%s' "$duration" | sed -r 's|.* s ([0-9]+) kiB.*|\1|' )
        fi

        if [[ -n "$cmd" ]]; then
            cmdToPrint=$cmd
        else
            cmdToPrint=${commandToBenchmark[0]}
        fi
        echo "$cmdToPrint;\"${commandToBenchmark[*]}\";tar$compression;$nBytesPerFile;${duration%% s *};$rss;\"$( date --iso-8601=seconds )\"" >> "$dataFile"
    fi
}


function waitForMountPoint()
{
    sleep 0.1s
    for (( i = 0; i < 30; ++i )); do
        if mountpoint -q "$1"; then
            break
        fi
        sleep 1s
        echoerr "Waiting for mountpoint"
    done # throw error after timeout?
}


function benchmarkMountedCat()
{
    # Run and time mounting of TAR file. Do not unmount because we need it for further benchmarks!
    find . -maxdepth 1 -name "tar-with*.index.sqlite" -delete

    echo "Create index first and load it later to avoid benchmarking mounting"
    echo "Run $cmd ${tarFile}${compression} $mountFolder"
    $cmd "${tarFile}${compression}" "$mountFolder" 2>&1 1>/dev/null
    waitForMountPoint "$mountFolder"

    fusermount -u "$mountFolder"
    sleep 0.1s

    # Load index, i.e., skip first decoding so that the ParallelXZReader cache is empty!
    benchmarkCommand $cmd "${tarFile}${compression}" "$mountFolder"
    waitForMountPoint "$mountFolder"

    benchmarkCommand cat "$mountFolder/large"
    catDuration=$duration  # set by benchmarkCommand!

    fusermount -u "$mountFolder"

    echoerr
}


extendedBenchmarks=1
thresholdReachedArchivemount=()
thresholdReachedFuseArchive=()


mountFolder=$( mktemp -d )

dataFile="benchmark-bandwidths-$( date +%Y-%m-%dT%H-%M ).dat"
echo '# tool command compression fileSize/B duration/s peakRssMemory/kiB startTime' > "$dataFile"

for (( nBytesPerFile = 4 * 1024; nBytesPerFile <= 16 * 1024 * 1024 * 1024; nBytesPerFile *= 4 )); do
for compression in '' '.bz2' '.gz' '.xz' '.zst'; do
    humanReadableSize=$( numfmt --to=iec-i --suffix=B "$nBytesPerFile" )
    tarFile="tar-with-$humanReadableSize-file.tar"

    echoerr ""
    echoerr "Test with tar$compression archive containing $humanReadableSize"

    cmd=  # clear for benchmarkCommand method because it is not relevant for these tests
    if [[ ! -f $tarFile ]]; then
        tarFolder="$( mktemp -d -p "$( pwd )" )"
        mkdir -p -- "$tarFolder"
        base64 /dev/urandom | head -c "$nBytesPerFile" > "$tarFolder/large"
        benchmarkCommand tar -c -C "$tarFolder" -f "$tarFile" --owner=user --group=group .
        'rm' -rf -- "$tarFolder"
    fi

    if [[ ! -f $tarFile$compression ]]; then
        case "$compression" in
            '.bz2') lbzip2 --keep "$tarFile"; ;;
            '.gz' ) pigz --keep "$tarFile"; ;;
            # Use same block sizes for zstd as for xz for a fair comparison!
            # And maybe even for gzip, which has spacing 16 MiB. However, gzip only has this large spacing because
            # the seek point metadata is ten thousand times larger!
            '.xz' ) xz -T 0 --block-size=$(( 1024*1024 )) --keep "$tarFile"; ;;
            '.zst') createMultiFrameZstd "$tarFile" "$(( 1024*1024 ))"; ;;
        esac
    fi

    for cmd in "ratarmount -P 1" "ratarmount -P $( nproc )"; do
        benchmarkMountedCat
    done

    if [[ $extendedBenchmarks -eq 1 ]]; then
        # archivemount has a performance bug for this, so stop benchmarking higher values after a threshold
        cmd=archivemount
        if ! printf '%s\n' "${thresholdReachedArchivemount[@]}" | grep -q '^'"$cmd$compression"'$'; then
            benchmarkMountedCat
            if [[ ${duration%%.*} -ge 10 ]]; then
                thresholdReachedArchivemount+=( $cmd$compression )
            fi
        fi

        # fuse-archive is slow for xz and bzip2, so save benchmark time by omitting these for very large files.
        cmd=fuse-archive
        if ! printf '%s\n' "${thresholdReachedFuseArchive[@]}" | grep -q '^'"$cmd$compression"'$'; then
            benchmarkMountedCat
            if [[ ${duration%%.*} -ge 20 ]]; then
                thresholdReachedFuseArchive+=( $cmd$compression )
            fi
        fi

        # Benchmark decompression and listing with other tools for comparison
        cmd=  # clear for benchmarkCommand method because it is not relevant for these tests
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
                benchmarkCommand xz --force --keep --decompress --stdout "${tarFile}${compression}"
                # xz -T 0 will not decompress in parallel!
                # https://stackoverflow.com/questions/22244962/multiprocessor-support-for-xz
                #benchmarkCommand xz -T 0 --force --keep --decompress --stdout "${tarFile}${compression}"
                benchmarkCommand pixz -k -d "${tarFile}${compression}"
                ;;
            '.zst')
                benchmarkCommand zstd --force --keep --decompress --stdout "${tarFile}${compression}"
                ;;
        esac
    fi
done
done

rmdir "$mountFolder"

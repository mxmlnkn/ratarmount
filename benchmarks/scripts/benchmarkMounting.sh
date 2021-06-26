#!/usr/bin/env bash

# E.g., call with:
# fname=benchmark-archivemount-$( date +%Y-%m-%dT%H-%M )
# rm tar-with*; bash benchmarkMounting.sh 2>"$fname.err" | tee "$fname.out"

#set -x
set -e

echoerr() { echo "$@" 1>&2; }


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
    if [[ -d /dev/shm ]]; then frameFile=$( mktemp --tmpdir=/dev/shm ); fi
    if [[ -z $frameFile ]]; then frameFile=$( mktemp ); fi
    if [[ -z $frameFile ]]; then
        echo "Could not create a temporary file for the frames." 1>&2
        return 1
    fi

    echo "Compress into $file.zst" 1>&2
    true > "$file.zst"
    for (( offset = 0; offset < fileSize; offset += frameSize )); do
        dd if="$file" of="$frameFile" bs=$(( 1024*1024 )) \
           iflag=skip_bytes,count_bytes skip="$offset" count="$frameSize" 2>/dev/null
        zstd -c -q -- "$frameFile" >> "$file.zst"
    done

    'rm' -f -- "$frameFile"
)


function benchmarkCommand()
{
    local commandToBenchmark=( "$@" )

    echoerr "Running: ${commandToBenchmark[*]} ..."

    duration=$( { /bin/time -f '%e s %M kiB max rss' \
                      "${commandToBenchmark[@]}"; } 2>&1 1>/dev/null |
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
    benchmarkCommand tar --hard-dereference --dereference -c -C "$tarFolder" -f "$tarFile" --owner=user --group=group .
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

for nFolders in 1 10 100 300 1000 2000; do
for nBytesPerFile in 0 $(( 64 * 1024 )); do
for compression in '' '.gz' '.bz2' '.zst'; do
    tarFile="tar-with-$nFolders-folders-with-$nFilesPerFolder-files-${nBytesPerFile}B-files.tar"

    echoerr ""
    echoerr "Test with tar$compression archive containing $nFolders folders with each $nFilesPerFolder files with each $nBytesPerFile bytes"

    cmd=  # clear for benchmarCommand method because it is not relevant for these tests
    if [[ ! -f $tarFile ]]; then
        createLargeTar
    fi

    if [[ ! -f $tarFile.$compression ]]; then
        case "$compression" in
            '.bz2') benchmarkCommand lbzip2 --keep "$tarFile"; ;;
            '.gz' ) benchmarkCommand pigz --keep "$tarFile"; ;;
            '.zst') benchmarkFunction createMultiFrameZstd "$(( 1024 * 1024 ))" "$tarFile"; ;;
        esac
    fi

    # Benchmark decompression and listing with other tools for comparison
    benchmarkCommand tar tvlf "${tarFile}${compression}"

    case "$compression" in
        '.bz2')
            benchmarkCommand lbzip2 --keep --decompress --stdout "${tarFile}${compression}"
            benchmarkCommand bzip2 --keep --decompress --stdout "${tarFile}${compression}"
            ;;
        '.gz' )
            benchmarkCommand pigz --keep --decompress --stdout "${tarFile}${compression}"
            benchmarkCommand gzip --keep --decompress --stdout "${tarFile}${compression}"
            ;;
        '.zst')
            benchmarkCommand zstd --keep --decompress --stdout "${tarFile}${compression}"
            ;;
    esac

    for cmd in archivemount "ratarmount -P $( nproc )"; do
        benchmark
    done

    if [[ "$compression" == '.bz2' ]]; then
        cmd=ratarmount
        benchmark
    fi

    # I don't have enough free space on my SSD to keep 4x 100GB large files around
    if [[ -n "$compression" &&
          ( $( stat --format=%s -- ${tarFile}${compression} ) -gt $(( 100*1024*1024*1024 )) ) ]]
    then
        'rm' "${tarFile}${compression}"
    fi
done
done
done

rmdir "$mountFolder"

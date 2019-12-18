#!/usr/bin/env bash

#set -x
set -e

echoerr() { echo "$@" 1>&2; }


createLargeTar()
(
    nameLength=$1
    nFilesPerFolder=$2
    nFolders=$3
    nBytesPerFile=$4

    # creates a TAR with many files with long names making file names out to be the most memory consuming
    # part of the metadata required for the TAR index
    if [[ ! "$nFolders" -eq "$nFolders" ]]; then
        echoerr "Argument 1 must be number to specify the number of folders containing each 1k files but is: $nFolders"
        return 1
    fi

    echoerr "Creating a tar with $(( nFolders * nFilesPerFolder )) files..."
    tarFolder="$( mktemp -d )"

    for (( iFolder = 0; iFolder < nFolders; ++iFolder )); do
        subFolder="$tarFolder/$( printf "%0${nameLength}d" "$iFolder" )"
        mkdir -p -- "$subFolder"
        for (( iFile = 0; iFile < nFilesPerFolder; ++iFile )); do
            base64 /dev/urandom | head -c $nBytesPerFile > "$subFolder/$( printf "%0${nameLength}d" "$iFile" )"
        done
    done

    tarFile="tar-with-$nFolders-folders-with-$nFilesPerFolder-files-${nBytesPerFile}B-files.tar"
    echoerr tar -c -C "$tarFolder" -f "$tarFile" --owner=user --group=group .
    tar -c -C "$tarFolder" -f "$tarFile" --owner=user --group=group .
    'rm' -rf -- "$tarFolder"
)

function benchmark()
{
    echoerr ""

    tarFile="tar-with-$nFolders-folders-with-$nFilesPerFolder-files-${nBytesPerFile}B-files.tar"

    if [[ ! -f $tarFile ]]; then
        echoerr "Creating $tarFile ..."
        { time createLargeTar $nameLength $nFilesPerFolder $nFolders $nBytesPerFile; } | sed -n -r 's|real[ \t]+||p'
    fi

    if [[ ! -f $tarFile.bz2 ]]; then
        bzip2 -k "$tarFile"
    fi

    if [[ ! -f $tarFile.gz ]]; then
        gzip -k "$tarFile"
    fi

    if [[ $compression == .bz2 ]]; then
        duration=$( { TIMEFORMAT=%3R; time bzcat $tarFile$compression; } 2>&1 1>/dev/null )
        echoerr "bzcat $tarFile$compression took $duration"
        echo "\"bzcat\";tar$compression;$nameLength;$nFolders;$nFilesPerFolder;$nBytesPerFile;$duration;0" >> $dataFile
    fi

    find . -maxdepth 1 -name "tar-with*.index.sqlite" -delete
    duration=$( { /bin/time -f '%e s %M kiB max rss' $cmd "$tarFile$compression" "$mountFolder"; } 2>&1 | 'grep' 'max rss' )
    echoerr "Mounting tar$compression archive containing $nFolders folders with each $nFilesPerFolder files with each $nBytesPerFile bytes with $cmd took $duration"
    rss=$( echo "$duration" | sed -r 's|.* s ([0-9]+) kiB.*|\1|' )
    echo "\"$cmd\";tar$compression;$nameLength;$nFolders;$nFilesPerFolder;$nBytesPerFile;${duration%% s *};$rss" >> $dataFile

    sleep 0.1s
    for (( i = 0; i < 30; ++i )); do
        if mountpoint -q "$mountFolder"; then
            break
        fi
        sleep 1s
        echoerr "Waiting for mountpoint"
    done # throw error after timeout?

    timeoutDuration='30m'

    if [[ $nFolders == 1 ]]; then
        nFoldersTests=1
        nFilesTest=10
    else
        nFoldersTests=10
        nFilesTest=1
    fi
    for (( iFolder = 0; iFolder < nFoldersTests; iFolder += 1 )); do
    for (( iFile = 0; iFile < nFilesTest; iFile += 1 )); do
        testFile=$( printf "%0${nameLength}d" "$(( iFolder * nFolders / nFoldersTests ))" )/$( printf "%0${nameLength}d" "$(( iFile * nFilesPerFolder / nFilesTest ))" )

        duration=$( { TIMEFORMAT=%3R; time cat -- "$mountFolder/$testFile"; } 2>&1 1>/dev/null )
        echoerr "Cat $testFile took $duration"
        echo "\"cat $testFile\";tar$compression;$nameLength;$nFolders;$nFilesPerFolder;$nBytesPerFile;$duration;0" >> $dataFile

        duration=$( { TIMEFORMAT=%3R; time stat -- "$mountFolder/$testFile"; } 2>&1 1>/dev/null )
        echoerr "Stat $testFile took $duration"
        echo "\"stat $testFile\";tar$compression;$nameLength;$nFolders;$nFilesPerFolder;$nBytesPerFile;$duration;0" >> $dataFile
    done
    done

    duration=$( { TIMEFORMAT=%3R; time find "$mountFolder" | wc -l; } 2>&1 1>/dev/null )
    echoerr "Find took $duration"
    echo "\"find\";tar$compression;$nameLength;$nFolders;$nFilesPerFolder;$nBytesPerFile;$duration;0" >> $dataFile

    #duration=$( { /bin/time -f '%e s' timeout "$timeoutDuration" find "$mountFolder" -type f -exec crc32 {} \; | wc -l; } 2>&1 1>/dev/null )
    #echoerr "CRC32 took $duration (timeout $timeoutDuration)"

    fusermount -u "$mountFolder"
}

mountFolder=$( mktemp -d )

dataFile="benchmark-archivemount-$( date +%Y-%m-%dT%H-%M ).dat"
echo '# description compression nameLength nFolders nFilesPerFolder nBytesPerFile duration/s peakRssMemory/kiB' > $dataFile

nameLength=32
nFilesPerFolder=1000
for nFolders in 1 10 100 1000 2000; do
for nBytesPerFile in 0 64 4096; do
for compression in '' .gz .bz2; do
for cmd in archivemount ratarmount; do
    benchmark
done
done
done
done

rmdir "$mountFolder"

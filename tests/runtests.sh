#!/bin/bash

cd -- "$( dirname -- "${BASH_SOURCE[0]}" )/.." || { echo 'Failed to cd to ratarmount.py folder!'; exit 1; }

if [[ -z "$RATARMOUNT_CMD" ]]; then
    RATARMOUNT_CMD="python3 -u $( realpath -- ratarmount.py )"
    #RATARMOUNT_CMD=ratarmount
    export RATARMOUNT_CMD
fi

TMP_FILES_TO_CLEANUP=()
MOUNT_POINTS_TO_CLEANUP=()
cleanup()
{
    sleep 0.5s # Give a bit of time for the mount points to become stable before trying to unmount them
    for folder in "${MOUNT_POINTS_TO_CLEANUP[@]}"; do
        if [[ -d "$folder" ]] && mountpoint -- "$folder" &>/dev/null; then
            fusermount -u -- "$folder"
        fi
    done
    sleep 0.5s
    for folder in "${MOUNT_POINTS_TO_CLEANUP[@]}"; do
        if [[ -d "$folder" ]]; then rmdir -- "$folder"; fi
    done
    MOUNT_POINTS_TO_CLEANUP=()

    for file in "${TMP_FILES_TO_CLEANUP[@]}"; do
        if [ -d "$file" ]; then rmdir -- "$file"; fi
        if [ -f "$file" ]; then rm -- "$file"; fi
    done
    TMP_FILES_TO_CLEANUP=()
}

trap 'cleanup' EXIT

echoerr() { echo "$@" 1>&2; }

createMultiFrameZstd()
(
    # Detect being piped into
    if [ -t 0 ]; then
        file=$1
        frameSize=$2
        if [[ ! -f "$file" ]]; then echo "Could not find file '$file'." 1>&2; return 1; fi
        fileSize=$( stat -c %s -- "$file" )
    else
        if [ -t 1 ]; then echo 'You should pipe the output to somewhere!' 1>&2; return 1; fi
        #echo 'Will compress from stdin...' 1>&2
        frameSize=$1
    fi
    if [[ ! $frameSize =~ ^[0-9]+$ ]]; then
        echo "Frame size '$frameSize' is not a valid number." 1>&2
        return 1
    fi

    # Create a temporary file. I avoid simply piping to zstd
    # because it wouldn't store the uncompressed size.
    if [[ -d --tmpdir=/dev/shm ]]; then frameFile=$( mktemp --tmpdir=/dev/shm ); fi
    if [[ -z $frameFile ]]; then frameFile=$( mktemp ); fi
    if [[ -z $frameFile ]]; then
        echo "Could not create a temporary file for the frames." 1>&2
        return 1
    fi

    if [ -t 0 ]; then
        true > "$file.zst"
        for (( offset = 0; offset < fileSize; offset += frameSize )); do
            dd if="$file" of="$frameFile" bs=$(( 1024*1024 )) \
               iflag=skip_bytes,count_bytes skip="$offset" count="$frameSize" 2>/dev/null
            zstd -c -q -- "$frameFile" >> "$file.zst"
        done
    else
        while true; do
            dd of="$frameFile" bs=$(( 1024*1024 )) \
               iflag=count_bytes count="$frameSize" 2>/dev/null
            # pipe is finished when reading it yields no further data
            if [[ ! -s "$frameFile" ]]; then break; fi
            zstd -c -q -- "$frameFile"
        done
    fi

    'rm' -f -- "$frameFile"
)

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
    sleep 0.2s
    while mountpoint "$mountFolder" &>/dev/null; do
        sleep 0.2s
        fusermount -u "$mountFolder"
    done
}

returnError()
{
    local lineNumber message
    if [ $# -eq 2 ]; then
        lineNumber=:$1
        message=$2
    else
        message=$*
    fi

    echoerr -e "\e[37m${FUNCNAME[1]}$lineNumber <- ${FUNCNAME[*]:2}\e[0m"
    echoerr -e "\e[37m$message\e[0m"
    echoerr -e '\e[31mTEST FAILED!\e[0m'

    echo "==> ratarmount.stdout.log <=="
    cat ratarmount.stdout.log
    echo
    echo "==> ratarmount.stderr.log <=="
    cat ratarmount.stderr.log

    exit 1
}

runAndCheckRatarmount()
{
    MOUNT_POINTS_TO_CLEANUP+=( "${*: -1}" )
    $RATARMOUNT_CMD "$@" >ratarmount.stdout.log 2>ratarmount.stderr.log &&
    checkStat "${@: -1}" # mount folder must exist and be stat-able after mounting
    ! 'grep' -C 5 -Ei '(warn|error)' ratarmount.stdout.log ratarmount.stderr.log ||
        returnError "$LINENO" "Found warnings while executing: $RATARMOUNT_CMD $*"
}

checkFileInTAR()
{
    local archive="$1"; shift
    local fileInTar="$1"; shift
    local correctChecksum="$1"

    local startTime
    startTime=$( date +%s )

    rm -f ratarmount.{stdout,stderr}.log

    local mountFolder
    mountFolder="$( mktemp -d )" || returnError "$LINENO" 'Failed to create temporary directory'
    MOUNT_POINTS_TO_CLEANUP+=( "$mountFolder" )

    # try with index recreation
    local args=( -P "$parallelization" -c --ignore-zeros --recursive "$archive" "$mountFolder" )
    {
        runAndCheckRatarmount "${args[@]}" &&
        checkStat "$mountFolder/$fileInTar" &&
        verifyCheckSum "$mountFolder" "$fileInTar" "$archive" "$correctChecksum"
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"
    funmount "$mountFolder"
    'grep' -q 'Creating offset dictionary' ratarmount.stdout.log ratarmount.stderr.log ||
        returnError "$LINENO" "Looks like index was not created while executing: $RATARMOUNT_CMD $*"

    # retry without forcing index recreation
    local args=( -P "$parallelization" --ignore-zeros --recursive "$archive" "$mountFolder" )
    {
        runAndCheckRatarmount "${args[@]}" &&
        checkStat "$mountFolder/$fileInTar" &&
        verifyCheckSum "$mountFolder" "$fileInTar" "$archive" "$correctChecksum"
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"
    funmount "$mountFolder"
    'grep' -q 'Loading offset dictionary' ratarmount.stdout.log ratarmount.stderr.log ||
        returnError "$LINENO" "Looks like index was not loaded while executing: $RATARMOUNT_CMD $*"

    rmdir "$mountFolder"

    local duration
    duration=$(( $( date +%s ) - startTime ))
    echoerr "Tested successfully '$fileInTar' in '$archive' for checksum $correctChecksum in ${duration}s"

    return 0
}

checkFileInTARPrefix()
{
    local prefix="$1"; shift
    local archive="$1"; shift
    local fileInTar="$1"; shift
    local correctChecksum="$1"

    rm -f ratarmount.{stdout,stderr}.log

    local mountFolder
    mountFolder="$( mktemp -d )" || returnError "$LINENO" 'Failed to create temporary directory'
    MOUNT_POINTS_TO_CLEANUP+=( "$mountFolder" )

    # try with index recreation
    local args=( -P "$parallelization" -c --ignore-zeros --recursive --prefix "$prefix" "$archive" "$mountFolder" )
    {
        runAndCheckRatarmount "${args[@]}" &&
        checkStat "$mountFolder/$fileInTar" &&
        verifyCheckSum "$mountFolder" "$fileInTar" "$archive" "$correctChecksum"
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"
    funmount "$mountFolder"

    rmdir "$mountFolder"

    echoerr "[${FUNCNAME[0]}] Tested successfully '$fileInTar' in '$archive' for checksum $correctChecksum"

    return 0
}

checkLinkInTAR()
{
    local archive="$1"; shift
    local fileInTar="$1"; shift
    local correctLinkTarget="$1"

    rm -f ratarmount.{stdout,stderr}.log

    local mountFolder
    mountFolder="$( mktemp -d )" || returnError "$LINENO" 'Failed to create temporary directory'
    MOUNT_POINTS_TO_CLEANUP+=( "$mountFolder" )

    # try with index recreation
    local args=( -P "$parallelization" -c --recursive "$archive" "$mountFolder" )
    {
        runAndCheckRatarmount "${args[@]}" &&
        checkStat "$mountFolder/$fileInTar"
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"
    if [[ $( readlink -- "$mountFolder/$fileInTar" ) != "$correctLinkTarget" ]]; then
        echoerr -e "\e[37mLink target of '$fileInTar' in mounted TAR '$archive' does not match"'!\e[0m'
        returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"
    fi
    funmount "$mountFolder"

    rmdir "$mountFolder"

    echoerr "[${FUNCNAME[0]}] Tested successfully '$fileInTar' in '$archive' for link target $correctLinkTarget"

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
    fileNameDataSizeInMB=$1
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

    largeTarFolder="$( mktemp -d )" || return 1

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
    mountFolder="$( mktemp -d )" || returnError "$LINENO" 'Failed to create temporary directory'
    MOUNT_POINTS_TO_CLEANUP+=( "$mountFolder" )

    # benchmark creating the index

    $RATARMOUNT_CMD -P "$parallelization" -c -f --recursive "$largeTar" "$mountFolder" &
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

    $RATARMOUNT_CMD -P "$parallelization" -f --recursive "$largeTar" "$mountFolder" &
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

    local mib
    for mib in 1 8 64 256; do
        echoerr "Benchmarking ${mib}MiB TAR metadata ..."

        printf '%i ' "$mib" >> "$logFile"

        testLargeTar "$mib" | sed -n -r '
            s|Creating offset dictionary for /[^:]*.tar took ([0-9.]+)s|\1|p;
            s|Writing out TAR.* took ([0-9.]+)s and is sized ([0-9]+) B|\1 \2|p;
            s|Loading offset dictionary.* took ([0-9.]+)s|\1|p;
        ' | sed -z 's|\n| |g' >> "$logFile"

        # not nice but hard to do differently as the pipe opens testLargeTar in a subshell and tee
        # redirects it directly to tty, so we can't store an output!
        local timeSeriesFile="benchmark-memory-${mib}-MiB-saving.dat"
        printf '%s ' "$( getPeakMemoryFromFile "$timeSeriesFile" )" >> "$logFile"
        'mv' "$timeSeriesFile" "$benchmarksFolder/$timeSeriesFile"

        local timeSeriesFile="benchmark-memory-${mib}-MiB-loading.dat"
        printf '%s ' "$( getPeakMemoryFromFile "$timeSeriesFile" )" >> "$logFile"
        'mv' "$timeSeriesFile" "$benchmarksFolder/$timeSeriesFile"

        echo >> "$logFile"
    done
}

checkAutomaticIndexRecreation()
(
    rm -f ratarmount.{stdout,stderr}.log

    tmpFolder="$( mktemp -d )" || returnError "$LINENO" 'Failed to create temporary directory'
    cd -- "$tmpFolder" || returnError "$LINENO" 'Failed to cd into temporary directory'

    archive='momo.tar'
    mountFolder='momo'

    # 1. Create a simple TAR
    fileName='meme'
    echo 'mimi' > "$fileName"
    tar -cf "$archive" "$fileName"

    # 1. Check and create index
    $RATARMOUNT_CMD "$archive" >ratarmount.stdout.log 2>ratarmount.stderr.log
    ! 'grep' -Eqi '(warn|error)' ratarmount.stdout.log ratarmount.stderr.log ||
        returnError "$LINENO" "Found warnings while executing: $RATARMOUNT_CMD $archive"
    diff -- "$fileName" "$mountFolder/$fileName" || returnError "$LINENO" 'Files differ on simple mount!'
    funmount "$mountFolder"

    # 2. Check that index does not get recreated normally
    sleep 1 # because we are comparing timestamps with seconds precision ...
    indexFile='momo.tar.index.sqlite'
    [[ -f $indexFile ]] || returnError "$LINENO" 'Index file not found!'
    lastModification=$( stat -c %Y -- "$indexFile" )
    $RATARMOUNT_CMD "$archive" >ratarmount.stdout.log 2>ratarmount.stderr.log
    ! 'grep' -Eqi '(warn|error)' ratarmount.stdout.log ratarmount.stderr.log ||
        returnError "$LINENO" "Found warnings while executing: $RATARMOUNT_CMD $archive"
    diff -- "$fileName" "$mountFolder/$fileName" || returnError "$LINENO" 'Files differ on simple remount!'
    funmount "$mountFolder"
    [[ $lastModification -eq $( stat -c %Y -- "$indexFile" ) ]] ||
        returnError "$LINENO" 'Index changed even though TAR did not!'

    # 3. Change contents (and timestamp) without changing the size
    #    (Luckily TAR is filled to 10240 Bytes anyways for very small files)
    sleep 1 # because we are comparing timestamps with seconds precision ...
    fileName="${fileName//e/a}"
    echo 'momo' > "$fileName"
    tar -cf "$archive" "$fileName"

    # modification timestamp detection is turned off for now by default to facilitate index sharing because
    # the mtime check can proove problematic as the mtime changes when downloading a file.
    $RATARMOUNT_CMD "$archive" >ratarmount.stdout.log 2>ratarmount.stderr.log
    ! 'grep' -Eqi '(warn|error)' ratarmount.stdout.log ratarmount.stderr.log ||
        returnError "$LINENO" "Found warnings while executing: $RATARMOUNT_CMD $archive"
    ! [[ -f "$mountFolder/${fileName}" ]] ||
        returnError "$LINENO" 'Index should not have been recreated and therefore contain outdated file name!'
    funmount "$mountFolder"

    $RATARMOUNT_CMD --verify-mtime "$archive" >ratarmount.stdout.log 2>ratarmount.stderr.log
    'grep' -Eqi 'warn' ratarmount.stdout.log ratarmount.stderr.log ||
        returnError "$LINENO" "Found no warnings while executing: $RATARMOUNT_CMD --verify-mtime $archive"
    diff -- "$fileName" "$mountFolder/${fileName}" ||
        returnError "$LINENO" 'Files differ when trying to trigger index recreation!'
    funmount "$mountFolder"

    [[ $lastModification -ne $( stat -c %Y -- "$indexFile" ) ]] || \
        returnError "$LINENO" 'Index did not change even though TAR did!'
    lastModification=$( stat -c %Y -- "$indexFile" )

    # 4. Check that index changes if size changes but modification timestamp does not
    sleep 1 # because we are comparing timestamps with seconds precision ...
    fileName="heho"
    head -c $(( 100 * 1024 )) /dev/urandom > "$fileName"
    tar -cf "$archive" "$fileName"
    touch -d "@$lastModification" "$archive"

    $RATARMOUNT_CMD "$archive" >ratarmount.stdout.log 2>ratarmount.stderr.log
    'grep' -Eqi 'warn' ratarmount.stdout.log ratarmount.stderr.log ||
        returnError "$LINENO" "Found no warnings while executing: $RATARMOUNT_CMD $archive"
    diff -- "$fileName" "$mountFolder/${fileName}" || returnError "$LINENO" 'Files differ!'
    funmount "$mountFolder"
    [[ $lastModification -ne $( stat -c %Y -- "$indexFile" ) ]] || \
        returnError "$LINENO" 'Index did not change even though TAR filesize did!'

    cd .. || returnError "$LINENO" 'Could not cd to parent in order to clean up!'
    rm -rf -- "$tmpFolder"

    echoerr "[${FUNCNAME[0]}] Tested successfully"
)

checkUnionMount()
(
    rm -f ratarmount.{stdout,stderr}.log

    testsFolder="$( pwd )/tests"
    tmpFolder="$( mktemp -d )" || returnError "$LINENO" 'Failed to create temporary directory'
    cd -- "$tmpFolder" || returnError "$LINENO" 'Failed to cd into temporary directory'
    keyString='EXTRACTED VERSION'

    tarFiles=( 'hardlink' 'nested-symlinks' 'single-nested-file' 'symlinks' )

    for tarFile in "${tarFiles[@]}"; do
    (
        mkdir "$tarFile" &&
        cd -- "$_" &&
        tar -xf "$testsFolder/$tarFile.tar" 2>/dev/null &&
        find . -type f -execdir bash -c 'echo "$1" >> "$0"' {} "$keyString" \;
    )
    done

    mountPoint=$( mktemp -d )
    for tarFile in "${tarFiles[@]}"; do
        # Check whether a simple bind mount works, which is now an officially supported perversion of ratarmount
        runAndCheckRatarmount -c "$tarFile" "$mountPoint"
        diff -r --no-dereference "$tarFile" "$mountPoint" || returnError "$LINENO" 'Bind mounted folder differs!'
        funmount "$mountPoint"

        # Check that bind mount onto the mount point works
        runAndCheckRatarmount -c "$tarFile" "$tarFile"
        [[ $( find "$tarFile" -mindepth 1 | wc -l ) -gt 0 ]] || returnError "$LINENO" 'Bind mounted folder is empty!'
        funmount "$tarFile"

        # Check whether updating a folder with a TAR works
        runAndCheckRatarmount -c "$tarFile" "$testsFolder/$tarFile.tar" "$mountPoint"
        keyContainingFiles=$( find "$mountPoint" -type f -execdir bash -c '
            if command grep -q "$1" "$0"; then printf "%s\n" "$0"; fi' {} "$keyString" \; | wc -l )
        [[ $keyContainingFiles -eq 0 ]] ||
            returnError "$LINENO" 'Found file from updated folder even though all files are updated!'
        funmount "$mountPoint"

        # Check whether updating a TAR with a folder works
        runAndCheckRatarmount -c "$testsFolder/$tarFile.tar" "$tarFile" "$mountPoint"
        keyNotContainingFiles=$( find "$mountPoint" -type f -execdir bash -c '
            if ! command grep -q "$1" "$0"; then printf "%s\n" "$0"; fi' {} "$keyString" \; | wc -l )
        [[ $keyNotContainingFiles -eq 0 ]] ||
            returnError "$LINENO" 'Found files from TAR even though it was updated with a folder!'
        funmount "$mountPoint"
    done

    rmdir -- "$mountPoint"
    cd .. || returnError "$LINENO" 'Could not cd to parent in order to clean up!'
    rm -rf -- "$tmpFolder" || returnError "$LINENO" 'Something went wrong. Should have been able to clean up!'

    echoerr "[${FUNCNAME[0]}] Tested successfully"
)

checkUnionMountFileVersions()
(
    rm -f ratarmount.{stdout,stderr}.log

    testsFolder="$( pwd )/tests"
    tmpFolder="$( mktemp -d )" || returnError "$LINENO" 'Failed to create temporary directory'
    cd -- "$tmpFolder" || returnError "$LINENO" 'Failed to cd into temporary directory'

    tarFiles=( 'updated-file.tar' )

    mkdir -p folder/foo/fighter
    echo 'untarred' > folder/foo/fighter/ufo
    mkdir emptyFolder

    runAndCheckRatarmount -c emptyFolder folder "$testsFolder/updated-file.tar" emptyFolder folder mountPoint

    untarredFileMd5=$( md5sum folder/foo/fighter/ufo 2>/dev/null | sed 's| .*||' )
    verifyCheckSum mountPoint foo/fighter/ufo updated-file.tar "$untarredFileMd5" \
        || returnError "$LINENO" "File check failed"
    verifyCheckSum mountPoint foo/fighter/ufo.versions/1 "$( pwd )" "$untarredFileMd5" \
        || returnError "$LINENO" "File check failed"
    verifyCheckSum mountPoint foo/fighter/ufo.versions/2 "$( pwd )" 2709a3348eb2c52302a7606ecf5860bc \
        || returnError "$LINENO" "File check failed"
    verifyCheckSum mountPoint foo/fighter/ufo.versions/3 "$( pwd )" 9a12be5ebb21d497bd1024d159f2cc5f \
        || returnError "$LINENO" "File check failed"
    verifyCheckSum mountPoint foo/fighter/ufo.versions/4 "$( pwd )" b3de7534cbc8b8a7270c996235d0c2da \
        || returnError "$LINENO" "File check failed"
    verifyCheckSum mountPoint foo/fighter/ufo.versions/5 "$( pwd )" "$untarredFileMd5" \
        || returnError "$LINENO" "File check failed"

    funmount mountPoint
    cd .. || returnError "$LINENO" 'Could not cd to parent in order to clean up!'
    rm -rf -- "$tmpFolder" || returnError "$LINENO" 'Something went wrong. Should have been able to clean up!'

    echoerr "[${FUNCNAME[0]}] Tested successfully"
)

checkAutoMountPointCreation()
(
    rm -f ratarmount.{stdout,stderr}.log

    testsFolder="$( pwd )/tests"
    tmpFolder="$( mktemp -d )" || returnError "$LINENO" 'Failed to create temporary directory'
    cd -- "$tmpFolder" || returnError "$LINENO" 'Failed to cd into temporary directory'

    cp "$testsFolder/single-nested-file.tar" .
    $RATARMOUNT_CMD -- *.tar >ratarmount.stdout.log 2>ratarmount.stderr.log
    ! 'grep' -Eqi '(warn|error)' ratarmount.stdout.log ratarmount.stderr.log ||
        returnError "$LINENO" "Found warnings while executing: $RATARMOUNT_CMD -- *.tar"
    command grep -q 'iriya' single-nested-file/foo/fighter/ufo ||
    returnError "$LINENO" 'Check for auto mount point creation failed!'

    funmount 'single-nested-file'
    sleep 1s
    [[ ! -d 'single-nested-file' ]] ||
        returnError "$LINENO" 'Automatically created mount point was not removed after unmount!'

    cd .. || returnError "$LINENO" 'Could not cd to parent in order to clean up!'
    rm -rf -- "$tmpFolder" || returnError "$LINENO" 'Something went wrong. Should have been able to clean up!'

    echoerr "[${FUNCNAME[0]}] Tested successfully"
)


checkTarEncoding()
{
    local archive="$1"; shift
    local encoding="$1"; shift
    local fileInTar="$1"; shift
    local correctChecksum="$1"

    rm -f ratarmount.{stdout,stderr}.log

    local mountFolder
    mountFolder="$( mktemp -d )" || returnError "$LINENO" 'Failed to create temporary directory'
    MOUNT_POINTS_TO_CLEANUP+=( "$mountFolder" )

    # try with index recreation
    local args=( -P "$parallelization" -c --encoding "$encoding" --recursive "$archive" "$mountFolder" )
    {
        runAndCheckRatarmount "${args[@]}" &&
        checkStat "$mountFolder/$fileInTar" &&
        verifyCheckSum "$mountFolder" "$fileInTar" "$archive" "$correctChecksum"
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"
    funmount "$mountFolder"

    rmdir "$mountFolder"

    echoerr "[${FUNCNAME[0]}] Tested successfully '$fileInTar' in '$archive' for encoding $encoding"

    return 0
}


recompressFile()
{
    # Given a file it returns paths to all variants of (uncompressed, bz2, gzip, xz, zst).

    rm -f ratarmount.{stdout,stderr}.log

    local recompressedFiles=()
    local tmpFolder
    tmpFolder=$( mktemp -d )

    local file=$1
    cp -- "$file" "$tmpFolder"
    file="$tmpFolder/$( basename -- "$file" )"

    local uncompressedFile=
    uncompressedFile=${file%.*}
    [[ "$uncompressedFile" != "$file" ]] || returnError "$LINENO" 'Given file seems to have no extension!'

    # 1. Extract if necessary
    local fileCompression
    # Deleting all i,p,d is a fun trick to get the shorthand suffixes from the longhand compression names!
    fileCompression=$( file --mime-type -- "$file" | sed 's|.*[/-]||; s|[ipd]||g' )
    case "$fileCompression" in
        bz2)
            bzip2 --keep --stdout --decompress -- "$file" > "$uncompressedFile"
            ;;
        gz)
            gzip --keep --stdout --decompress -- "$file" > "$uncompressedFile"
            ;;
        xz)
            pixz -d "$file" "$uncompressedFile"
            ;;
        zst)
            zstd --keep --stdout --decompress -- "$file" > "$uncompressedFile"
            ;;
        *)
            uncompressedFile=$file
            ;;
    esac

    # 2. Compress into all supported formats
    for compression in bz2 gz xz zst; do
        if [[ "$compression" == "$fileCompression" ]]; then
            recompressedFiles+=( "$file" )
            continue
        fi

        recompressedFile="$tmpFolder/$( basename -- "$uncompressedFile" ).$compression"
        recompressedFiles+=( "$recompressedFile" )

        case "$compression" in
            bz2)
                bzip2 --keep --stdout "$uncompressedFile" > "$recompressedFile"
                ;;
            gz)
                gzip --keep --stdout "$uncompressedFile" > "$recompressedFile"
                ;;
            xz)
                pixz "$uncompressedFile" "$recompressedFile"
                ;;
            zst)
                # Use block size < 10kiB in order to get multiframe zst files for all test TARs no matter how small
                createMultiFrameZstd $(( 8*1024 )) < "$uncompressedFile" > "$recompressedFile"
                ;;
        esac

        [ -s "$recompressedFile" ] ||
            returnError "$LINENO" "Something went wrong during ${compression} compression of ${uncompressedFile} into ${recompressedFile}."
    done

    if [[ "$( file --mime-type -- "$uncompressedFile" )" =~ tar$ ]]; then
        printf '%s\n' "$uncompressedFile"
    else
        # Do not return non-TAR uncompressed files and cleanup if they were created by us.
        rm -- "$uncompressedFile"
    fi

    printf '%s\n'  "${recompressedFiles[@]}"
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
                python3 -m timeit 'from indexed_bzip2 import IndexedBzip2File as IBF; IBF( '"'$file'"' ).read();'
                ;;
            gzip)
                python3 -m timeit 'from indexed_gzip import IndexedGzipFile as IBF; IBF( '"'$file'"' ).read();'
                ;;
            xz)
                python3 -m timeit 'import lzmaffi; lzmaffi.open( '"'$file'"' ).read();'
                ;;
            zstd)
                python3 -m timeit 'from indexed_zstd import IndexedZstdFile as IBF; IBF( '"'$file'"' ).read();'
                ;;
        esac
    done < <( recompressFile 'tests/2k-recursive-tars.tar.bz2' )

    cleanup
    rmdir -- "$tmpFolder"
}


checkIndexPathOption()
{
    # The --index-path should have highest priority, overwriting all --index-folders and default locations
    local archive="$1"; shift
    local fileInTar="$1"; shift
    local correctChecksum="$1"

    rm -f ratarmount.{stdout,stderr}.log

    local mountFolder indexFolder indexFile
    mountFolder="$( mktemp -d )" || returnError "$LINENO" 'Failed to create temporary directory'
    indexFolder="$( mktemp -d )" || returnError "$LINENO" 'Failed to create temporary directory'
    indexFile="$indexFolder/ratarmount.index"
    MOUNT_POINTS_TO_CLEANUP+=( "$mountFolder" )
    TMP_FILES_TO_CLEANUP+=( "$indexFile" "$indexFolder" )

    # Check that index gets created at the specified location
    [ ! -f "$indexFile" ] || returnError "$LINENO" 'Index should not exist before test!'
    local args=( --index-file "$indexFile" "$archive" "$mountFolder" )
    {
        runAndCheckRatarmount "${args[@]}" &&
        checkStat "$mountFolder/$fileInTar" &&
        verifyCheckSum "$mountFolder" "$fileInTar" "$archive" "$correctChecksum"
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"
    funmount "$mountFolder"
    [ -s "$indexFile" ] || returnError "$LINENO" 'Index was not created!'
    cleanup

    echoerr "[${FUNCNAME[0]}] Tested successfully '$fileInTar' in '$archive'"

    return 0
}


checkIndexFolderFallback()
{
    # The --index-folders should overwrite the default index locations and also give fallbacks in order
    local archive="$1"; shift
    local fileInTar="$1"; shift
    local correctChecksum="$1"

    rm -f ratarmount.{stdout,stderr}.log

    local args mountFolder indexFolder
    mountFolder="$( mktemp -d )" || returnError "$LINENO" 'Failed to create temporary directory'
    indexFolder="$( mktemp -d )" || returnError "$LINENO" 'Failed to create temporary directory'
    MOUNT_POINTS_TO_CLEANUP+=( "$mountFolder" )

    # Check that index gets created in the specified folder
    [ -z "$( find "$indexFolder" -type f -size +0c )" ] || returnError "$LINENO" 'Index should not exist before test!'
    args=( --index-folders "$indexFolder" "$archive" "$mountFolder" )
    {
        runAndCheckRatarmount "${args[@]}" &&
        checkStat "$mountFolder/$fileInTar" &&
        verifyCheckSum "$mountFolder" "$fileInTar" "$archive" "$correctChecksum"
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"
    funmount "$mountFolder"
    [ -n "$( find "$indexFolder" -type f -size +0c )" ] || returnError "$LINENO" 'Index was not created!'
    find "$indexFolder" -type f -size +0c -delete

    # Check that the special "empty" folder works signaling to store alongside the TAR
    local indexFile=${archive}.index.sqlite
    rm -f -- "$indexFile"
    args=( --index-folders '' "$archive" "$mountFolder" )
    {
        runAndCheckRatarmount "${args[@]}" &&
        checkStat "$mountFolder/$fileInTar" &&
        verifyCheckSum "$mountFolder" "$fileInTar" "$archive" "$correctChecksum"
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"
    funmount "$mountFolder"
    [ -f "$indexFile" ] || returnError "$LINENO" "Index '$indexFile' was not created."
    rm -- "$indexFile"

    # Check that the multiple folders can be specified and the fallback is used if locations before it are not writable
    [ ! -d /SHOULD_NOT_EXIST ] || returnError "$LINENO" 'Location should not exist!'
    [ -z "$( find "$indexFolder" -type f -size +0c )" ] || returnError "$LINENO" 'Index should not exist before test!'
    args=( --index-folders "/SHOULD_NOT_EXIST,$indexFolder" "$archive" "$mountFolder" )
    {
        runAndCheckRatarmount "${args[@]}" &&
        checkStat "$mountFolder/$fileInTar" &&
        verifyCheckSum "$mountFolder" "$fileInTar" "$archive" "$correctChecksum"
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"
    funmount "$mountFolder"
    [ ! -d /SHOULD_NOT_EXIST ] || returnError "$LINENO" 'Location should not exist!'
    [ -n "$( find "$indexFolder" -type f -size +0c )" ] ||
        returnError "$LINENO" "Index was not created in '$indexFolder'"
    find "$indexFolder" -type f -size +0c -delete

    # Check that the multiple folders can be specified using JSON
    [ ! -d /SHOULD_NOT_EXIST ] || returnError "$LINENO" 'Location should not exist!'
    [ -z "$( find "$indexFolder" -type f -size +0c )" ] ||
        returnError "$LINENO" "Index location '$indexFolder' should be empty before test."
    args=( --index-folders '["'"$indexFolder"'", "/SHOULD_NOT_EXIST"]' "$archive" "$mountFolder" )
    {
        runAndCheckRatarmount "${args[@]}" &&
        checkStat "$mountFolder/$fileInTar" &&
        verifyCheckSum "$mountFolder" "$fileInTar" "$archive" "$correctChecksum"
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"
    funmount "$mountFolder"
    [ ! -d /SHOULD_NOT_EXIST ] || returnError "$LINENO" 'Location should not exist!'
    [ -n "$( find "$indexFolder" -type f -size +0c )" ] || returnError "$LINENO" 'Index was not created!'
    find "$indexFolder" -type f -size +0c -delete


    cleanup
    echoerr "[${FUNCNAME[0]}] Tested successfully '$fileInTar' in '$archive'"

    return 0
}


checkIndexArgumentChangeDetection()
{
    # Ratarmount should warn when an index created without the --recursive option is loaded with --recursive

    # The --index-folders should overwrite the default index locations and also give fallbacks in order
    local archive="$1"; shift
    local fileInTar="$1"; shift
    local correctChecksum="$1"

    rm -f ratarmount.{stdout,stderr}.log

    local args mountFolder indexFolder
    mountFolder="$( mktemp -d )" || returnError "$LINENO" 'Failed to create temporary directory'
    indexFolder="$( mktemp -d )" || returnError "$LINENO" 'Failed to create temporary directory'
    indexFile="$indexFolder/ratarmount.index"
    MOUNT_POINTS_TO_CLEANUP+=( "$mountFolder" )
    TMP_FILES_TO_CLEANUP+=( "$indexFile" "$indexFolder" )

    # Create an index with default configuration
    args=( --index-file "$indexFile" "$archive" "$mountFolder" )
    {
        runAndCheckRatarmount "${args[@]}" &&
        checkStat "$mountFolder/$fileInTar" &&
        verifyCheckSum "$mountFolder" "$fileInTar" "$archive" "$correctChecksum"
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"
    funmount "$mountFolder"
    [ -s "$indexFile" ] || returnError "$LINENO" "Index '$indexFile' was not created."

    # Check for warnings when loading that index with different index-influencing arguments
    args=( --recursive --ignore-zeros --index-file "$indexFile" "$archive" "$mountFolder" )
    {
        $RATARMOUNT_CMD "${args[@]}" >ratarmount.stdout.log 2>ratarmount.stderr.log &&
        checkStat "$mountFolder/$fileInTar" &&
        verifyCheckSum "$mountFolder" "$fileInTar" "$archive" "$correctChecksum"
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"
    funmount "$mountFolder"
    'grep' -Eqi '(warn|error)' ratarmount.stdout.log ratarmount.stderr.log ||
        returnError "$LINENO" "Expected warnings while executing: $RATARMOUNT_CMD ${args[*]}"

    cleanup
    echoerr "[${FUNCNAME[0]}] Tested successfully '$fileInTar' in '$archive'"

    return 0
}


checkSuffixStripping()
{
    local archive="$1"; shift
    local fileInTar="$1"; shift
    local correctChecksum="$1"

    rm -f ratarmount.{stdout,stderr}.log

    local mountFolder
    mountFolder="$( mktemp -d )" || returnError "$LINENO" 'Failed to create temporary directory'
    MOUNT_POINTS_TO_CLEANUP+=( "$mountFolder" )

    # try with index recreation
    local args=( -P "$parallelization" -c --ignore-zeros --recursive --strip-recursive-tar-extension "$archive" "$mountFolder" )
    {
        runAndCheckRatarmount "${args[@]}" &&
        checkStat "$mountFolder/$fileInTar" &&
        verifyCheckSum "$mountFolder" "$fileInTar" "$archive" "$correctChecksum"
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"
    funmount "$mountFolder"

    echoerr "[${FUNCNAME[0]}] Tested successfully '$fileInTar' in '$archive' for checksum $correctChecksum"

    return 0
}

checkRecursiveFolderMounting()
{
    # Do all test archive checks at once by copying them to temporary folder and recursively mounting that folder

    rm -f ratarmount.{stdout,stderr}.log

    local archiveFolder mountFolder
    archiveFolder="$( mktemp -d )" || returnError "$LINENO" 'Failed to create temporary directory'
    mountFolder="$( mktemp -d )" || returnError "$LINENO" 'Failed to create temporary directory'
    MOUNT_POINTS_TO_CLEANUP+=( "$mountFolder" )

    for (( iTest = 0; iTest < ${#tests[@]}; iTest += 3 )); do
        'cp' --no-clobber -- "${tests[iTest+1]}" "$archiveFolder"
    done
    runAndCheckRatarmount -c --ignore-zeros --recursive "$@" "$archiveFolder" "$mountFolder"

    local nChecks=0
    for (( iTest = 0; iTest < ${#tests[@]}; iTest += 3 )); do
        checksum=${tests[iTest]}
        archive=${tests[iTest+1]}
        fileInTar=${tests[iTest+2]}

        recursiveMountFolder="$mountFolder/$( basename -- "$archive" )"
        checkStat "$recursiveMountFolder/$fileInTar" || returnError "stat failed for: $recursiveMountFolder/$fileInTar"
        verifyCheckSum "$recursiveMountFolder" "$fileInTar" "$archive" "$checksum" || returnError 'checksum mismatch!'
        (( ++nChecks ))
    done

    cleanup
    rm -rf -- "$archiveFolder"

    echoerr "[${FUNCNAME[0]}] Tested $nChecks files successfully"
}

checkNestedRecursiveFolderMounting()
{
    local archive="$1"; shift
    local fileInTar="$1"; shift
    local correctChecksum="$1"

    rm -f ratarmount.{stdout,stderr}.log

    local mountFolder archiveFolder
    archiveFolder="$( mktemp -d )" || returnError "$LINENO" 'Failed to create temporary directory'
    mountFolder="$( mktemp -d )" || returnError "$LINENO" 'Failed to create temporary directory'
    MOUNT_POINTS_TO_CLEANUP+=( "$mountFolder" )

    mkdir -- "$archiveFolder/nested-folder"
    cp -- "$archive" "$archiveFolder/nested-folder"
    local args=( -P "$parallelization" -c --ignore-zeros --recursive --strip-recursive-tar-extension "$archiveFolder" "$mountFolder" )
    {
        recursiveMountFolder="$mountFolder/nested-folder/$( basename -- "${archive%.tar}" )"
        runAndCheckRatarmount "${args[@]}" &&
        checkStat "$recursiveMountFolder/$fileInTar" &&
        verifyCheckSum "$recursiveMountFolder" "$fileInTar" "$archive" "$correctChecksum"
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"

    cleanup
    rm -rf -- "$archiveFolder"

    echoerr "[${FUNCNAME[0]}] Tested successfully '$fileInTar' in '$archive'"

    return 0
}

checkSelfReferencingHardLinks()
{
    # This tests self-referencing hardlinks with no actual file in the tar.
    # The tar should mount and list the files but neither their contents nor stats have to be available.
    local archive=$1

    rm -f ratarmount.{stdout,stderr}.log

    local mountFolder
    mountFolder="$( mktemp -d )" || returnError "$LINENO" 'Failed to create temporary directory'
    MOUNT_POINTS_TO_CLEANUP+=( "$mountFolder" )

    local args=( -P "$parallelization" -c "$archive" "$mountFolder" )
    {
        runAndCheckRatarmount "${args[@]}"
        if [[ -z "$( find "$mountFolder" -mindepth 1 2>/dev/null )" ]]; then returnError "$LINENO" 'Expected files in mount point'; fi
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"

    cleanup
    rm -rf -- "$archiveFolder"

    echoerr "[${FUNCNAME[0]}] Tested successfully '$archive'"

    return 0
}


rm -f ratarmount.{stdout,stderr}.log

# Linting only to be done locally because in CI it is in separate steps
if [[ -z "$CI" ]]; then
    # Ignore Python 3.9. because of the Optiona[T] type hint bug in pylint: https://github.com/PyCQA/pylint/issues/3882
    if [[ ! $( python3 --version ) =~ \ 3\.9\.* ]]; then
        pylint ratarmount.py setup.py | tee pylint.log
        if 'grep' -E -q ': E[0-9]{4}: ' pylint.log; then
            echoerr 'There were warnings during the pylint run!'
            exit 1
        fi
    fi
    mypy ratarmount.py setup.py || returnError "$LINENO" 'Mypy failed!'
    pytype -d import-error ratarmount.py || returnError "$LINENO" 'Pytype failed!'
    black -q --line-length 120 --skip-string-normalization ratarmount.py tests/tests.py

    while read -r file; do
        flake8 "$file" || returnError "$LINENO" 'Flake8 failed!'
    done < <( git ls-tree --name-only HEAD '*.py' )

    shellcheck tests/*.sh || returnError "$LINENO" 'shellcheck failed!'
fi


python3 tests/tests.py || returnError "$LINENO" "tests/tests.py"


rm -f tests/*.index.*

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
    832c78afcb9832e1a21c18212fc6c38b tests/gnu-sparse-files.tar                   01.sparse1.bin
    832c78afcb9832e1a21c18212fc6c38b tests/gnu-sparse-files.tar                   02.normal1.bin
    832c78afcb9832e1a21c18212fc6c38b tests/gnu-sparse-files.tar                   03.sparse1.bin
    b3de7534cbc8b8a7270c996235d0c2da tests/concatenated.tar                       foo/fighter
    2709a3348eb2c52302a7606ecf5860bc tests/concatenated.tar                       foo/bar
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
    b026324c6904b2a9cb4b88d6d61c81d1 tests/2k-recursive-tars.tar.bz2              mimi/00001.tar/foo
    8f30b20831bade7a2236edf09a55af60 tests/2k-recursive-tars.tar.bz2              mimi/01333.tar/foo
    f95f8943f6dcf7b3c1c8c2cab5455f8b tests/2k-recursive-tars.tar.bz2              mimi/02000.tar/foo
    c157a79031e1c40f85931829bc5fc552 tests/2k-recursive-tars.tar.bz2              mimi/foo
    19696f24a91fc4e8950026f9c801a0d0 tests/simple.bz2                             simple
    19696f24a91fc4e8950026f9c801a0d0 tests/simple.gz                              simple
    2709a3348eb2c52302a7606ecf5860bc tests/file-existing-as-non-link-and-link.tar foo/fighter/ufo
    d3b07384d113edec49eaa6238ad5ff00 tests/two-self-links-to-existing-file.tar    bar
)


for parallelization in 1 2 0; do

echo "== Testing with -P $parallelization =="
export parallelization


checkIndexPathOption tests/single-file.tar bar d3b07384d113edec49eaa6238ad5ff00
checkIndexFolderFallback tests/single-file.tar bar d3b07384d113edec49eaa6238ad5ff00
checkIndexArgumentChangeDetection tests/single-file.tar bar d3b07384d113edec49eaa6238ad5ff00
checkSuffixStripping tests/2k-recursive-tars.tar.bz2 mimi/00001/foo b026324c6904b2a9cb4b88d6d61c81d1
checkNestedRecursiveFolderMounting tests/single-file.tar bar d3b07384d113edec49eaa6238ad5ff00

checkTarEncoding tests/single-file.tar utf-8 bar d3b07384d113edec49eaa6238ad5ff00
checkTarEncoding tests/single-file.tar latin1 bar d3b07384d113edec49eaa6238ad5ff00
checkTarEncoding tests/special-char.tar latin1 'Datei-mit-dämlicher-Kodierung.txt' 2709a3348eb2c52302a7606ecf5860bc

checkLinkInTAR tests/symlinks.tar foo ../foo
checkLinkInTAR tests/symlinks.tar python /usr/bin/python

checkFileInTARPrefix '' tests/single-nested-file.tar foo/fighter/ufo 2709a3348eb2c52302a7606ecf5860bc
checkFileInTARPrefix foo tests/single-nested-file.tar fighter/ufo 2709a3348eb2c52302a7606ecf5860bc
checkFileInTARPrefix foo/fighter tests/single-nested-file.tar ufo 2709a3348eb2c52302a7606ecf5860bc

checkAutomaticIndexRecreation || returnError "$LINENO" 'Automatic index recreation test failed!'
checkAutoMountPointCreation || returnError "$LINENO" 'Automatic mount point creation test failed!'
checkUnionMount || returnError "$LINENO" 'Union mounting test failed!'
checkUnionMountFileVersions || returnError "$LINENO" 'Union mount file version access test failed!'

checkSelfReferencingHardLinks tests/single-self-link.tar ||
    returnError "$LINENO" 'Self-referencing hardlinks test failed!'
checkSelfReferencingHardLinks tests/two-self-links.tar ||
    returnError "$LINENO" 'Self-referencing hardlinks test failed!'

checkRecursiveFolderMounting
checkRecursiveFolderMounting --lazy

for (( iTest = 0; iTest < ${#tests[@]}; iTest += 3 )); do
    checksum=${tests[iTest]}
    tarPath=${tests[iTest+1]}
    fileName=${tests[iTest+2]}

    readarray -t files < <( recompressFile "$tarPath" )
    TMP_FILES_TO_CLEANUP+=( "${files[@]}" )
    [[ ${#files[@]} -gt 3 ]] || returnError "$LINENO" 'Something went wrong during recompression.'

    for file in "${files[@]}"; do
        case "$( file --mime-type -- "$file" | sed 's|.*[/-]||' )" in
            bzip2|gzip|xz|zstd|tar)
                TMP_FILES_TO_CLEANUP+=( "${file}.index.sqlite" )
                checkFileInTAR "$file" "$fileName" "$checksum"
                ;;
        esac
        (( ++nFiles ))
    done < <( recompressFile "$tarPath" )
    cleanup
    rmdir -- "$( dirname -- "$file" )"
done

benchmarkDecoderBackends
#benchmarkSerialization # takes quite long, and a benchmark is not a test ...

rm -f tests/*.index.*
rmdir tests/*/

done  # for parallelization


echo -e '\e[32mAll tests ran successfully.\e[0m'

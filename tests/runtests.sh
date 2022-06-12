#!/bin/bash

cd -- "$( dirname -- "${BASH_SOURCE[0]}" )/.." || { echo 'Failed to cd to ratarmount.py folder!'; exit 1; }

if [[ -z "$RATARMOUNT_CMD" ]]; then
    RATARMOUNT_CMD="python3 -u $( realpath -- ratarmount.py )"
    #RATARMOUNT_CMD=ratarmount
    export RATARMOUNT_CMD
fi


# MAC does not have mountpoint check!
if ! command -v mountpoint &>/dev/null; then
    mountpoint()
    {
        if [[ "$1" == '--' ]]; then shift; fi
        # Note that this does not the slightly more rigorous check of grepping for " on $1"
        # because on the Github actions runner it seems we are chrooted to /private, which means
        # the paths
        mount | 'grep' -F -q "$1"
    }
    export -f mountpoint
fi

if uname | 'grep' -q -i darwin; then
    getFileSize() { stat -f %z -- "$1"; }
    getFileMode() { stat -f %OLp -- "$1"; }
    getFileMtime() { stat -f %m -- "$1"; }
    setFileMTime() { touch -m -t "$( date -r "$1" +%Y%m%d%H%M.%S )" "$2"; }
    safeRmdir() { if [[ -z "$( find "$1" -maxdepth 1 )" ]]; then rmdir "$1"; fi; }
else
    getFileSize() { stat -c %s -- "$1"; }
    getFileMode() { stat -c %a -- "$1"; }
    getFileMtime() { stat -c %Y -- "$1"; }
    setFileMTime() { touch -d "@$1" "$2"; }
    safeRmdir() { rmdir --ignore-fail-on-non-empty -- "$1"; }
fi

export -f getFileSize

TMP_FILES_TO_CLEANUP=()
MOUNT_POINTS_TO_CLEANUP=()
cleanup()
{
    sleep 0.5s # Give a bit of time for the mount points to become stable before trying to unmount them
    for folder in "${MOUNT_POINTS_TO_CLEANUP[@]}"; do
        if [[ -d "$folder" ]]; then
            funmount "$folder"
        fi
    done
    sleep 0.5s
    for folder in "${MOUNT_POINTS_TO_CLEANUP[@]}"; do
        if [[ -d "$folder" ]]; then safeRmdir "$folder"; fi
    done
    MOUNT_POINTS_TO_CLEANUP=()

    for file in "${TMP_FILES_TO_CLEANUP[@]}"; do
        if [ -d "$file" ]; then safeRmdir "$file"; fi
        if [ -f "$file" ]; then rm -- "$file"; fi
    done
    TMP_FILES_TO_CLEANUP=()
}

trap 'cleanup' EXIT

echoerr() { echo "$@" 1>&2; }

toolMissing=0
for tool in dd zstd stat grep tar diff find gzip pixz bzip2; do
    if ! command -v "$tool" &>/dev/null; then
        echoerr -e '\e[37mDid not find the required '"$tool"' command!\e[0m'
        toolMissing=1
    fi
done
if [[ $toolMissing -eq 1 ]]; then exit 1; fi

createMultiFrameZstd()
(
    # Detect being piped into
    if [ -t 0 ]; then
        file=$1
        frameSize=$2
        if [[ ! -f "$file" ]]; then echo "Could not find file '$file'." 1>&2; return 1; fi
        fileSize=$( getFileSize "$file" )
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
            if uname | 'grep' -q  -i darwin; then
                # This is only a very rudimentary hack and should be used for very large frameSize values!
                python3 -c "with open('$file', 'rb') as ifile, open('$frameFile', 'wb') as ofile:
                    ifile.seek($offset)
                    ofile.write(ifile.read($frameSize))
                "
            else
                dd if="$file" of="$frameFile" bs=$(( 1024*1024 )) \
                   iflag=skip_bytes,count_bytes skip="$offset" count="$frameSize" 2>/dev/null
            fi
            zstd -c -q -- "$frameFile" >> "$file.zst"
        done
    else
        while true; do
            if uname | 'grep' -q  -i darwin; then
                # untested!
                head -c "$frameSize" > "$frameFile"
            else
                dd of="$frameFile" bs=$(( 1024*1024 )) iflag=count_bytes count="$frameSize" 2>/dev/null
            fi
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

    while mountpoint -- "$mountFolder" &>/dev/null; do
        sleep 0.2s
        $RATARMOUNT_CMD -u "$mountFolder"
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

runRatarmount()
{
    MOUNT_POINTS_TO_CLEANUP+=( "${*: -1}" )
    $RATARMOUNT_CMD "$@" >ratarmount.stdout.log 2>ratarmount.stderr.log.tmp &&
    checkStat "${@: -1}" # mount folder must exist and be stat-able after mounting

    # Python 3.6 on macOS gives a warning:
    # /opt/hostedtoolcache/Python/3.6.15/x64/lib/python3.6/site-packages/rarfile.py:71:
    # CryptographyDeprecationWarning: Python 3.6 is no longer supported by the Python core team.
    # Therefore, support for it is deprecated in cryptography and will be removed in a future release.
    # from cryptography.hazmat.backends import default_backend
    # sed -i does not work on macOS: "sed: -i may not be used with stdin".
    sed '/CryptographyDeprecationWarning/d' ratarmount.stderr.log.tmp > ratarmount.stderr.log
}

runAndCheckRatarmount()
{
    runRatarmount "$@"
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
    if [[ "$archive" =~ .tar ]]; then
        'grep' -q 'Creating offset dictionary' ratarmount.stdout.log ratarmount.stderr.log ||
            returnError "$LINENO" "Looks like index was not created while executing: $RATARMOUNT_CMD ${args[*]}"
    fi

    # retry without forcing index recreation
    local args=( -P "$parallelization" --ignore-zeros --recursive "$archive" "$mountFolder" )
    {
        runAndCheckRatarmount "${args[@]}" &&
        checkStat "$mountFolder/$fileInTar" &&
        verifyCheckSum "$mountFolder" "$fileInTar" "$archive" "$correctChecksum"
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"
    funmount "$mountFolder"

    if [[ "$archive" =~ .tar ]]; then
        'grep' -q 'Successfully loaded offset dictionary' ratarmount.stdout.log ratarmount.stderr.log ||
            returnError "$LINENO" "Looks like index was not loaded while executing: $RATARMOUNT_CMD ${args[*]}"
    fi

    safeRmdir "$mountFolder"

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

    safeRmdir "$mountFolder"

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

    safeRmdir "$mountFolder"

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

    while ! mountpoint -- "$mountFolder"; do sleep 1s; done
    $RATARMOUNT_CMD -u "$mountFolder"
    wait "$memoryUsagePid"
    wait "$ratarmountPid"

    # do again but this time benchmark loading the created index

    $RATARMOUNT_CMD -P "$parallelization" -f --recursive "$largeTar" "$mountFolder" &
    local ratarmountPid="$!"

    local timeSeriesFile="benchmark-memory-${fileNameDataSizeInMB}-MiB-loading.dat"
    memoryUsage "$ratarmountPid" "$timeSeriesFile" &
    local memoryUsagePid="$!"

    while ! mountpoint -- "$mountFolder"; do sleep 1s; done
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
    runAndCheckRatarmount "$archive"
    diff -- "$fileName" "$mountFolder/$fileName" || returnError "$LINENO" 'Files differ on simple mount!'
    funmount "$mountFolder"

    # 2. Check that index does not get recreated normally
    sleep 1 # because we are comparing timestamps with seconds precision ...
    indexFile="$archive.index.sqlite"
    [[ -f $indexFile ]] || returnError "$LINENO" 'Index file not found!'
    lastModification=$( getFileMtime "$indexFile" )
    runAndCheckRatarmount "$archive"
    diff -- "$fileName" "$mountFolder/$fileName" || returnError "$LINENO" 'Files differ on simple remount!'
    funmount "$mountFolder"
    [[ $lastModification -eq $( getFileMtime "$indexFile" ) ]] ||
        returnError "$LINENO" 'Index changed even though TAR did not!'

    # 3. Change contents (and timestamp) without changing the size
    #    (Luckily TAR is filled to 10240 Bytes anyways for very small files)
    sleep 1 # because we are comparing timestamps with seconds precision ...
    fileName="${fileName//e/a}"
    echo 'momo' > "$fileName"
    tar -cf "$archive" "$fileName"

    # modification timestamp detection is turned off for now by default to facilitate index sharing because
    # the mtime check can proove problematic as the mtime changes when downloading a file.
    runAndCheckRatarmount "$archive"
    ! [[ -f "$mountFolder/${fileName}" ]] ||
        returnError "$LINENO" 'Index should not have been recreated and therefore contain outdated file name!'
    funmount "$mountFolder"

    runRatarmount --verify-mtime "$archive"
    'grep' -Eqi 'warn' ratarmount.stdout.log ratarmount.stderr.log ||
        returnError "$LINENO" "Found no warnings while executing: $RATARMOUNT_CMD --verify-mtime $archive"
    diff -- "$fileName" "$mountFolder/${fileName}" ||
        returnError "$LINENO" 'Files differ when trying to trigger index recreation!'
    funmount "$mountFolder"

    [[ $lastModification -ne $( getFileMtime "$indexFile" ) ]] || \
        returnError "$LINENO" 'Index did not change even though TAR did!'
    lastModification=$( getFileMtime "$indexFile" )

    # 4. Check that index changes if size changes but modification timestamp does not
    sleep 1 # because we are comparing timestamps with seconds precision ...
    fileName="heho"
    head -c $(( 100 * 1024 )) /dev/urandom > "$fileName"
    tar -cf "$archive" "$fileName"
    setFileMTime "$lastModification" "$archive"

    runRatarmount "$archive"
    'grep' -Eqi 'warn' ratarmount.stdout.log ratarmount.stderr.log ||
        returnError "$LINENO" "Found no warnings while executing: $RATARMOUNT_CMD $archive"
    diff -- "$fileName" "$mountFolder/${fileName}" || returnError "$LINENO" 'Files differ!'
    funmount "$mountFolder"
    [[ $lastModification -ne $( getFileMtime "$indexFile" ) ]] || \
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
        # macOS is missing the --no-dereference option
        if ! uname | 'grep' -q -i darwin; then
            diff -r --no-dereference "$tarFile" "$mountPoint" || returnError "$LINENO" 'Bind mounted folder differs!'
        fi
        funmount "$mountPoint"

        # Check that bind mount onto the mount point works (does not work with AppImage for some reason!)
        if [[ "$RATARMOUNT_CMD" =~ ^python3* ]]; then
            runAndCheckRatarmount -c "$tarFile" "$tarFile"
            [[ $( find "$tarFile" -mindepth 1 | wc -l ) -gt 0 ]] || returnError "$LINENO" 'Bind mounted folder is empty!'
            funmount "$tarFile"
        fi

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

    # Check whether union mounting two folders works

    mkdir -p "folder1/subfolder"
    echo 'hello' > "folder1/subfolder/world"
    mkdir "folder2"
    echo 'iriya' > "folder2/ufo"

    runAndCheckRatarmount -c "folder1" "folder2" "$mountPoint"

    checkStat "$mountPoint/" || returnError "$LINENO" 'Could not stat file!'
    checkStat "$mountPoint/ufo" || returnError "$LINENO" 'Could not stat file!'
    checkStat "$mountPoint/subfolder/world" || returnError "$LINENO" 'Could not stat file!'

    verifyCheckSum "$mountPoint" "subfolder/world" "<folder union mount>" b1946ac92492d2347c6235b4d2611184 ||
        returnError "$LINENO" 'Checksum mismatches!'
    verifyCheckSum "$mountPoint" "ufo" "<folder union mount>" 2709a3348eb2c52302a7606ecf5860bc ||
        returnError "$LINENO" 'Checksum mismatches!'

    # Clean up

    safeRmdir "$mountPoint"
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
    mkdir -p folder2/foo/fighter
    echo 'untarred' > folder2/foo/fighter/ufo
    mkdir emptyFolder
    mkdir emptyFolder2

    runAndCheckRatarmount -c emptyFolder folder "$testsFolder/updated-file.tar" emptyFolder2 folder2 mountPoint

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
    runAndCheckRatarmount -- *.tar
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

    safeRmdir "$mountFolder"

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

    if [[ ! -f "$file" ]]; then
        echoerr "\e[31mFile '$file' does not exist.\[e0m"
        return 1
    fi

    cp -- "$file" "$tmpFolder"
    file="$tmpFolder/$( basename -- "$file" )"

    local uncompressedFile=
    uncompressedFile=${file%.*}
    [[ "$uncompressedFile" != "$file" ]] || returnError "$LINENO" 'Given file seems to have no extension!'

    local extension=${file##*.}
    if [[ "$extension" == zip || "$extension" == rar ]]; then
        printf '%s\n' "$file"
        return 0
    fi

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
    supportedCompressions=( bz2 gz xz zst )
    for compression in "${supportedCompressions[@]}"; do
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

    printf '%s\n' "${recompressedFiles[@]}"
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
                printf '% 5s : ' "pbz2"
                python3 -m timeit 'from indexed_bzip2 import IndexedBzip2File as IBF; IBF( '"'$file'"', parallelization = 0 ).read();'
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
    args=( -P "$parallelization" --recursive --ignore-zeros --index-file "$indexFile" "$archive" "$mountFolder" )
    {
        runRatarmount "${args[@]}" &&
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
        'cp' -- "${tests[iTest+1]}" "$archiveFolder"
    done
    runAndCheckRatarmount -P "$parallelization" -c --ignore-zeros --recursive "$@" "$archiveFolder" "$mountFolder"

    local nChecks=0
    for (( iTest = 0; iTest < ${#tests[@]}; iTest += 3 )); do
        checksum=${tests[iTest]}
        archive=${tests[iTest+1]}
        fileInTar=${tests[iTest+2]}

        # TODO recursive mounting of split files is not yet working because we would have to do more
        #      work detecting the wildly varying file extensions and joining virtual file objects.
        if [[ $archive =~ split ]]; then continue; fi

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
    local args=(
        -P "$parallelization" -c --ignore-zeros --recursive --strip-recursive-tar-extension
        "$archiveFolder" "$mountFolder"
    )
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

checkWriteOverlayFileMetadataModifications()
{
    local fileSubPath="$1"
    local filePath="$mountFolder/$1"

    ## Change modification time

    setFileMTime 1234567890 "$filePath"
    if [[ "$( getFileMtime "$filePath" )" != 1234567890 ]]; then
        returnError "$LINENO" 'Modification time did not change'
    fi

    ## Change permissions

    chmod 777 "$filePath"
    local mode
    mode=$( getFileMode "$filePath" )
    if [[ "$mode" != 777 ]]; then
        returnError "$LINENO" "Permissions could not be changed ($mode != 777)"
    fi

    ## Change permissions again (in case they already were 777 for the first test)

    chmod 700 "$filePath"
    mode=$( getFileMode "$filePath" )
    if [[ "$mode" != 700 ]]; then
        returnError "$LINENO" "Permissions could not be changed ($mode != 700)"
    fi

    # Make copy to compare moved file
    local tmpCopy
    if [[ -f "$filePath" ]]; then
        tmpCopy=$( mktemp )
        'cp' "$filePath" "$tmpCopy"
    fi

    ## Rename file

    'mv' "$filePath" "${filePath}.new"
    if [[ -e "$filePath" ]]; then returnError "$LINENO" 'File should have been renamed'; fi
    if [[ ! -e "${filePath}.new" ]]; then returnError "$LINENO" 'Renamed file should exist'; fi
    if [[ -n "$tmpCopy" ]]; then
        diff -q "$tmpCopy" "${filePath}.new" || returnError "$LINENO" 'Mismatching contents'
    fi

    ## Rename file back

    'mv' "${filePath}.new" "$filePath"
    if [[ -e "${filePath}.new" ]]; then returnError "$LINENO" 'File should have been renamed'; fi
    if [[ ! -e "$filePath" ]]; then returnError "$LINENO" 'Renamed file should exist'; fi
    if [[ -n "$tmpCopy" ]]; then
        diff -q "$tmpCopy" "$filePath" || returnError "$LINENO" 'Mismatching contents'
    fi
}

checkWriteOverlayFile()
{
    local fileSubPath="$1"
    local filePath="$mountFolder/$1"

    ## Create file

    touch "$filePath"
    verifyCheckSum "$mountFolder" "$fileSubPath" '[write overlay]' d41d8cd98f00b204e9800998ecf8427e ||
        returnError "$LINENO" 'Mismatching checksum'

    ## Write into file

    echo "summer" > "$filePath"
    verifyCheckSum "$mountFolder" "$fileSubPath" '[write overlay]' e75e33e14332df297c9ef5ea0cdcd006 ||
        returnError "$LINENO" 'Mismatching checksum'

    ## Append to file

    echo "sky" >> "$filePath"
    verifyCheckSum "$mountFolder" "$fileSubPath" '[write overlay]' d95778027cdefb2416a93446c9892992 ||
        returnError "$LINENO" 'Mismatching checksum'

    checkWriteOverlayFileMetadataModifications "$fileSubPath"

    ## Delete file

    'rm' "$filePath"
    if [[ -f "$filePath" ]]; then returnError "$LINENO" 'File should have been deleted'; fi
    if find "$mountFolder" | 'grep' -q "$filePath"; then returnError "$LINENO" 'File should not appear in listing'; fi
}


checkWriteOverlayWithNewFiles()
{
    local archive='tests/single-nested-folder.tar'

    rm -f ratarmount.{stdout,stderr}.log

    local mountFolder
    mountFolder="$( mktemp -d )" || returnError "$LINENO" 'Failed to create temporary directory'
    MOUNT_POINTS_TO_CLEANUP+=( "$mountFolder" )

    local overlayFolder;
    overlayFolder=$( mktemp -d )
    # Create the overlay folder on some filesystem, e.g., NTFS FUSE, which does not support
    # permission changes for testing the metadata database.
    #overlayFolder=$( mktemp -d -p "$( pwd )" )

    local args=( -P "$parallelization" -c --write-overlay "$overlayFolder" "$archive" "$mountFolder" )
    {
        runAndCheckRatarmount "${args[@]}"
        if [[ -z "$( find "$mountFolder" -mindepth 1 2>/dev/null )" ]]; then returnError "$LINENO" 'Expected files in mount point'; fi
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"

    verifyCheckSum "$mountFolder" 'foo/fighter/ufo' 'tests/single-nested-folder.tar' 2709a3348eb2c52302a7606ecf5860bc ||
        returnError "$LINENO" 'Mismatching checksum'

    # Check for file directly in the write overlay root
    checkWriteOverlayFile 'iriya'

    # Check file inside an archive folder which does not exist in the overlay (yet)
    checkWriteOverlayFile 'foo/iriya'

    ## Test folder creation, modification, and deletion
    mkdir "$mountFolder/base"
    if [[ ! -d "$mountFolder/base" ]]; then returnError "$LINENO" 'Folder could not be created'; fi
    checkWriteOverlayFileMetadataModifications 'base'
    rmdir "$mountFolder/base"
    if [[ -d "$mountFolder/base" ]]; then returnError "$LINENO" 'Folder could not be removed'; fi

    ## Test folder creation inside nested path not existing in overlay (yet)
    mkdir "$mountFolder/foo/base"
    if [[ ! -d "$mountFolder/foo/base" ]]; then returnError "$LINENO" 'Folder could not be created'; fi
    checkWriteOverlayFileMetadataModifications 'base'
    rmdir "$mountFolder/foo/base"
    if [[ -d "$mountFolder/foo/base" ]]; then returnError "$LINENO" 'Folder could not be removed'; fi

    ## Create symbolic link between overlay files

    echo 'summer' > "$mountFolder/iriya"
    ( cd "$mountFolder" && ln -s "iriya" "twin-iriya"; )
    verifyCheckSum "$mountFolder" 'iriya' '[write overlay]' e75e33e14332df297c9ef5ea0cdcd006 ||
        returnError "$LINENO" 'Mismatching checksum'
    verifyCheckSum "$mountFolder" 'twin-iriya' '[write overlay]' e75e33e14332df297c9ef5ea0cdcd006 ||
        returnError "$LINENO" 'Mismatching checksum'
    if [[ "$( readlink -- "$mountFolder/twin-iriya" )" != "iriya" ]]; then
        returnError "$LINENO" 'Expected different link target for created symbolic link'
    fi

    ## Delete symbolic link

    unlink "$mountFolder/twin-iriya"
    if [[ -f "$mountFolder/twin-iriya" ]]; then returnError "$LINENO" 'Symbolic link should have been deleted'; fi

    ## Create hard link between overlay files

    ( cd "$mountFolder" && ln "iriya" "twin-iriya"; ) || returnError "$LINENO" 'Could not create hardlink'
    verifyCheckSum "$mountFolder" 'iriya' '[write overlay]' e75e33e14332df297c9ef5ea0cdcd006 ||
        returnError "$LINENO" 'Mismatching checksum'
    verifyCheckSum "$mountFolder" 'twin-iriya' '[write overlay]' e75e33e14332df297c9ef5ea0cdcd006 ||
        returnError "$LINENO" 'Mismatching checksum'

    ## Delete hard link

    unlink "$mountFolder/twin-iriya"
    if [[ -f "$mountFolder/twin-iriya" ]]; then returnError "$LINENO" 'Symbolic link should have been deleted'; fi

    ## Create symbolic link to file in archive

    ( cd "$mountFolder" && ln -s "foo/fighter/ufo" "sighting"; )
    verifyCheckSum "$mountFolder" 'foo/fighter/ufo' 'tests/single-nested-folder.tar' 2709a3348eb2c52302a7606ecf5860bc ||
        returnError "$LINENO" 'Mismatching checksum'
    verifyCheckSum "$mountFolder" 'sighting' 'tests/single-nested-folder.tar' 2709a3348eb2c52302a7606ecf5860bc ||
        returnError "$LINENO" 'Mismatching checksum'
    if [[ "$( readlink -- "$mountFolder/sighting" )" != "foo/fighter/ufo" ]]; then
        returnError "$LINENO" 'Expected different link target for created symbolic link'
    fi

    'rm' "$mountFolder/iriya"

    echoerr "[${FUNCNAME[0]}] Tested successfully file modifications for overlay files."
}

checkWriteOverlayWithArchivedFiles()
{
    local archive='tests/nested-tar.tar'

    rm -f ratarmount.{stdout,stderr}.log

    local mountFolder
    mountFolder="$( mktemp -d )" || returnError "$LINENO" 'Failed to create temporary directory'
    MOUNT_POINTS_TO_CLEANUP+=( "$mountFolder" )

    local overlayFolder;
    overlayFolder=$( mktemp -d )

    local args=( -P "$parallelization" -c --write-overlay "$overlayFolder" "$archive" "$mountFolder" )
    {
        runAndCheckRatarmount "${args[@]}"
        if [[ -z "$( find "$mountFolder" -mindepth 1 2>/dev/null )" ]]; then returnError "$LINENO" 'Expected files in mount point'; fi
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"

    verifyCheckSum "$mountFolder" 'foo/fighter/ufo' 'tests/nested-tar.tar' 2709a3348eb2c52302a7606ecf5860bc ||
        returnError "$LINENO" 'Mismatching checksum'
    verifyCheckSum "$mountFolder" 'foo/lighter.tar' 'tests/nested-tar.tar' 2a06cc391128d74e685a6cb7cfe9f94d ||
        returnError "$LINENO" 'Mismatching checksum'

    # Checks for modifying files "in the" archive (this requires special handling to simulate modifications!)

    local file
    file="$mountFolder/foo/fighter/ufo"

    ## Delete archived file

    if [[ ! -f "$file" ]]; then returnError "$LINENO" 'File should exist'; fi
    'rm' "$file"
    if [[ -e "$file" ]]; then returnError "$LINENO" 'File should have been deleted'; fi

    ## Overwrite deleted file with new one

    echo 'summer' > "$file"
    verifyCheckSum "$mountFolder" 'foo/fighter/ufo' '[write overlay]' e75e33e14332df297c9ef5ea0cdcd006 ||
        returnError "$LINENO" 'Mismatching checksum'

    ## Delete new file again

    'rm' "$file"
    if [[ -e "$file" ]]; then returnError "$LINENO" 'File should have been deleted'; fi

    ## Remove folder

    if [[ ! -d "$mountFolder/foo/fighter" ]]; then returnError "$LINENO" 'Folder should exist'; fi
    rmdir "$mountFolder/foo/fighter"
    if [[ -e "$mountFolder/foo/fighter" ]]; then returnError "$LINENO" 'Folder could not be removed'; fi
    if [[ -e "$mountFolder/foo/fighter/ufo" ]]; then returnError "$LINENO" 'Folder could not be removed'; fi

    ## Append to archived file

    printf '%512s' ' ' | tr ' ' '\0' >> "$mountFolder/foo/lighter.tar"
    verifyCheckSum "$mountFolder" 'foo/lighter.tar' 'tests/nested-tar.tar' 7a534382c5b51762f072fe0d3a916e29 ||
        returnError "$LINENO" 'Mismatching checksum'

    # Roll back modification for further tests

    'rm' "$overlayFolder/foo/lighter.tar"

    ## Write into file

    echo "summer" > "$mountFolder/foo/lighter.tar"
    verifyCheckSum "$mountFolder" 'foo/lighter.tar' '[write overlay]' e75e33e14332df297c9ef5ea0cdcd006 ||
        returnError "$LINENO" 'Mismatching checksum'

    # Remount to reset state
    funmount "$mountFolder"
    'rm' -rf "$overlayFolder"
    {
        runAndCheckRatarmount "${args[@]}"
        if [[ -z "$( find "$mountFolder" -mindepth 1 2>/dev/null )" ]]; then returnError "$LINENO" 'Expected files in mount point'; fi
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"


    ## Change modification time

    file="$mountFolder/foo/fighter/ufo"
    verifyCheckSum "$mountFolder" 'foo/fighter/ufo' '[write overlay]' 2709a3348eb2c52302a7606ecf5860bc ||
        returnError "$LINENO" 'Mismatching checksum'
    setFileMTime 1234567890 "$file"
    if [[ "$( getFileMtime "$file" )" != 1234567890 ]]; then
        returnError "$LINENO" 'Modification time did not change'
    fi

    if [[ -e "$overlayFolder/foo/fighter/ufo" ]]; then returnError "$LINENO" 'Touch should not copy file to overlay'; fi

    ## Change permissions

    chmod 777 "$file"
    local mode
    mode=$( getFileMode "$file" )
    if [[ "$mode" != 777 ]]; then
        returnError "$LINENO" "Permissions could not be changed ($mode != 777)"
    fi
    chmod 700 "$file"
    mode=$( getFileMode "$file" )
    if [[ "$mode" != 700 ]]; then
        returnError "$LINENO" "Permissions could not be changed ($mode != 700)"
    fi

    # Remount to reset state
    funmount "$mountFolder"
    'rm' -rf "$overlayFolder"
    {
        runAndCheckRatarmount "${args[@]}"
        if [[ -z "$( find "$mountFolder" -mindepth 1 2>/dev/null )" ]]; then returnError "$LINENO" 'Expected files in mount point'; fi
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"

    ## Rename archive file

    'mv' "$file" "$mountFolder/foo/fighter/ugo"
    if [[ -f "$file" ]]; then returnError "$LINENO" 'File should have been renamed'; fi
    verifyCheckSum "$mountFolder" 'foo/fighter/ugo' '[write overlay]' 2709a3348eb2c52302a7606ecf5860bc ||
        returnError "$LINENO" 'Mismatching checksum'

    ## Undo the rename

    'mv' "$mountFolder/foo/fighter/ugo" "$file"
    if [[ -f "$mountFolder/iriya" ]]; then returnError "$LINENO" 'File should have been renamed'; fi
    verifyCheckSum "$mountFolder" 'foo/fighter/ufo' '[write overlay]' 2709a3348eb2c52302a7606ecf5860bc ||
        returnError "$LINENO" 'Mismatching checksum'


    echoerr "[${FUNCNAME[0]}] Tested successfully file modifications for archive files using the overlay."


    'rm' -r -- "$overlayFolder"
}

checkGnuIncremental()
{
    local archive="$1"; shift
    local fileInTar="$1"; shift
    local correctChecksum="$1"

    rm -f ratarmount.{stdout,stderr}.log

    local mountFolder
    mountFolder="$( mktemp -d )" || returnError "$LINENO" 'Failed to create temporary directory'
    MOUNT_POINTS_TO_CLEANUP+=( "$mountFolder" )

    # try with index recreation and forcing to read as GNU incremental
    local args=( -P "$parallelization" -c --ignore-zeros --recursive --gnu-incremental "$archive" "$mountFolder" )
    {
        runAndCheckRatarmount "${args[@]}" &&
        checkStat "$mountFolder/$fileInTar" &&
        verifyCheckSum "$mountFolder" "$fileInTar" "$archive" "$correctChecksum"
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"
    funmount "$mountFolder"

    safeRmdir "$mountFolder"

    echoerr "[${FUNCNAME[0]}] Tested successfully '$fileInTar' in '$archive' for checksum $correctChecksum"

    return 0
}


checkTruncated()
{
    local archive="$1"; shift
    local fileInTar="$1"; shift
    local correctChecksum="$1"

    rm -f ratarmount.{stdout,stderr}.log

    local mountFolder
    mountFolder="$( mktemp -d )" || returnError "$LINENO" 'Failed to create temporary directory'
    MOUNT_POINTS_TO_CLEANUP+=( "$mountFolder" )

    local args=( -P "$parallelization" -c "$archive" "$mountFolder" )
    {
        # Avoid runAndCheckRatarmount because it checks for warnings
        runRatarmount "${args[@]}" &&
        checkStat "$mountFolder/$fileInTar"
        # Different checksum on macOS? But it is in a broken state anyway, so I wouldn't guarantee anything.
        echo -n "Contents of $mountFolder/$fileInTar: "
        cat "$mountFolder/$fileInTar"
        echo
        #verifyCheckSum "$mountFolder" "$fileInTar" "$archive" "$correctChecksum"
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"

    cleanup

    echoerr "[${FUNCNAME[0]}] Tested successfully '$fileInTar' in '$archive' for checksum $correctChecksum"

    return 0
}


rm -f ratarmount.{stdout,stderr}.log

# Linting only to be done locally because in CI it is in separate steps
if [[ -z "$CI" ]]; then
    files=()
    while read -r file; do
        files+=( "$file" )
    done < <(
        git ls-tree -r --name-only HEAD |
            'grep' '[.]py$' |
            'grep' -v -F '__init__.py' |
            'grep' -v 'benchmarks/' |
            'grep' -v -F 'setup.py' |
            'grep' -v 'test.*.py'
    )

    testFiles=()
    while read -r file; do
        testFiles+=( "$file" )
    done < <( git ls-tree -r --name-only HEAD | 'grep' 'test.*[.]py$' | 'grep' -v 'conftest[.]py$' )

    echo "Checking files:"
    printf '    %s\n' "${files[@]}" "${testFiles[@]}"

    pylint "${files[@]}" "${testFiles[@]}" | tee pylint.log
    if 'grep' -E -q ': E[0-9]{4}: ' pylint.log; then
        echoerr 'There were warnings during the pylint run!'
        exit 1
    fi
    rm pylint.log

    mypy "${files[@]}" || returnError "$LINENO" 'Mypy failed!'
    mypy "${testFiles[@]}" || returnError "$LINENO" 'Mypy failed!'

    pytype -d import-error -P"$( cd core && pwd ):$( pwd )" "${files[@]}" \
        || returnError "$LINENO" 'Pytype failed!'

    black -q --line-length 120 --skip-string-normalization "${files[@]}" "${testFiles[@]}"

    flake8 "${files[@]}" "${testFiles[@]}" || returnError "$LINENO" 'Flake8 failed!'

    shellcheck tests/*.sh || returnError "$LINENO" 'shellcheck failed!'

    # Pytest has serious performance issues. It does collect all tests beforehand and does not free memory
    # after tests have finished it seems. Or maybe that memory is a bug with indexed_gzip but the problem is
    # that after that all tests after that one outlier also run slower. Maybe because of a Python garbage collector
    # bug? For that reason, run each test file separately.
    for testFile in "${testFiles[@]}"; do
        if [[ "${testFile//test_//}" != "$testFile" ]]; then
            # Fusepy warns about usage of use_ns because the implicit behavior is deprecated.
            # But there has been no development to fusepy for 4 years, so I think it should be fine to ignore.
            pytest --disable-warnings "$testFile" || returnError "$LINENO" 'pytest failed!'
        fi
    done
fi


rm -f tests/*.index.*
'cp' 'tests/single-file.tar' 'tests/#not-a-good-name! Ör, is it?.tar'


tests=()

# TODO Some bug with rarfile throwing: Failed the read enough data: req=304 got=51 and then seek(0) not working?
if ! uname | 'grep' -q -i darwin; then
tests+=(
    2709a3348eb2c52302a7606ecf5860bc tests/nested-with-symlink.rar                foo/fighter/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/nested-with-symlink.rar                foo/fighter/saucer
    2b87e29fca6ee7f1df6c1a76cb58e101 tests/nested-with-symlink.rar                foo/lighter.tar/fighter/bar
)
fi

# zipfile returns unseekable file object with python 3.6. Therefore I disabled it completely there.
python3MinorVersion=$( python3 --version | sed -n -E 's|.* 3[.]([0-9]+)[.][0-9]+|\1|p' )
if [[ -n "$python3MinorVersion" && "$python3MinorVersion" -gt 6 ]]; then
if ! uname | 'grep' -q -i darwin; then
tests+=(
    2709a3348eb2c52302a7606ecf5860bc tests/zip.rar                                natsu.zip/ufo
    10d6977ec2ab378e60339323c24f9308 tests/zip.rar                                natsu.zip/foo
    49b996b16f59ab6c87dea31e227f8798 tests/rar-misrecognized-as-zip.rar           bag.zip/README.md
    49b996b16f59ab6c87dea31e227f8798 tests/rar-misrecognized-as-zip.rar           bag.zip/CHANGELOG.md
    49b996b16f59ab6c87dea31e227f8798 tests/rar-misrecognized-as-zip.rar           bag1.zip/CHANGELOG.md
)
fi
tests+=(
    2709a3348eb2c52302a7606ecf5860bc tests/file-in-non-existing-folder.zip        foo2/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/rar.zip                                natsu.rar/ufo
    10d6977ec2ab378e60339323c24f9308 tests/rar.zip                                natsu.rar/foo
    2709a3348eb2c52302a7606ecf5860bc tests/nested-with-symlink.zip                foo/fighter/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/nested-with-symlink.zip                foo/fighter/saucer
    2b87e29fca6ee7f1df6c1a76cb58e101 tests/nested-with-symlink.zip                foo/lighter.tar/fighter/bar
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.zip                     foo/fighter/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.zip                     foo/jet/ufo
)
fi

tests+=(
    f47c75614087a8dd938ba4acff252494 tests/simple-file-split.001                  simple-file-split
    f47c75614087a8dd938ba4acff252494 tests/simple-file-split.002                  simple-file-split
    d3b07384d113edec49eaa6238ad5ff00 tests/single-file-split.tar.001              bar

    2709a3348eb2c52302a7606ecf5860bc tests/file-in-non-existing-folder.rar        foo2/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.rar                     foo/fighter/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.rar                     foo/jet/ufo

    2709a3348eb2c52302a7606ecf5860bc tests/updated-file-implicitly-with-folder.tar foo/fighter
    2709a3348eb2c52302a7606ecf5860bc tests/updated-file-implicitly-with-folder.tar foo.versions/2/fighter
    c157a79031e1c40f85931829bc5fc552 tests/updated-file-implicitly-with-folder.tar foo.versions/1
    2709a3348eb2c52302a7606ecf5860bc tests/updated-file-implicitly-with-folder.tar bar/par/sora/natsu
    2709a3348eb2c52302a7606ecf5860bc tests/updated-file-implicitly-with-folder.tar bar/par/sora.versions/2/natsu
    cd85c6a5e5053c04f95e1df301c80755 tests/updated-file-implicitly-with-folder.tar bar/par/sora.versions/1

    d3b07384d113edec49eaa6238ad5ff00 'tests/#not-a-good-name! Ör, is it?.tar'     bar
    d3b07384d113edec49eaa6238ad5ff00 tests/single-file.tar                        bar
    d3b07384d113edec49eaa6238ad5ff00 tests/single-file-with-leading-dot-slash.tar bar
    2b87e29fca6ee7f1df6c1a76cb58e101 tests/folder-with-leading-dot-slash.tar      foo/bar
    2709a3348eb2c52302a7606ecf5860bc tests/folder-with-leading-dot-slash.tar      foo/fighter/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/single-nested-file.tar                 foo/fighter/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/single-nested-folder.tar               foo/fighter/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/nested-tar.tar                         foo/fighter/ufo
    2b87e29fca6ee7f1df6c1a76cb58e101 tests/nested-tar.tar                         foo/lighter.tar/fighter/bar
    2709a3348eb2c52302a7606ecf5860bc tests/directly-nested-tar.tar                fighter/ufo
    2b87e29fca6ee7f1df6c1a76cb58e101 tests/directly-nested-tar.tar                lighter.tar/fighter/bar
    2709a3348eb2c52302a7606ecf5860bc tests/nested-tar-with-overlapping-name.tar   foo/fighter/ufo
    2b87e29fca6ee7f1df6c1a76cb58e101 tests/nested-tar-with-overlapping-name.tar   foo/fighter.tar/fighter/bar
    2709a3348eb2c52302a7606ecf5860bc tests/hardlink.tar                           hardlink/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/hardlink.tar                           hardlink/natsu
    b3de7534cbc8b8a7270c996235d0c2da tests/concatenated.tar                       foo/fighter
    2709a3348eb2c52302a7606ecf5860bc tests/concatenated.tar                       foo/bar

    2709a3348eb2c52302a7606ecf5860bc tests/nested-symlinks.tar                    foo/foo
    2709a3348eb2c52302a7606ecf5860bc tests/nested-symlinks.tar                    foo/fighter/foo

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

    19696f24a91fc4e8950026f9c801a0d0 tests/simple.bz2                             simple
    19696f24a91fc4e8950026f9c801a0d0 tests/simple.gz                              simple
    19696f24a91fc4e8950026f9c801a0d0 tests/simple.xz                              simple
    19696f24a91fc4e8950026f9c801a0d0 tests/simple.zst                             simple
    2709a3348eb2c52302a7606ecf5860bc tests/file-existing-as-non-link-and-link.tar foo/fighter/ufo
    d3b07384d113edec49eaa6238ad5ff00 tests/two-self-links-to-existing-file.tar    bar

    c9172d469a8faf82fe598c0ce978fcea tests/base64.gz                              base64

    2709a3348eb2c52302a7606ecf5860bc tests/nested-directly-compressed.tar.bz2     directly-compressed/ufo.bz2/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/nested-directly-compressed.tar.bz2     directly-compressed/ufo.gz/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/nested-directly-compressed.tar.bz2     directly-compressed/ufo.xz/ufo

    c157a79031e1c40f85931829bc5fc552 tests/absolute-file-incremental.tar          14130612002/tmp/foo
    8ddd8be4b179a529afa5f2ffae4b9858 tests/incremental-backup.level.0.tar         root-file.txt
    5bbf5a52328e7439ae6e719dfe712200 tests/incremental-backup.level.0.tar         foo/1
    c193497a1a06b2c72230e6146ff47080 tests/incremental-backup.level.0.tar         foo/2
    febe6995bad457991331348f7b9c85fa tests/incremental-backup.level.0.tar         foo/3
    3d45efe945446cd53a944972bf60810c tests/incremental-backup.level.1.tar         foo/3
    5bbf5a52328e7439ae6e719dfe712200 tests/incremental-backup.level.1.tar         foo/moved
    c157a79031e1c40f85931829bc5fc552 tests/single-file-incremental-mockup.tar     14130613451/foo
    c157a79031e1c40f85931829bc5fc552 tests/single-file-incremental-long-name-mockup.tar 14130613451/000000000100000000020000000003000000000400000000050000000006000000000700000000080000000009000000000A000000000B000000000C
    c157a79031e1c40f85931829bc5fc552 tests/single-file-incremental-long-name.tar 000000000100000000020000000003000000000400000000050000000006000000000700000000080000000009000000000A000000000B000000000C

    832c78afcb9832e1a21c18212fc6c38b tests/gnu-sparse-files.tar                   01.sparse1.bin
    832c78afcb9832e1a21c18212fc6c38b tests/gnu-sparse-files.tar                   02.normal1.bin
    832c78afcb9832e1a21c18212fc6c38b tests/gnu-sparse-files.tar                   03.sparse1.bin

    b026324c6904b2a9cb4b88d6d61c81d1 tests/2k-recursive-tars.tar.bz2              mimi/00001.tar/foo
    3059b91c3562cd29457192eb3c3fe376 tests/2k-recursive-tars.tar.bz2              mimi/01234.tar.versions/1
    8f30b20831bade7a2236edf09a55af60 tests/2k-recursive-tars.tar.bz2              mimi/01333.tar/foo
    f95f8943f6dcf7b3c1c8c2cab5455f8b tests/2k-recursive-tars.tar.bz2              mimi/02000.tar/foo
    c157a79031e1c40f85931829bc5fc552 tests/2k-recursive-tars.tar.bz2              mimi/foo
)


for parallelization in 1 2 0; do

echo "== Testing with -P $parallelization =="
export parallelization

if [[ ! -f tests/2k-recursive-tars.tar ]]; then
    bzip2 -q -d -k tests/2k-recursive-tars.tar.bz2
fi

checkWriteOverlayWithNewFiles || returnError "$LINENO" 'Write overlay tests failed!'
checkWriteOverlayWithArchivedFiles || returnError "$LINENO" 'Write overlay tests for archive files failed!'

checkTruncated tests/truncated.tar foo/foo 5753d2a2da40d04ad7f3cc7a024b6e90

# GNU inremental TARs without directory entries cannot be reliably recognized as such. GNU TAR seems to use the
# heuristic of the prefix but the prefix can also be validly set without it being an incremental TAR.
# Note that bsdtar, which is based on libarchive, can somehow recognize this as a GNU tar and strips the prefix.
# Busyboxes TAR implementation does show the prefix for this file and if there are directory entries it will fail
# on the incremental directory type marker with: "tar: unknown typeflag: 0x44".
# This busybox behavior seems to support my understanding that without directory entries, incremental files behave
# like valid TARs and cannot be identified reliably as incremental TARs.
# TODO Look at the source code of bsdtar how it recognizes GNU incremental TARs
checkGnuIncremental tests/single-file-incremental.tar foo c157a79031e1c40f85931829bc5fc552
checkGnuIncremental tests/absolute-file-incremental.tar /tmp/foo c157a79031e1c40f85931829bc5fc552

checkIndexPathOption tests/single-file.tar bar d3b07384d113edec49eaa6238ad5ff00
checkIndexFolderFallback tests/single-file.tar bar d3b07384d113edec49eaa6238ad5ff00
checkIndexArgumentChangeDetection tests/single-file.tar bar d3b07384d113edec49eaa6238ad5ff00
checkSuffixStripping tests/2k-recursive-tars.tar mimi/00001/foo b026324c6904b2a9cb4b88d6d61c81d1
checkSuffixStripping tests/2k-recursive-tars.tar mimi/01234.tar 3059b91c3562cd29457192eb3c3fe376
checkNestedRecursiveFolderMounting tests/single-file.tar bar d3b07384d113edec49eaa6238ad5ff00

checkTarEncoding tests/single-file.tar utf-8 bar d3b07384d113edec49eaa6238ad5ff00
checkTarEncoding tests/single-file.tar latin1 bar d3b07384d113edec49eaa6238ad5ff00
checkTarEncoding tests/special-char.tar latin1 'Datei-mit-dämlicher-Kodierung.txt' 2709a3348eb2c52302a7606ecf5860bc
checkTarEncoding tests/nested-special-char.tar latin1 'Ördner-mìt-dämlicher-Ködierúng/Datei-mit-dämlicher-Kodierung.txt' 2709a3348eb2c52302a7606ecf5860bc

checkLinkInTAR tests/symlinks.tar foo ../foo
checkLinkInTAR tests/symlinks.tar python /usr/bin/python

checkFileInTARPrefix '' tests/single-nested-file.tar foo/fighter/ufo 2709a3348eb2c52302a7606ecf5860bc
checkFileInTARPrefix foo tests/single-nested-file.tar fighter/ufo 2709a3348eb2c52302a7606ecf5860bc
checkFileInTARPrefix foo/fighter tests/single-nested-file.tar ufo 2709a3348eb2c52302a7606ecf5860bc

checkAutomaticIndexRecreation || returnError "$LINENO" 'Automatic index recreation test failed!'
checkAutoMountPointCreation || returnError "$LINENO" 'Automatic mount point creation test failed!'
if ! uname | 'grep' -q -i darwin; then
    checkUnionMount || returnError "$LINENO" 'Union mounting test failed!'
fi
checkUnionMountFileVersions || returnError "$LINENO" 'Union mount file version access test failed!'

# These tests do not work on macOS. It seems that incomplete getattr calls are handled differently there.
# These TARs are pathological anyway. They self-link and no earlier actual versions of the same file exists,
# so it can't get any information about the file except for the link location. It's enough that ratarmount
# doesn't hang with 100% CPU time in these cases, which I tested manually on macOS.
if ! uname | 'grep' -q -i darwin; then
    checkSelfReferencingHardLinks tests/single-self-link.tar ||
        returnError "$LINENO" 'Self-referencing hardlinks test failed!'
    checkSelfReferencingHardLinks tests/two-self-links.tar ||
        returnError "$LINENO" 'Self-referencing hardlinks test failed!'
fi

for (( iTest = 0; iTest < ${#tests[@]}; iTest += 3 )); do
    checksum=${tests[iTest]}
    tarPath=${tests[iTest+1]}
    fileName=${tests[iTest+2]}

    # Only test some larger files for all compression backends because most of the files are minimal
    # tests which all have the same size of 20*512B. In the first place, the compression backends
    # should be tested more rigorously inside their respective projects not by ratarmount.
    if [[ "$fileName" =~ 2k-recursive ]]; then
        # readarray does not work on macOS!
        #readarray -t files < <( recompressFile "$tarPath" )
        files=()
        while IFS=$'\n' read -r line; do
            files+=( "$line" )
        done < <( recompressFile "$tarPath" || returnError "$LINENO" 'Something went wrong during recompression.' )
        TMP_FILES_TO_CLEANUP+=( "${files[@]}" )
    else
        files=( "$tarPath" )
    fi

    for file in "${files[@]}"; do
        case "$( file --mime-type -- "$file" | sed 's|.*[/-]||' )" in
            bzip2|gzip|xz|zstd|tar|rar|zip)
                TMP_FILES_TO_CLEANUP+=( "${file}.index.sqlite" )
                checkFileInTAR "$file" "$fileName" "$checksum"
                ;;
        esac
        (( ++nFiles ))
    done
    cleanup
    safeRmdir "$( dirname -- "$file" )"
done

checkRecursiveFolderMounting
checkRecursiveFolderMounting --lazy

rm -f tests/*.index.*
rmdir tests/*/

done  # for parallelization


benchmarkDecoderBackends


echo -e '\e[32mAll tests ran successfully.\e[0m'

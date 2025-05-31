#!/bin/bash

cd -- "$( dirname -- "${BASH_SOURCE[0]}" )/.." || { echo 'Failed to cd to git root!'; exit 1; }

#export PYTHONTRACEMALLOC=1

if [[ -z "$RATARMOUNT_CMD" ]]; then
    TEST_EXTERNAL_COMMAND=0
    # I don't see a way to call a non-installed module folder via an absolute path.
    # Therefore, this will use the non-installed 'ratarmount' module in the current folder if it exists,
    # and if the test did change directory, e.g., into a temporary folder, then ratarmount must have
    # been installed for this test to find it!
    RATARMOUNT_CMD="python3 -X dev -W ignore::DeprecationWarning -u -m ratarmount"
    #RATARMOUNT_CMD=ratarmount
else
    TEST_EXTERNAL_COMMAND=1
fi
RATARMOUNT_CMD="$RATARMOUNT_CMD --index-minimum-file-count 0"
export RATARMOUNT_CMD
echo "RATARMOUNT_CMD: $RATARMOUNT_CMD"

if [[ -z "$PARALLELIZATIONS" ]]; then
    PARALLELIZATIONS="1 2"
fi

python3MinorVersion=$( python3 -c 'import sys; print(sys.version_info.minor)' )

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
    for folder in "${MOUNT_POINTS_TO_CLEANUP[@]}"; do
        if [[ -d "$folder" ]]; then
            funmount "$folder"
        fi
    done
    for folder in "${MOUNT_POINTS_TO_CLEANUP[@]}"; do
        if [[ -d "$folder" ]]; then safeRmdir "$folder"; fi
    done
    MOUNT_POINTS_TO_CLEANUP=()

    # Remove things in reversed order so that deleting folders with rmdir succeeds after having deleted all files in it.
    local size=${#TMP_FILES_TO_CLEANUP[@]}
    for (( i = 0; i < size; ++i )); do
        file=${TMP_FILES_TO_CLEANUP[size - 1 - i]}
        if [ -d "$file" ]; then safeRmdir "$file"; fi
        if [ -f "$file" ]; then rm -- "$file"; fi
        if [ -L "$file" ]; then unlink -- "$file"; fi
        if [ -e "$file" ]; then echo "Failed to clean up: $file"; fi
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
        echoerr -e "\e[37mFile sum of '$fileInTar' ($checksum) in mounted TAR '$archive' does not match ($correctChecksum) when creating index"'!\e[0m'
        return 1
    fi
}

funmount()
{
    local mountFolder="$1"
    while mountpoint -- "$mountFolder" &>/dev/null; do
        $RATARMOUNT_CMD -u "$mountFolder"
        if mountpoint -- "$mountFolder" &>/dev/null; then
            sleep 0.1s
        fi
    done
}


waitForMountpoint()
{
    for (( i=0; i<10; ++i )); do
        if mountpoint -q -- "$1"; then break; fi
        sleep 1s
    done
    if ! mountpoint -q -- "$1"; then return 1; fi
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
    rm -f ratarmount.stderr.log
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
    rm -f ratarmount.stdout.log ratarmount.stderr.log
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

    local mountFolder
    mountFolder="$( mktemp -d --suffix .test.ratarmount )" ||
        returnError "$LINENO" 'Failed to create temporary directory'
    MOUNT_POINTS_TO_CLEANUP+=( "$mountFolder" )

    local args=()
    if [[ "$archive" != *"://"* ]]; then args+=( '--recursive' ); fi

    # try with index recreation
    args+=( -P "$parallelization" -c --detect-gnu-incremental --ignore-zeros "$archive" "$mountFolder" )
    {
        runAndCheckRatarmount "${args[@]}" &&
        checkStat "$mountFolder/$fileInTar" &&
        verifyCheckSum "$mountFolder" "$fileInTar" "$archive" "$correctChecksum"
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"
    funmount "$mountFolder"
    if [[ "$archive" =~ [.]tar ]]; then
        'grep' -q 'Creating offset dictionary' ratarmount.stdout.log ratarmount.stderr.log ||
            returnError "$LINENO" "Looks like index was not created while executing: $RATARMOUNT_CMD ${args[*]}"
    fi

    # retry without forcing index recreation
    args=()
    if [[ "$archive" != *"://"* ]]; then args+=( '--recursive' ); fi
    args+=( -P "$parallelization" --detect-gnu-incremental --ignore-zeros "$archive" "$mountFolder" )
    {
        runAndCheckRatarmount "${args[@]}" &&
        checkStat "$mountFolder/$fileInTar" &&
        verifyCheckSum "$mountFolder" "$fileInTar" "$archive" "$correctChecksum"
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"
    funmount "$mountFolder"

    # The libarchive backend does not create indexes for now because it doesn't help the poor performance much and
    # introduces complexity with index compatibility to other backends.
    if [[ "$archive" =~ [.]tar && ! "$archive" =~ [.]7z$ ]]; then
        'grep' -q 'Successfully loaded offset dictionary' ratarmount.stdout.log ratarmount.stderr.log ||
            returnError "$LINENO" "Looks like index was not loaded for '$archive' while executing: $RATARMOUNT_CMD ${args[*]}"
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

    local mountFolder
    mountFolder="$( mktemp -d --suffix .test.ratarmount )" ||
        returnError "$LINENO" 'Failed to create temporary directory'
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

    local mountFolder
    mountFolder="$( mktemp -d --suffix .test.ratarmount )" ||
        returnError "$LINENO" 'Failed to create temporary directory'
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

checkAutomaticIndexRecreation()
(
    tmpFolder="$( mktemp -d --suffix .test.ratarmount )" || returnError "$LINENO" 'Failed to create temporary directory'
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

    # 3. Change only the timestamp without changing the size and file metadata.
    #    (Luckily TAR is filled to 10240 Bytes anyways for very small files)
    #    modification timestamp detection is turned off for now by default to facilitate index sharing because
    #    the mtime check can prove problematic as the mtime changes when downloading a file.
    sleep 1 # because we are comparing timestamps with seconds precision ...
    touch -- "$archive"

    runAndCheckRatarmount "$archive"
    [[ $lastModification -eq $( getFileMtime "$indexFile" ) ]] ||
        returnError "$LINENO" 'Index changed even though TAR did not except for the modification timestamp!'
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
    testsFolder="$( pwd )/tests"
    tmpFolder="$( mktemp -d --suffix .test.ratarmount )" || returnError "$LINENO" 'Failed to create temporary directory'
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

    mountPoint=$( mktemp -d --suffix .test.ratarmount )
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

    funmount "$mountPoint"

    # Check whether union mounting of two folders with the same name works.
    mkdir -p "folder2/subfolder"
    echo 'hallo' > "folder2/subfolder/world-de"

    runAndCheckRatarmount -c "folder1/subfolder" "folder2/subfolder" "$mountPoint"

    checkStat "$mountPoint/world" || returnError "$LINENO" 'Could not stat file!'
    checkStat "$mountPoint/world-de" || returnError "$LINENO" 'Could not stat file!'

    verifyCheckSum "$mountPoint" "world" "<folder union mount>" b1946ac92492d2347c6235b4d2611184 ||
        returnError "$LINENO" 'Checksum mismatches!'
    verifyCheckSum "$mountPoint" "world-de" "<folder union mount>" aee97cb3ad288ef0add6c6b5b5fae48a ||
        returnError "$LINENO" 'Checksum mismatches!'

    # Clean up

    funmount "$mountPoint"
    safeRmdir "$mountPoint"
    cd .. || returnError "$LINENO" 'Could not cd to parent in order to clean up!'
    rm -rf -- "$tmpFolder" || returnError "$LINENO" 'Something went wrong. Should have been able to clean up!'

    echoerr "[${FUNCNAME[0]}] Tested successfully"
)

checkUnionMountFileVersions()
(
    testsFolder="$( pwd )/tests"
    tmpFolder="$( mktemp -d --suffix .test.ratarmount )" || returnError "$LINENO" 'Failed to create temporary directory'
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
    testsFolder="$( pwd )/tests"
    tmpFolder="$( mktemp -d --suffix .test.ratarmount )" || returnError "$LINENO" 'Failed to create temporary directory'
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

    local mountFolder
    mountFolder="$( mktemp -d --suffix .test.ratarmount )" || returnError "$LINENO" 'Failed to create temporary directory'
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

    local recompressedFiles=()
    local tmpFolder
    tmpFolder=$( mktemp -d --suffix .test.ratarmount )

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


checkIndexPathOption()
{
    # The --index-path should have highest priority, overwriting all --index-folders and default locations
    local archive="$1"; shift
    local fileInTar="$1"; shift
    local correctChecksum="$1"

    local mountFolder indexFolder indexFile
    mountFolder="$( mktemp -d --suffix .test.ratarmount )" || returnError "$LINENO" 'Failed to create temporary directory'
    indexFolder="$( mktemp -d --suffix .test.ratarmount )" || returnError "$LINENO" 'Failed to create temporary directory'
    indexFile="$indexFolder/ratarmount.index"
    MOUNT_POINTS_TO_CLEANUP+=( "$mountFolder" )
    TMP_FILES_TO_CLEANUP+=( "$indexFolder" "$indexFile" )

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

    local args mountFolder indexFolder
    mountFolder="$( mktemp -d --suffix .test.ratarmount )" || returnError "$LINENO" 'Failed to create temporary directory'
    indexFolder="$( mktemp -d --suffix .test.ratarmount )" || returnError "$LINENO" 'Failed to create temporary directory'
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
    args=( --index-folders '' --index-minimum-file-count 0 "$archive" "$mountFolder" )
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

    local args mountFolder indexFolder
    mountFolder="$( mktemp -d --suffix .test.ratarmount )" || returnError "$LINENO" 'Failed to create temporary directory'
    indexFolder="$( mktemp -d --suffix .test.ratarmount )" || returnError "$LINENO" 'Failed to create temporary directory'
    indexFile="$indexFolder/ratarmount.index"
    MOUNT_POINTS_TO_CLEANUP+=( "$mountFolder" )
    TMP_FILES_TO_CLEANUP+=( "$indexFolder" "$indexFile" )

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

    local mountFolder
    mountFolder="$( mktemp -d --suffix .test.ratarmount )" || returnError "$LINENO" 'Failed to create temporary directory'
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

    local archiveFolder mountFolder
    archiveFolder="$( mktemp -d --suffix .test.ratarmount )" || returnError "$LINENO" 'Failed to create temporary directory'
    mountFolder="$( mktemp -d --suffix .test.ratarmount )" || returnError "$LINENO" 'Failed to create temporary directory'
    MOUNT_POINTS_TO_CLEANUP+=( "$mountFolder" )

    local sourceFile targetFile
    for (( iTest = 0; iTest < ${#tests[@]}; iTest += 3 )); do
        sourceFile="${tests[iTest+1]}"
        targetFile="$archiveFolder/$( basename -- "$sourceFile" )"
        TMP_FILES_TO_CLEANUP+=( "$targetFile" "$targetFile.index.sqlite" )
        'cp' -- "$sourceFile" "$archiveFolder"
    done
    runAndCheckRatarmount -P "$parallelization" -c --detect-gnu-incremental --ignore-zeros --recursive \
        "$@" "$archiveFolder" "$mountFolder"

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

    local mountFolder archiveFolder
    archiveFolder="$( mktemp -d --suffix .test.ratarmount )" || returnError "$LINENO" 'Failed to create temporary directory'
    mountFolder="$( mktemp -d --suffix .test.ratarmount )" || returnError "$LINENO" 'Failed to create temporary directory'
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

    local mountFolder
    mountFolder="$( mktemp -d --suffix .test.ratarmount )" || returnError "$LINENO" 'Failed to create temporary directory'
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
        TMP_FILES_TO_CLEANUP+=( "$tmpCopy" )
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
        'rm' -- "$tmpCopy"
    fi
}

checkWriteOverlayFile()
{
    local fileSubPath="$1"
    local filePath="$mountFolder/$1"

    TMP_FILES_TO_CLEANUP+=( "$filePath" )

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

    local mountFolder
    mountFolder="$( mktemp -d --suffix .test.ratarmount )" || returnError "$LINENO" 'Failed to create temporary directory'
    MOUNT_POINTS_TO_CLEANUP+=( "$mountFolder" )

    local overlayFolder;
    overlayFolder=$( mktemp -d --suffix .test.ratarmount )
    TMP_FILES_TO_CLEANUP+=( "$overlayFolder" "$overlayFolder/.ratarmount.overlay.sqlite" )
    # Create the overlay folder on some filesystem, e.g., NTFS FUSE, which does not support
    # permission changes for testing the metadata database.
    #overlayFolder=$( mktemp -d --suffix .test.ratarmount -p "$( pwd )" )

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

    TMP_FILES_TO_CLEANUP+=(
        "$overlayFolder/base"
        "$overlayFolder/foo"
        "$overlayFolder/foo/base"
        "$overlayFolder/iriya"
        "$overlayFolder/sighting"
    )

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


    'rm' -r -- "$overlayFolder"
    cleanup

    echoerr "[${FUNCNAME[0]}] Tested successfully file modifications for overlay files."
}

checkWriteOverlayWithArchivedFiles()
{
    local archive='tests/nested-tar.tar'

    local mountFolder
    mountFolder="$( mktemp -d --suffix .test.ratarmount )" || returnError "$LINENO" 'Failed to create temporary directory'
    MOUNT_POINTS_TO_CLEANUP+=( "$mountFolder" )

    local overlayFolder;
    overlayFolder=$( mktemp -d --suffix .test.ratarmount )
    TMP_FILES_TO_CLEANUP+=( "$overlayFolder" "$overlayFolder/.ratarmount.overlay.sqlite" )

    local args=( -P "$parallelization" -c --write-overlay "$overlayFolder" "$archive" "$mountFolder" )
    {
        runAndCheckRatarmount "${args[@]}"
        if [[ -z "$( find "$mountFolder" -mindepth 1 2>/dev/null )" ]]; then
            returnError "$LINENO" 'Expected files in mount point'
        fi
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
        if [[ -z "$( find "$mountFolder" -mindepth 1 2>/dev/null )" ]]; then
            returnError "$LINENO" 'Expected files in mount point'
        fi
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
        if [[ -z "$( find "$mountFolder" -mindepth 1 2>/dev/null )" ]]; then
            returnError "$LINENO" 'Expected files in mount point'
        fi
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


checkWriteOverlayWithSymbolicLinks()
{
    local mountFolder
    mountFolder="$( mktemp -d --suffix .test.ratarmount )" || returnError "$LINENO" 'Failed to create temporary directory'
    MOUNT_POINTS_TO_CLEANUP+=( "$mountFolder" )

    # Create test folder structure like this:
    # /tmp
    # +- tmp1/
    # |  +- overlay2 -> ../tmp2
    # |  +- bar -> ../tmp2/bar
    # +- tmp2/
    #    +- bar
    local overlayFolder overlayFolder2;
    overlayFolder=$( mktemp -d --suffix .test.ratarmount )
    overlayFolder2=$( mktemp -d --suffix .test.ratarmount )
    ( cd -- "$overlayFolder" && ln -s "$( realpath --relative-to "$overlayFolder" "$overlayFolder2" )" overlay2 )
    echo foo > "${overlayFolder2}/bar"
    ( cd -- "$overlayFolder" && ln -s "$( realpath --relative-to "$overlayFolder" "$overlayFolder2/bar" )" bar )

    TMP_FILES_TO_CLEANUP+=(
        "$overlayFolder"
        "$overlayFolder2"
        "${overlayFolder2}/overlay2"
        "${overlayFolder2}/overlay2/foo"
        "${overlayFolder2}/bar"
    )

    local args=( -P "$parallelization" -c --write-overlay "$overlayFolder" "$overlayFolder2" "$mountFolder" )
    {
        runAndCheckRatarmount "${args[@]}"
        if [[ -z "$( find "$mountFolder" -mindepth 1 2>/dev/null )" ]]; then
            returnError "$LINENO" 'Expected files in mount point'
        fi
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"

    verifyCheckSum "$mountFolder" 'overlay2/bar' '[write overlay]' d3b07384d113edec49eaa6238ad5ff00 ||
        returnError "$LINENO" 'Failed to read linked file via mount point'

    # Create file in relative symbolic link to cousin folder.
    echo bar > "${mountFolder}/overlay2/foo"

    verifyCheckSum "$mountFolder" 'overlay2/foo' '[write overlay]' c157a79031e1c40f85931829bc5fc552 ||
        returnError "$LINENO" 'Failed to create file in write overlay'
    verifyCheckSum "$overlayFolder" 'overlay2/foo' '[write overlay]' c157a79031e1c40f85931829bc5fc552 ||
        returnError "$LINENO" 'Failed to create file in write overlay'
    verifyCheckSum "$overlayFolder2" 'foo' '[write overlay]' c157a79031e1c40f85931829bc5fc552 ||
        returnError "$LINENO" 'Failed to create file in write overlay'

    echoerr "[${FUNCNAME[0]}] Tested successfully writes to symbolically linked folders in the overlay."

    'rm' -r -- "$overlayFolder" "$overlayFolder2"
    cleanup
}


checkWriteOverlayCommitDelete()
{
    local tmpFolder;
    tmpFolder="$( mktemp -d --suffix .test.ratarmount )" || returnError "$LINENO" 'Failed to create temporary directory'
    mkdir "$tmpFolder/overlay"
    TMP_FILES_TO_CLEANUP+=( "$tmpFolder" )

    local archive='single-nested-folder.tar'
    cp "tests/$archive" "$tmpFolder/"
    archive="$tmpFolder/$archive"
    TMP_FILES_TO_CLEANUP+=( "$archive" "$archive.index.sqlite" )

    [[ $( tar -tvlf "$archive" | wc -l ) -eq 2 ]] || returnError "$LINENO" 'Expected two entries in TAR'

    local mountFolder="$tmpFolder/mounted"
    mkdir "$mountFolder"
    MOUNT_POINTS_TO_CLEANUP+=( "$mountFolder" )

    local overlayFolder="$tmpFolder/overlay"
    TMP_FILES_TO_CLEANUP+=( "$overlayFolder" "$overlayFolder/.ratarmount.overlay.sqlite" )

    local args=( -P "$parallelization" -c --write-overlay "$overlayFolder" "$archive" "$mountFolder" )
    {
        runAndCheckRatarmount "${args[@]}"
        if [[ -z "$( find "$mountFolder" -mindepth 1 2>/dev/null )" ]]; then
            returnError "$LINENO" 'Expected files in mount point'
        fi
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"

    verifyCheckSum "$mountFolder" 'foo/fighter/ufo' 'tests/single-nested-folder.tar' 2709a3348eb2c52302a7606ecf5860bc ||
        returnError "$LINENO" 'Mismatching checksum'

    # Delete file
    'rm' "$mountFolder/foo/fighter/ufo" || returnError "$LINENO" 'Failed to delete ufo file'
    overlayIndex="$overlayFolder/.ratarmount.overlay.sqlite"
    [[ -f "$overlayIndex" ]] || returnError "$LINENO" "Expected $overlayIndex to be created"

    funmount "$mountFolder"

    args=( --commit-overlay "${args[@]}" )
    {
        echo commit | $RATARMOUNT_CMD "${args[@]}" >ratarmount.stdout.log 2>ratarmount.stderr.log.tmp
        ! 'grep' -C 5 -Ei '(warn|error)' ratarmount.stdout.log ratarmount.stderr.log ||
            returnError "$LINENO" "Found warnings while executing: $RATARMOUNT_CMD ${args[*]}"
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"

    tar -tvlf "$archive"
    [[ $( tar -tvlf "$archive" | wc -l ) -eq 1 ]] || returnError "$LINENO" 'Expected one less entry in TAR'

    cleanup

    echoerr "[${FUNCNAME[0]}] Tested successfully file modifications for overlay files."
}


checkSymbolicLinkRecursion()
{
    local mountFolder
    mountFolder="$( mktemp -d --suffix .test.ratarmount )" || returnError "$LINENO" 'Failed to create temporary directory'
    MOUNT_POINTS_TO_CLEANUP+=( "$mountFolder" )

    # Create test folder structure like in issue 102 with a mix of relative and absolute links to test both:
    # /tmp
    # +- collections/
    # |  +- part1.gz -> /tmp/downloads/file.gz
    # +- downloads
    # |  +- file.gz -> ../datastore/01234567
    # +- datastore/
    #    +- 01234567
    local folder;
    folder=$( mktemp -d --suffix .test.ratarmount )
    TMP_FILES_TO_CLEANUP+=(
        "$folder" "$folder/datastorage" "$folder/downloads" "$folder/collections"
        "$folder/datastorage/01234567" "$folder/downloads/file.gz" "$folder/collections/part1.gz"
    )
    (
        cd -- "$folder" &&
        mkdir datastorage downloads collections &&
        echo 'bar' | gzip > datastorage/01234567 &&
        ( cd downloads && ln -s ../datastorage/01234567 file.gz ) &&
        ( cd collections && ln -s "$folder/downloads/file.gz" part1.gz )
    )

    local args=( -P "$parallelization" -c -r -l "$folder/collections" "$mountFolder" )
    {
        runAndCheckRatarmount "${args[@]}"
        if [[ -z "$( find "$mountFolder" -mindepth 1 2>/dev/null )" ]]; then
            returnError "$LINENO" 'Expected files in mount point'
        fi
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"

    verifyCheckSum "$mountFolder" 'part1.gz/part1' '[write overlay]' c157a79031e1c40f85931829bc5fc552 ||
        returnError "$LINENO" 'Failed to read linked file via mount point'

    echoerr "[${FUNCNAME[0]}] Tested successfully recursive mounting of symbolical links to archives."

    'rm' -r -- "$folder"
}


checkGnuIncremental()
{
    local archive="$1"; shift
    local fileInTar="$1"; shift
    local correctChecksum="$1"

    local mountFolder
    mountFolder="$( mktemp -d --suffix .test.ratarmount )" || returnError "$LINENO" 'Failed to create temporary directory'
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

    local mountFolder
    mountFolder="$( mktemp -d --suffix .test.ratarmount )" || returnError "$LINENO" 'Failed to create temporary directory'
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


getBlockSize()
{
    python3 -c 'import os, sys; print(os.statvfs(sys.argv[1]))' "$1"
}


checkStatfs()
{
    local mountFolder
    mountFolder="$( mktemp -d --suffix .test.ratarmount )" || returnError "$LINENO" 'Failed to create temporary directory'
    MOUNT_POINTS_TO_CLEANUP+=( "$mountFolder" )

    local overlayFolder
    overlayFolder=$( mktemp -d --suffix .test.ratarmount )
    echo 'foo' > "$overlayFolder/bar"

    local args=( -P "$parallelization" -c "$overlayFolder" "$mountFolder" )
    {
        runAndCheckRatarmount "${args[@]}"
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"

    getBlockSize "$mountFolder" >/dev/null || returnError "$LINENO" 'Failed to create file in write overlay'
    getBlockSize "$mountFolder/bar" >/dev/null || returnError "$LINENO" 'Failed to create file in write overlay'

    echoerr "[${FUNCNAME[0]}] Tested successfully statfs to mounted folder."

    cleanup
    'rm' -r -- "$overlayFolder"

    return 0
}


checkStatfsWriteOverlay()
{
    local mountFolder
    mountFolder="$( mktemp -d --suffix .test.ratarmount )" || returnError "$LINENO" 'Failed to create temporary directory'
    MOUNT_POINTS_TO_CLEANUP+=( "$mountFolder" )

    local overlayFolder overlayFolder2
    overlayFolder=$( mktemp -d --suffix .test.ratarmount )
    overlayFolder2=$( mktemp -d --suffix .test.ratarmount )
    echo 'foo' > "$overlayFolder2/bar"

    local args=( -P "$parallelization" -c --write-overlay "$overlayFolder" "$overlayFolder2" "$mountFolder" )
    {
        runAndCheckRatarmount "${args[@]}"
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"

    getBlockSize "$mountFolder" >/dev/null || returnError "$LINENO" 'Failed to create file in write overlay'
    getBlockSize "$mountFolder/bar" >/dev/null || returnError "$LINENO" 'Failed to create file in write overlay'

    echoerr "[${FUNCNAME[0]}] Tested successfully statfs to mounted folder with write overlay."

    cleanup
    'rm' -r -- "$overlayFolder" "$overlayFolder2"

    return 0
}


checkExtendedAttributes()
{
    local mountFolder
    mountFolder="$( mktemp -d --suffix .test.ratarmount )" ||
        returnError "$LINENO" 'Failed to create temporary directory'
    MOUNT_POINTS_TO_CLEANUP+=( "$mountFolder" )

    local folder1 folder2 expectedOutput
    folder1=$( mktemp -d --suffix .test.ratarmount )
    folder2=$( mktemp -d --suffix .test.ratarmount )
    TMP_FILES_TO_CLEANUP+=( "$folder1" "$folder2" "$folder1/bar" "$folder2/foo" )
    echo 'foo' > "$folder1/bar"
    echo 'bar' > "$folder2/foo"
    setfattr -n 'user.tags' -v 'bar' "$folder1/bar"
    setfattr -n 'user.tags' -v 'foo' "$folder2/foo"

    local args=(
        -P "$parallelization" -c --disable-union-mount
        "$folder1" "$folder2" tests/file-with-attribute.{bsd,gnu}.tar.bz2 "$mountFolder"
    )
    {
        runAndCheckRatarmount "${args[@]}"
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"

    expectedOutput=$( mktemp )
    cat <<EOF > "$expectedOutput"
user.tags="bar"
user.tags="foo"
user.tags="mytag"
user.tags="mytag"
EOF
    diff <( getfattr --dump -R -- "$mountFolder" | sed '/^#/d; /^$/d' ) <( sed '/^#/d; /^$/d' "$expectedOutput" ) ||
        returnError "$LINENO" 'Mismatching extended attributes'

    'rm' -f -- "$expectedOutput"

    echoerr "[${FUNCNAME[0]}] Tested successfully extended file attributes."

    cleanup

    return 0
}


checkURLProtocolFile()
{
    checkFileInTAR 'file://tests/single-file.tar' bar d3b07384d113edec49eaa6238ad5ff00 ||
        returnError "$LINENO" 'Failed to read via file:// protocol'
    echo checkFileInTAR 'file://tests/' single-file.tar 1a28538854d1884e4415cb9bfb7a2ad8
    checkFileInTAR 'file://tests/' single-file.tar 1a28538854d1884e4415cb9bfb7a2ad8 ||
        returnError "$LINENO" 'Failed to read via file:// protocol'
    checkFileInTAR 'file://tests' single-file.tar 1a28538854d1884e4415cb9bfb7a2ad8 ||
        returnError "$LINENO" 'Failed to read via file:// protocol'
}


checkFileInTARForeground()
{
    # Similar to checkFileInTAR but calls ratarmount with -f as is necessary for some threaded fsspec backends.
    # TODO make those fsspec backends work without -f, e.g., by only mounting them in FuseMount.init, maybe
    #      trying to open in __init__ and close them at the end of __init__ and reopen them in init for better
    #      error reporting, or even better, somehow find out how to close only those threads and restart them
    #      in FuseMount.init.
    local archive="$1"; shift
    local fileInTar="$1"; shift
    local correctChecksum="$1"

    local startTime
    startTime=$( date +%s )

    rm -f ratarmount.{stdout,stderr}.log

    local mountFolder
    mountFolder="$( mktemp -d --suffix .test.ratarmount )" ||
        returnError "$LINENO" 'Failed to create temporary directory'
    MOUNT_POINTS_TO_CLEANUP+=( "$mountFolder" )

    $RATARMOUNT_CMD -c -f -d 3 "$archive" "$mountFolder" >ratarmount.stdout.log 2>ratarmount.stderr.log &
    waitForMountpoint "$mountFolder" || returnError 'Waiting for mountpoint timed out!'
    ! 'grep' -C 5 -Ei '(warn|error)' ratarmount.stdout.log ratarmount.stderr.log ||
        returnError "$LINENO" "Found warnings while executing: $RATARMOUNT_CMD $*"

    echo "Check access to $archive"
    verifyCheckSum "$mountFolder" "$fileInTar" "$archive" "$correctChecksum" || returnError "$LINENO" 'Checksum mismatches!'
    funmount "$mountFolder"

    safeRmdir "$mountFolder"
}


checkURLProtocolHTTP()
{
    # With Ubuntu 22.04 and Python 3.7 I get this error: "The HTTP server doesn't appear to support range requests."
    if [[ -n "$python3MinorVersion" && "$python3MinorVersion" -le 8 ]]; then
        return 0
    fi

    local pid mountPoint protocol port
    mountPoint=$( mktemp -d --suffix .test.ratarmount )
    protocol='http'
    port=8000

    # Failed alternatives to set up a test HTTP server:
    #     python3 -m http.server -b 127.0.0.1 8000 &  # Does not support range requests
    #     python3 -m RangeHTTPServer -b 127.0.0.1 8000 &  # Client has spurious errors every 5th test or so with this.
    #         TODO Debug this... Bug could be in fsspec/implementations/http.py, aiohttp, RangeHTTPServer, ...
    #     sudo apt install busybox-static
    #     busybox httpd -f -p 8000 &  # Does not support range requests.
    # sudo apt install ruby-webrick
    if ! command -v ruby &>/dev/null; then
        echo "Ruby not found. Please install ruby-webrick."
        return 0
    fi
    ruby -run -e httpd --version || returnError "$LINENO" 'Failed to start up ruby HTTP test server!'
    ruby -run -e httpd . --port $port --bind-address=127.0.0.1 1>'httpd-ruby-webrick.log' 2>&1 &
    pid=$!
    sleep 10
    cat httpd-ruby-webrick.log
    wget -O /dev/null 127.0.0.1:$port


    checkFileInTARForeground "$protocol://127.0.0.1:$port/tests/single-file.tar" 'bar' d3b07384d113edec49eaa6238ad5ff00 ||
        returnError "$LINENO" 'Failed to read from HTTP server'
    checkFileInTARForeground "$protocol://127.0.0.1:$port/tests/" 'single-file.tar' 1a28538854d1884e4415cb9bfb7a2ad8 ||
        returnError "$LINENO" 'Failed to read from HTTP server'
    checkFileInTARForeground "$protocol://127.0.0.1:$port/tests" 'single-file.tar' 1a28538854d1884e4415cb9bfb7a2ad8 ||
        returnError "$LINENO" 'Failed to read from HTTP server'

    kill $pid &>/dev/null
    rmdir "$mountPoint"
}


checkURLProtocolFTP()
{
    # Because I am not able to suppress the FTP fsspec warning:
    # /opt/hostedtoolcache/Python/3.8.18/x64/lib/python3.8/site-packages/fsspec/implementations/ftp.py:87:
    #   UserWarning: `encoding` not supported for python<3.9, ignoring
    if [[ -n "$python3MinorVersion" && "$python3MinorVersion" -lt 9 ]]; then
        return 0
    fi

    local pid user password
    # python3 -m pip install pyftpdlib pyopenssl>=23
    user='pqvFUMqbqp'
    password='ioweb123GUIweb'
    port=8021
    echo "Starting FTP server..."
    python3 -m pyftpdlib  --user="$user" --password="$password" --port "$port" --interface 127.0.0.1 &
    pid=$!
    sleep 2s
    wget -O /dev/null "ftp://$user:$password@127.0.0.1:$port/tests/single-file.tar"

    checkFileInTAR "ftp://$user:$password@127.0.0.1:$port/tests/single-file.tar" bar d3b07384d113edec49eaa6238ad5ff00 ||
        returnError "$LINENO" 'Failed to read from FTP server'
    checkFileInTAR "ftp://$user:$password@127.0.0.1:$port/tests/" single-file.tar 1a28538854d1884e4415cb9bfb7a2ad8 ||
        returnError "$LINENO" 'Failed to read from FTP server'
    checkFileInTAR "ftp://$user:$password@127.0.0.1:$port/tests" single-file.tar 1a28538854d1884e4415cb9bfb7a2ad8 ||
        returnError "$LINENO" 'Failed to read from FTP server'

    # Check remote and/or compressed indexes.

    local archive='tests/single-file.tar'
    local fileInTar='bar'
    local correctChecksum='d3b07384d113edec49eaa6238ad5ff00'
    local mountFolder
    mountFolder="$( mktemp -d --suffix .test.ratarmount )" ||
        returnError "$LINENO" 'Failed to create temporary directory'

    runAndCheckRatarmount --recreate-index "$archive" "$mountFolder"
    [[ -f 'tests/single-file.tar.index.sqlite' ]] || returnError "$LINENO" 'Expected index to have been created!'
    mv 'tests/single-file.tar.index.sqlite' 'remote.index.sqlite'
    funmount "$mountFolder"

    # Test remote uncompressed index

    local indexFile="ftp://$user:$password@127.0.0.1:$port/remote.index.sqlite"
    echo "Checking with remote uncompressed index: $indexFile ..."
    args=( --index-file "$indexFile" "$archive" "$mountFolder" )
    {
        runAndCheckRatarmount "${args[@]}" &&
        checkStat "$mountFolder/$fileInTar" &&
        verifyCheckSum "$mountFolder" "$fileInTar" "$archive" "$correctChecksum"
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"
    funmount "$mountFolder"

    'grep' -q 'Successfully loaded offset dictionary' ratarmount.stdout.log ratarmount.stderr.log ||
        returnError "$LINENO" "Index was not loaded for '$archive' while executing: $RATARMOUNT_CMD ${args[*]}"
    [[ ! -f 'tests/single-file.tar.index.sqlite' ]] || returnError "$LINENO" 'Index should not have been recreated!'

    # Test local compressed index

    gzip -f 'remote.index.sqlite'
    [[ ! -f 'remote.index.sqlite' ]] || returnError "$LINENO" 'Index should not have been deleted after compression!'
    indexFile='remote.index.sqlite.gz'
    [[ -f "$indexFile" ]] || returnError "$LINENO" 'Index should not have been compressed!'
    echo "Checking with local compressed index: $indexFile ..."
    args=( --index-file "$indexFile" "$archive" "$mountFolder" )
    {
        runAndCheckRatarmount "${args[@]}" &&
        checkStat "$mountFolder/$fileInTar" &&
        verifyCheckSum "$mountFolder" "$fileInTar" "$archive" "$correctChecksum"
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"
    funmount "$mountFolder"

    'grep' -q 'Successfully loaded offset dictionary' ratarmount.stdout.log ratarmount.stderr.log ||
        returnError "$LINENO" "Index was not loaded for '$archive' while executing: $RATARMOUNT_CMD ${args[*]}"
    [[ ! -f 'tests/single-file.tar.index.sqlite' ]] || returnError "$LINENO" 'Index should not have been recreated!'

    # Test remote compressed index

    indexFile="ftp://$user:$password@127.0.0.1:$port/remote.index.sqlite.gz"
    echo "Checking with remote compressed index: $indexFile ..."
    args=( --index-file "$indexFile" "$archive" "$mountFolder" )
    {
        runAndCheckRatarmount "${args[@]}" &&
        checkStat "$mountFolder/$fileInTar" &&
        verifyCheckSum "$mountFolder" "$fileInTar" "$archive" "$correctChecksum"
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"
    funmount "$mountFolder"

    'grep' -q 'Successfully loaded offset dictionary' ratarmount.stdout.log ratarmount.stderr.log ||
        returnError "$LINENO" "Index was not loaded for '$archive' while executing: $RATARMOUNT_CMD ${args[*]}"
    [[ ! -f 'tests/single-file.tar.index.sqlite' ]] || returnError "$LINENO" 'Index should not have been recreated!'

    # Test with URL chaining remote index

    indexFile=''
    tar -cf remote.index.tar remote.index.sqlite.gz
    [[ -f remote.index.tar ]] || returnError "$LINENO" 'Index should not have been archived!'
    indexFile="file://remote.index.sqlite.gz::tar://::ftp://$user:$password@127.0.0.1:$port/remote.index.tar"
    echo "Checking with URL-chained remote compressed index: $indexFile ..."
    args=( --index-file "$indexFile" "$archive" "$mountFolder" )
    {
        runAndCheckRatarmount "${args[@]}" &&
        checkStat "$mountFolder/$fileInTar" &&
        verifyCheckSum "$mountFolder" "$fileInTar" "$archive" "$correctChecksum"
    } || returnError "$LINENO" "$RATARMOUNT_CMD ${args[*]}"
    funmount "$mountFolder"

    'grep' -q 'Successfully loaded offset dictionary' ratarmount.stdout.log ratarmount.stderr.log ||
        returnError "$LINENO" "Index was not loaded for '$archive' while executing: $RATARMOUNT_CMD ${args[*]}"
    [[ ! -f 'tests/single-file.tar.index.sqlite' ]] || returnError "$LINENO" 'Index should not have been recreated!'

    # Clean up

    rm -f remote.index*
    kill $pid
}


killRogueSSH()
{
    local pid
    for pid in $( pgrep -f start-asyncssh-server ) $( pgrep -f ssh:// ); do
        kill "$pid"
        sleep 1
        kill -9 "$pid"
    done
    sleep 1
}


checkURLProtocolSSHErrorOnPython314()
{
    cat <<EOF >/dev/null
Traceback (most recent call last):
  File ".../python3.14/site-packages/ratarmountcore/factory.py", line 180, in openFsspec
    elif openFile.fs.isdir(openFile.path):
         ~~~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^
  File ".../python3.14/site-packages/fsspec/asyn.py", line 118, in wrapper
    return sync(self.loop, func, *args, **kwargs)
  File ".../python3.14/site-packages/fsspec/asyn.py", line 103, in sync
    raise return_result
  File ".../python3.14/site-packages/fsspec/asyn.py", line 56, in _runner
    result[0] = await coro
                ^^^^^^^^^^
  File ".../python3.14/site-packages/fsspec/asyn.py", line 677, in _isdir
    return (await self._info(path))["type"] == "directory"
            ^^^^^^^^^^^^^^^^^^^^^^
  File ".../python3.14/site-packages/sshfs/utils.py", line 27, in wrapper
    return await func(*args, **kwargs)
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File ".../python3.14/site-packages/sshfs/spec.py", line 142, in _info
    attributes = await channel.stat(path)
                 ^^^^^^^^^^^^^^^^^^^^^^^^
  File ".../python3.14/site-packages/asyncssh/sftp.py", line 4616, in stat
    return await self._handler.stat(path, flags,
           ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                                    follow_symlinks=follow_symlinks)
                                    ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File ".../python3.14/site-packages/asyncssh/sftp.py", line 2713, in stat
    return cast(SFTPAttrs,  await self._make_request(
                            ^^^^^^^^^^^^^^^^^^^^^^^^^
        FXP_STAT, String(path), flag_bytes))
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File ".../python3.14/site-packages/asyncssh/sftp.py", line 2468, in _make_request
    result = self._packet_handlers[resptype](self, resp)
  File ".../python3.14/site-packages/asyncssh/sftp.py", line 2484, in _process_status
    raise exc
asyncssh.sftp.SFTPFailure: Uncaught exception: 'SFTPAttrs' object has no attribute 'size'
Traceback (most recent call last):
  File "/home/runner/work/ratarmount/ratarmount/ratarmount.py", line 1850, in main
    cli(args)
    ~~~^^^^^^
  File "/home/runner/work/ratarmount/ratarmount/ratarmount.py", line 1794, in cli
    with FuseMount(
         ~~~~~~~~~^
        # fmt: off
        ^^^^^^^^^^
    ...<27 lines>...
        # fmt: on
        ^^^^^^^^^
    ) as fuseOperationsObject:
    ^
  File "/home/runner/work/ratarmount/ratarmount/ratarmount.py", line 570, in __init__
    mountSources.append((os.path.basename(path), openMountSource(path, **options)))
                                                 ~~~~~~~~~~~~~~~^^^^^^^^^^^^^^^^^
  File ".../python3.14/site-packages/ratarmountcore/factory.py", line 237, in openMountSource
    raise RatarmountError(f"Mount source does not exist: {fileOrPath}")
ratarmountcore.utils.RatarmountError: Mount source does not exist: ssh://127.0.0.1:8022/tests/single-file.tar
EOF
}


checkURLProtocolSSH()
{
    if [[ -n "$python3MinorVersion" && "$python3MinorVersion" -ge 14 ]]; then
        return 0
    fi

    local pid fingerprint publicKey mountPoint port file
    # rm -f ssh_host_key; ssh-keygen -q -N "" -C "" -t ed25519 -f ssh_host_key
    cat <<EOF > ssh_host_key
-----BEGIN OPENSSH PRIVATE KEY-----
b3BlbnNzaC1rZXktdjEAAAAABG5vbmUAAAAEbm9uZQAAAAAAAAABAAAAMwAAAAtzc2gtZW
QyNTUxOQAAACA6luxe0F9n0zBbFW6DExxYAMz2tinaHPb9IwLmreJMzgAAAIhe3ftsXt37
bAAAAAtzc2gtZWQyNTUxOQAAACA6luxe0F9n0zBbFW6DExxYAMz2tinaHPb9IwLmreJMzg
AAAECRurZq3m4qFnBUpJG3+SwhdL410zFoUODgRIU4aLTbpjqW7F7QX2fTMFsVboMTHFgA
zPa2Kdoc9v0jAuat4kzOAAAAAAECAwQF
-----END OPENSSH PRIVATE KEY-----
EOF
    # Only works on server. Also not hashed in not in known_hosts format.
    #fingerprint=$( ssh-keygen -lf ssh_host_key )
    fingerprint=$( ssh-keyscan -H -p 8022 127.0.0.1 2>/dev/null )
    file="$HOME/.ssh/known_hosts"
    mkdir -p -- "$HOME/.ssh/"
    if [[ ! -f "$file" ]] || ! 'grep' -q -F "$fingerprint" "$file"; then
        echo "$fingerprint" >> "$file"
    fi

    [[ -f ~/.ssh/id_ed25519 ]] || ssh-keygen -q -N "" -t ed25519 -f ~/.ssh/id_ed25519
    publicKey=$( cat ~/.ssh/id_ed25519.pub )
    file='ssh_user_ca'
    if [[ ! -f "$file" ]] || ! 'grep' -q -F "$publicKey" "$file"; then
        echo "$publicKey" >> "$file"
    fi

    killRogueSSH
    port=8022
    python3 tests/start-asyncssh-server.py &
    pid=$!
    echo "Started SSH server with process ID $pid"
    sleep 2

    mountPoint=$( mktemp -d --suffix .test.ratarmount )

    checkFileInTARForeground "ssh://127.0.0.1:$port/tests/single-file.tar" 'bar' d3b07384d113edec49eaa6238ad5ff00 ||
        returnError "$LINENO" 'Failed to read from SSH server'
    checkFileInTARForeground "ssh://127.0.0.1:$port/tests/" 'single-file.tar' 1a28538854d1884e4415cb9bfb7a2ad8 ||
        returnError "$LINENO" 'Failed to read from SSH server'
    checkFileInTARForeground "ssh://127.0.0.1:$port/tests" 'single-file.tar' 1a28538854d1884e4415cb9bfb7a2ad8 ||
        returnError "$LINENO" 'Failed to read from SSH server'

    kill $pid
    killRogueSSH
    rmdir "$mountPoint"
}


checkURLProtocolGit()
{
    # Pygit2 is missing wheels for Python 3.13
    # The manual installation from source fails because of:
    #   File "/opt/hostedtoolcache/Python/3.13.0-rc.3/x64/lib/python3.13/site-packages/pygit2/__init__.py",
    #       line 32, in <module>
    #   from ._pygit2 import *
    #       ImportError: libgit2.so.1.7: cannot open shared object file: No such file or directory
    # Even though the compilation was fine and the installation also looks fine:
    #   Install the project...
    #   -- Install configuration: "Debug"
    #   -- Installing: /usr/local/lib/pkgconfig/libgit2.pc
    #   -- Installing: /usr/local/lib/libgit2.so.1.7.2
    #   -- Installing: /usr/local/lib/libgit2.so.1.7
    #   -- Installing: /usr/local/lib/libgit2.so
    #   -- Installing: /usr/local/include/git2
    if ! python3 -c 'import pygit2; pygit2.enums.FileMode' &>/dev/null; then
        return 0
    fi

    # https://github.com/fsspec/filesystem_spec/blob/360e46d13069b0426565429f9f610bf704cfa062/
    # fsspec/implementations/git.py#L28C14-L28C58
    # > "git://[path-to-repo[:]][ref@]path/to/file" (but the actual
    # > file path should not contain "@" or ":").
    checkFileInTAR 'git://v0.15.2@tests/single-file.tar' bar d3b07384d113edec49eaa6238ad5ff00 ||
        returnError "$LINENO" 'Failed to read from HTTP server'
    checkFileInTAR 'git://v0.15.2@tests/' single-file.tar 1a28538854d1884e4415cb9bfb7a2ad8 ||
        returnError "$LINENO" 'Failed to read from HTTP server'
    checkFileInTAR 'git://v0.15.2@tests' single-file.tar 1a28538854d1884e4415cb9bfb7a2ad8 ||
        returnError "$LINENO" 'Failed to read from HTTP server'
}


checkURLProtocolGithub()
{
    # Cannot do automated tests because of Github rate limit...
    #     Trying to open with fsspec raised an exception: 403 Client Error: rate limit exceeded for url
    if [[ -n "$CI" ]]; then return 0; fi

    # https://github.com/fsspec/filesystem_spec/blob/360e46d13069b0426565429f9f610bf704cfa062/
    #   fsspec/implementations/github.py#L26
    # https://github.com/fsspec/filesystem_spec/blob/360e46d13069b0426565429f9f610bf704cfa062/
    #   fsspec/implementations/github.py#L202
    # https://github.com/fsspec/filesystem_spec/blob/360e46d13069b0426565429f9f610bf704cfa062/fsspec/utils.py#L37
    #
    # - "github://path/file", in which case you must specify org, repo and
    #   may specify sha in the extra args
    # - 'github://org:repo@/precip/catalog.yml', where the org and repo are
    #   part of the URI
    # - 'github://org:repo@sha/precip/catalog.yml', where the sha is also included
    #
    # ``sha`` can be the full or abbreviated hex of the commit you want to fetch
    # from, or a branch or tag name (so long as it doesn't contain special characters
    # like "/", "?", which would have to be HTTP-encoded).

    checkFileInTAR 'github://mxmlnkn:ratarmount@v0.15.2/tests/single-file.tar' bar d3b07384d113edec49eaa6238ad5ff00 ||
        returnError "$LINENO" 'Failed to read from HTTP server'
    checkFileInTAR 'github://mxmlnkn:ratarmount@v0.15.2/tests/' single-file.tar 1a28538854d1884e4415cb9bfb7a2ad8 ||
        returnError "$LINENO" 'Failed to read from HTTP server'
    checkFileInTAR 'github://mxmlnkn:ratarmount@v0.15.2/tests' single-file.tar 1a28538854d1884e4415cb9bfb7a2ad8 ||
        returnError "$LINENO" 'Failed to read from HTTP server'
}


checkURLProtocolS3()
{
    # Traceback (most recent call last):
    #   File "/home/runner/work/ratarmount/ratarmount/ratarmount.py", line 1928, in <module>
    #     main()
    #   File "/home/runner/work/ratarmount/ratarmount/ratarmount.py", line 1920, in main
    #     cli(args)
    #   File "/home/runner/work/ratarmount/ratarmount/ratarmount.py", line 1892, in cli
    #     foreground                   = bool(args.foreground),
    #   File "/home/runner/work/ratarmount/ratarmount/ratarmount.py", line 571, in __init__
    #     mountSources.append((os.path.basename(path), openMountSource(path, **options)))
    #   File "/opt/hostedtoolcache/Python/3.7.17/x64/lib/python3.7/site-packages/ratarmountcore/factory.py", line 382, in openMountSource
    #     openedURL = tryOpenURL(fileOrPath, printDebug=printDebug)
    #   File "/opt/hostedtoolcache/Python/3.7.17/x64/lib/python3.7/site-packages/ratarmountcore/factory.py", line 349, in tryOpenURL
    #     elif fileSystem.isdir(path):
    #   File "/opt/hostedtoolcache/Python/3.7.17/x64/lib/python3.7/site-packages/fsspec/asyn.py", line 114, in wrapper
    #     return sync(self.loop, func, *args, **kwargs)
    #   File "/opt/hostedtoolcache/Python/3.7.17/x64/lib/python3.7/site-packages/fsspec/asyn.py", line 99, in sync
    #     raise return_result
    #   File "/opt/hostedtoolcache/Python/3.7.17/x64/lib/python3.7/site-packages/fsspec/asyn.py", line 54, in _runner
    #     result[0] = await coro
    #   File "/opt/hostedtoolcache/Python/3.7.17/x64/lib/python3.7/site-packages/s3fs/core.py", line 1347, in _isdir
    #     return bool(await self._lsdir(path))
    #   File "/opt/hostedtoolcache/Python/3.7.17/x64/lib/python3.7/site-packages/s3fs/core.py", line 688, in _lsdir
    #     versions=versions,
    #   File "/opt/hostedtoolcache/Python/3.7.17/x64/lib/python3.7/site-packages/s3fs/core.py", line 714, in _iterdir
    #     await self.set_session()
    #   File "/opt/hostedtoolcache/Python/3.7.17/x64/lib/python3.7/site-packages/s3fs/core.py", line 492, in set_session
    #     self.session = aiobotocore.session.AioSession(**self.kwargs)
    # TypeError: __init__() got an unexpected keyword argument 'endpoint_url'
    if [[ -n "$python3MinorVersion" && "$python3MinorVersion" -le 7 ]]; then
        return 0
    fi

    local mountPoint pid weedFolder port
    mountPoint=$( mktemp -d --suffix .test.ratarmount )
    port=8053

    if [[ ! -f weed ]]; then
        wget -q 'https://github.com/seaweedfs/seaweedfs/releases/download/3.74/linux_amd64_large_disk.tar.gz'
        tar -xf 'linux_amd64_large_disk.tar.gz'
    fi
    [[ -x weed ]] || chmod u+x weed

    weedFolder=$( mktemp -d --suffix .test.ratarmount )
    TMP_FILES_TO_CLEANUP+=( "$weedFolder" )
    ./weed server -dir="$weedFolder" -s3 -s3.port "$port" -idleTimeout=30 -ip 127.0.0.1 &
    pid=$!

    # Wait for port to open
    echo "Waiting for seaweedfs to start up and port $port to open..."
    python3 tests/wait-for-port.py "$port" 50

    # Create bucket and upload test file
    python3 -c "
import os
import sys
import boto3

def list_buckets(client):
    result = client.list_buckets()
    return [x['Name'] for x in result['Buckets']] if 'Buckets' in result else []

def list_bucket_files(client, bucket_name):
    result = client.list_objects_v2(Bucket=bucket_name)
    return [x['Key'] for x in result['Contents']] if 'Contents' in result else []

endpoint_url = 'http://127.0.0.1:' + sys.argv[1]
print('Connect to:', endpoint_url)

client = boto3.client(
    's3', endpoint_url=endpoint_url,
    aws_access_key_id = '01234567890123456789',
    aws_secret_access_key = '0123456789012345678901234567890123456789'
)

bucket_name = 'bucket'
buckets = list_buckets(client)
print('Existing buckets:', buckets)
if bucket_name not in buckets:
    print(f'Create new bucket: {bucket_name} ...')
    client.create_bucket(Bucket=bucket_name)

path = 'tests/single-file.tar'
if not os.path.isfile(path):
    print('Failed to find file to upload:', path)
print(f'Upload file {path} to bucket.')
client.upload_file(path, bucket_name, 'single-file.tar')
" "$port"

    export FSSPEC_S3_ENDPOINT_URL="http://127.0.0.1:$port"
    # Even though no credentials are configured for the seaweedfs server, we need dummy credentials for boto3 -.-
    export AWS_ACCESS_KEY_ID=01234567890123456789
    export AWS_SECRET_ACCESS_KEY=0123456789012345678901234567890123456789

    # At last, test ratarmount.
    checkFileInTARForeground "s3://bucket/single-file.tar" 'bar' d3b07384d113edec49eaa6238ad5ff00 ||
        returnError "$LINENO" 'Failed to read from S3 server'
    checkFileInTARForeground "s3://bucket/" 'single-file.tar' 1a28538854d1884e4415cb9bfb7a2ad8 ||
        returnError "$LINENO" 'Failed to read from S3 server'
    checkFileInTARForeground "s3://bucket" 'single-file.tar' 1a28538854d1884e4415cb9bfb7a2ad8 ||
        returnError "$LINENO" 'Failed to read from S3 server'

    kill $pid &>/dev/null

    'rm' -rf "$weedFolder"
}


checkURLProtocolSamba()
{
    # Using impacket/examples/smbserver.py does not work for a multidude of reasons.
    # Therefore set up a server with tests/install-smbd.sh from outside and check for its existence here.

    local port=445
    if ! command -v smbclient &>/dev/null || ! python3 tests/wait-for-port.py "$port" 0; then
        echoerr "Skipping SMB test because no server was found on 127.0.0.1:$port."
        return 0
    fi

    mkdir -p /tmp/smbshare
    cp tests/single-file.tar /tmp/smbshare/
    chmod -R o+r /tmp/smbshare/

    local user='pqvfumqbqp'
    local password='ioweb123GUIweb'

    smbclient --user="$user" --password="$password" --port "$port" -c ls //127.0.0.1/test-share

    checkFileInTAR "smb://$user:$password@127.0.0.1:$port/test-share/single-file.tar" \
        bar d3b07384d113edec49eaa6238ad5ff00 || returnError "$LINENO" 'Failed to read from Samba server'
    checkFileInTAR "smb://$user:$password@127.0.0.1:$port/test-share/" \
        single-file.tar 1a28538854d1884e4415cb9bfb7a2ad8 || returnError "$LINENO" 'Failed to read from Samba server'
    checkFileInTAR "smb://$user:$password@127.0.0.1:$port/test-share" \
        single-file.tar 1a28538854d1884e4415cb9bfb7a2ad8 || returnError "$LINENO" 'Failed to read from Samba server'
}


checkURLProtocolIPFS()
{
    # TODO ipfsspec still fails to import with Python 3.14
    #      https://github.com/eigenein/protobuf/issues/177
    # Don't know why I get this error on Ubuntu 22.04 and Python 3.7:
    # ratarmountcore.utils.RatarmountError: Opening URL ipfs://QmZwm9gKZaayGWqYtMgj6cd4JaNK1Yp2ChYZhXrERGq4Gi failed
    # because path QmZwm9gKZaayGWqYtMgj6cd4JaNK1Yp2ChYZhXrERGq4Gi does not exist on remote!
    if [[ -n "$python3MinorVersion" && ( "$python3MinorVersion" -ge 14 || "$python3MinorVersion" -le 7 ) ]]; then
        return 0
    fi

    # Using impacket/examples/smbserver.py does not work for a multidude of reasons.
    # Therefore set up a server with tests/install-smbd.sh from outside and check for its existence here.
    local ipfs
    if command -v ipfs &>/dev/null; then
        ipfs=ipfs
    elif [[ -f ipfs ]]; then
        ipfs=./ipfs
    else
        wget -q -O- 'https://github.com/ipfs/kubo/releases/download/v0.30.0/kubo_v0.30.0_linux-amd64.tar.gz' |
            tar -zx kubo/ipfs
        ipfs=kubo/ipfs
    fi

    local pid=
    $ipfs init --profile server
    if ! pgrep ipfs; then
        $ipfs daemon &
        pid=$!
        sleep 5
    fi

    local folder
    folder=$( mktemp -d --suffix .test.ratarmount )
    cp tests/single-file.tar "$folder/"
    $ipfs add -r "$folder/"
    # These hashes should be reproducible as long as neither the contents nor the file names change!
    #added QmcbpsdbKYMpMjXvoFbr9pUWC3Z7ZQVXuEoPFRHaNukAsX tests/single-file.tar
    #added QmZwm9gKZaayGWqYtMgj6cd4JaNK1Yp2ChYZhXrERGq4Gi tests

    checkFileInTARForeground "ipfs://QmcbpsdbKYMpMjXvoFbr9pUWC3Z7ZQVXuEoPFRHaNukAsX" bar \
        d3b07384d113edec49eaa6238ad5ff00 || returnError "$LINENO" 'Failed to read from IPFS'
    checkFileInTARForeground "ipfs://QmZwm9gKZaayGWqYtMgj6cd4JaNK1Yp2ChYZhXrERGq4Gi" single-file.tar \
        1a28538854d1884e4415cb9bfb7a2ad8 || returnError "$LINENO" 'Failed to read from IPFS'

    if [[ -n "$pid" ]]; then kill "$pid"; fi
}


checkURLProtocolWebDAV()
{
    if ! pip show wsgidav &>/dev/null; then
        echoerr "Skipping WebDAV test because wsigdav package is not installed."
        return 0
    fi

    local port=8047
    # BEWARE OF LOOP MOUNTS when testing locally!
    # It will time out, when trying to expose PWD via WebDAV while mounting into PWD/mounted.
    wsgidav --host=127.0.0.1 --port=$port --root="$PWD/tests" --auth=anonymous &
    local pid=$!
    sleep 5

    checkFileInTARForeground "webdav://127.0.0.1:$port/single-file.tar" bar \
        d3b07384d113edec49eaa6238ad5ff00 || returnError "$LINENO" 'Failed to read from WebDAV server'
    checkFileInTARForeground "webdav://127.0.0.1:$port" single-file.tar \
        1a28538854d1884e4415cb9bfb7a2ad8 || returnError "$LINENO" 'Failed to read from WebDAV server'

    kill "$pid"

    local user password
    user='pqvfumqbqp'
    password='ioweb123GUIweb'

cat <<EOF > wsgidav-config.yaml
http_authenticator:
    domain_controller: null  # Same as wsgidav.dc.simple_dc.SimpleDomainController
    accept_basic: true  # Pass false to prevent sending clear text passwords
    accept_digest: true
    default_to_digest: true

simple_dc:
    user_mapping:
        "*":
            "$user":
                password: "$password"
EOF

    wsgidav --host=127.0.0.1 --port=$port --root="$PWD/tests" --config=wsgidav-config.yaml &
    pid=$!
    sleep 5

    checkFileInTARForeground "webdav://$user:$password@127.0.0.1:$port/single-file.tar" bar \
        d3b07384d113edec49eaa6238ad5ff00 || returnError "$LINENO" 'Failed to read from WebDAV server'
    checkFileInTARForeground "webdav://$user:$password@127.0.0.1:$port" single-file.tar \
        1a28538854d1884e4415cb9bfb7a2ad8 || returnError "$LINENO" 'Failed to read from WebDAV server'

    export WEBDAV_USER=$user
    export WEBDAV_PASSWORD=$password
    checkFileInTARForeground "webdav://127.0.0.1:$port/single-file.tar" bar \
        d3b07384d113edec49eaa6238ad5ff00 || returnError "$LINENO" 'Failed to read from WebDAV server'
    checkFileInTARForeground "webdav://127.0.0.1:$port" single-file.tar \
        1a28538854d1884e4415cb9bfb7a2ad8 || returnError "$LINENO" 'Failed to read from WebDAV server'
    unset WEBDAV_USER
    unset WEBDAV_PASSWORD

    # This server using SSL also works, but do not overload it with regular tests.
    # ratarmount 'webdav://www.dlp-test.com\WebDAV:WebDAV@www.dlp-test.com/webdav' mounted
    # checkFileInTARForeground "webdav://www.dlp-test.com\WebDAV:WebDAV@www.dlp-test.com/webdav" \
    #     mounted/WebDAV_README.txt 87d13914fe24e486be943cb6b1f4e224 ||
    #     returnError "$LINENO" 'Failed to read from WebDAV server'

    kill "$pid"
}


checkURLProtocolDropbox()
{
    if [[ -z "$DROPBOX_TOKEN" ]]; then
        echo "Skipping Dropbox test because DROPBOX_TOKEN is not configured."
        return 0
    fi

    checkFileInTAR "dropbox://tests/single-file.tar" bar d3b07384d113edec49eaa6238ad5ff00 ||
        returnError "$LINENO" 'Failed to read from Dropbox'
    checkFileInTAR "dropbox://tests/" single-file.tar 1a28538854d1884e4415cb9bfb7a2ad8 ||
        returnError "$LINENO" 'Failed to read from Dropbox'
    checkFileInTAR "dropbox://tests" single-file.tar 1a28538854d1884e4415cb9bfb7a2ad8 ||
        returnError "$LINENO" 'Failed to read from Dropbox'
}


checkRemoteSupport()
{
    # Some implementations of fsspec. See e.g. this list:
    # https://filesystem-spec.readthedocs.io/en/latest/api.html#other-known-implementations

    checkURLProtocolFile || returnError 'Failed file:// check'
    checkURLProtocolGit || returnError 'Failed git:// check'
    checkURLProtocolGithub || returnError 'Failed github:// check'
    checkURLProtocolFTP || returnError 'Failed ftp:// check'

    checkURLProtocolHTTP || returnError 'Failed http:// check'
    checkURLProtocolIPFS || returnError 'Failed ipfs:// check'
    checkURLProtocolS3 || returnError 'Failed s3:// check'
    checkURLProtocolSSH || returnError 'Failed ssh:// check'

    checkURLProtocolDropbox || returnError 'Failed dropbox:// check'
    checkURLProtocolSamba || returnError 'Failed smb:// check'
    checkURLProtocolWebDAV || returnError 'Failed webdav:// check'
}


rm -f ratarmount.{stdout,stderr}.log

# Linting only to be done locally because in CI it is in separate steps
if [[ -z "$CI" ]]; then
    COLUMNS=98 $RATARMOUNT_CMD --help | sed '/# Metadata Index Cache/,$d' > tests/ratarmount-help.txt

    files=()
    while read -r file; do
        files+=( "$file" )
    done < <(
        git ls-tree -r --name-only HEAD |
            'grep' '[.]py$' |
            'grep' -v -F '__init__.py' |
            'grep' -v 'benchmarks/' |
            'grep' -v -F 'setup.py' |
            'grep' -v 'test.*.py' |
            'grep' -v 'fuse.py'
    )

    testFiles=()
    while read -r file; do
        testFiles+=( "$file" )
    done < <( git ls-tree -r --name-only HEAD | 'grep' 'test.*[.]py$' | 'grep' -v 'conftest[.]py$' )

    echo "Checking files:"
    printf '    %s\n' "${files[@]}" "${testFiles[@]}"

    pylint --rcfile tests/.pylintrc "${files[@]}" "${testFiles[@]}" | tee pylint.log
    if 'grep' -E -q ': E[0-9]{4}: ' pylint.log; then
        echoerr 'There were warnings during the pylint run!'
        exit 1
    fi
    rm pylint.log

    mypy --config-file tests/.mypy.ini "${files[@]}" || returnError "$LINENO" 'Mypy failed!'
    mypy --config-file tests/.mypy.ini "${testFiles[@]}" || returnError "$LINENO" 'Mypy failed!'

    pytype -d import-error -P"$( cd core && pwd ):$( pwd )" "${files[@]}" \
        || returnError "$LINENO" 'Pytype failed!'

    black -q --line-length 120 --skip-string-normalization "${files[@]}" "${testFiles[@]}"

    filesToSpellCheck=()
    while read -r file; do
        filesToSpellCheck+=( "$file" )
    done < <( git ls-tree -r --name-only HEAD | 'grep' -E '[.](py|md|txt|sh|yml)' )
    # fsspec uses cachable instead of cacheable ...
    codespell "${filesToSpellCheck[@]}"

    flake8 --config tests/.flake8 "${files[@]}" "${testFiles[@]}" || returnError "$LINENO" 'Flake8 failed!'

    shellcheck tests/*.sh || returnError "$LINENO" 'shellcheck failed!'

    # Test runtimes 2024-04-04 on Ryzen 3900X. On the CI with nproc=4, the speedup is roughly 2x.
    # Note that pytest-xdist doesn't scale arbitrarily because it seems to start up threads sequentially,
    # which can take ~2s for 48 threads!
    # core/tests/test_AutoMountLayer.py         in 19.05s   parallelize -> 5.64s
    # core/tests/test_BlockParallelReaders.py   in 57.95s   parallelize -> 12.22s
    # core/tests/test_LibarchiveMountSource.py  in 246.99s  parallelize -> 74.43s
    # core/tests/test_RarMountSource.py         in 0.08s
    # core/tests/test_SQLiteBlobFile.py         in 0.24s
    # core/tests/test_SQLiteIndex.py            in 0.10s
    # core/tests/test_SQLiteIndexedTar.py       in 154.08s  parallelize -> 63.95s
    # core/tests/test_StenciledFile.py          in 1.91s
    # core/tests/test_SubvolumesMountSource.py  in 0.12s
    # core/tests/test_UnionMountSource.py       in 0.12s
    # core/tests/test_ZipMountSource.py         in 0.09s
    # core/tests/test_compressions.py           in 0.13s
    # core/tests/test_factory.py                in 0.36s
    # core/tests/test_utils.py                  in 0.22s
    # tests/test_cli.py                         in 67.09s  parallelize -> n=8: 8.91s, n=24: 4.54s, n=48: 4.33s,
    #                                                                     n=96: 6.52s

    # Pytest has serious performance issues. It does collect all tests beforehand and does not free memory
    # after tests have finished it seems. Or maybe that memory is a bug with indexed_gzip. But the problem is
    # that all tests after that one outlier also run slower! Maybe because of a Python garbage collector bug?
    # For that reason, run each test file separately.
    for testFile in "${testFiles[@]}"; do
        case "$testFile" in
            "tests/test_cli.py")
                # First off, n=auto seems to use the physical cores and ignores virtual ones.
                # Secondly, these tests scale much better than the others because most time is spent waiting for
                # the FUSE mount point to appear or disappear, which doesn't seem to be bottlenecked by CPU usage.
                python3 -X dev -W ignore::DeprecationWarning -u \
                    -c "import pytest, re, sys; sys.exit(pytest.console_main())" \
                    -n 24 --disable-warnings "$testFile" || returnError "$LINENO" 'pytest failed!'
                ;;
            "core/tests/test_AutoMountLayer.py"\
            |"core/tests/test_BlockParallelReaders.py"\
            |"core/tests/test_LibarchiveMountSource.py"\
            |"core/tests/test_SQLiteIndexedTar.py")
                echo "$testFile"  # pytest-xdist seems to omit the test file name
                pytest -n auto --disable-warnings "$testFile" || returnError "$LINENO" 'pytest failed!'
                ;;
            *)
                if [[ "${testFile//test_//}" != "$testFile" ]]; then
                    # Fusepy warns about usage of use_ns because the implicit behavior is deprecated.
                    # But there has been no development to fusepy for 4 years, so I think it should be fine to ignore.
                    pytest --disable-warnings "$testFile" || returnError "$LINENO" 'pytest failed!'
                fi
                ;;
        esac
    done
fi


# We need to run these tests without pytest because, for some reason,
# pytest slows the zip decryption fix down from 0.1 s to 1.1 s?!
python3 core/tests/test_ZipMountSource.py


rm -f tests/*.index.*
'cp' 'tests/single-file.tar' 'tests/#not-a-good-name! Ör, is it?.tar'


tests=()
pytestedTests=()


if python3 -c 'import libarchive' &>/dev/null; then
pytestedTests+=(
    2709a3348eb2c52302a7606ecf5860bc tests/nested-with-symlink.7z                 foo/fighter/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/nested-with-symlink.7z                 foo/fighter/saucer
    2b87e29fca6ee7f1df6c1a76cb58e101 tests/nested-with-symlink.7z                 foo/lighter.tar/fighter/bar
    2709a3348eb2c52302a7606ecf5860bc tests/zip.7z                                 natsu.zip/ufo
    10d6977ec2ab378e60339323c24f9308 tests/zip.7z                                 natsu.zip/foo
    2709a3348eb2c52302a7606ecf5860bc tests/file-in-non-existing-folder.7z         foo2/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.7z                      foo/fighter/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.7z                      foo/jet/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/double-compressed-nested-tar.tar.7z.7z nested-tar.tar.7z/nested-tar.tar/foo/fighter/ufo

    19696f24a91fc4e8950026f9c801a0d0 tests/simple.lzma                            simple
    19696f24a91fc4e8950026f9c801a0d0 tests/simple.lrz                             simple
    19696f24a91fc4e8950026f9c801a0d0 tests/simple.lz4                             simple
    19696f24a91fc4e8950026f9c801a0d0 tests/simple.lzip                            simple
    19696f24a91fc4e8950026f9c801a0d0 tests/simple.lzo                             simple
    19696f24a91fc4e8950026f9c801a0d0 tests/simple.Z                               simple

    d3b07384d113edec49eaa6238ad5ff00 tests/single-file.ar                         bar
    d3b07384d113edec49eaa6238ad5ff00 tests/single-file.cab                        bar
    d3b07384d113edec49eaa6238ad5ff00 tests/single-file.iso.bz2                    single-file.iso/bar
    d3b07384d113edec49eaa6238ad5ff00 tests/single-file.xar                        bar
    d3b07384d113edec49eaa6238ad5ff00 tests/single-file.bin.cpio                   bar
    d3b07384d113edec49eaa6238ad5ff00 tests/single-file.crc.cpio                   bar
    d3b07384d113edec49eaa6238ad5ff00 tests/single-file.hpbin.cpio                 bar
    d3b07384d113edec49eaa6238ad5ff00 tests/single-file.hpodc.cpio                 bar
    d3b07384d113edec49eaa6238ad5ff00 tests/single-file.newc.cpio                  bar
    d3b07384d113edec49eaa6238ad5ff00 tests/single-file.odc.cpio                   bar
    # The contents of files and file hierarchy of WARC is subject to change.
    4aecced75ff52fdd39bb52dae192258f tests/hello-world.warc                       warc-specifications/primers/web-archive-formats/hello-world.txt
)
fi


# TODO Some bug with rarfile throwing: Failed the read enough data: req=304 got=51 and then seek(0) not working?
if ! uname | 'grep' -q -i darwin; then
tests+=(
    2709a3348eb2c52302a7606ecf5860bc tests/nested-with-symlink.rar                foo/fighter/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/nested-with-symlink.rar                foo/fighter/saucer
    2b87e29fca6ee7f1df6c1a76cb58e101 tests/nested-with-symlink.rar                foo/lighter.tar/fighter/bar
)
fi

# zipfile returns unseekable file object with Python 3.6. Therefore, I disabled it completely there.
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
pytestedTests+=(
    2709a3348eb2c52302a7606ecf5860bc tests/file-in-non-existing-folder.zip        foo2/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/rar.zip                                natsu.rar/ufo
    10d6977ec2ab378e60339323c24f9308 tests/rar.zip                                natsu.rar/foo
    2709a3348eb2c52302a7606ecf5860bc tests/nested-with-symlink.zip                foo/fighter/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/nested-with-symlink.zip                foo/fighter/saucer
    2b87e29fca6ee7f1df6c1a76cb58e101 tests/nested-with-symlink.zip                foo/lighter.tar/fighter/bar
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.zip                     foo/fighter/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.zip                     foo/jet/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/mockup-self-extracting.zip             ufo

    2709a3348eb2c52302a7606ecf5860bc tests/denormal-paths.zip                     ufo
    d3b07384d113edec49eaa6238ad5ff00 tests/denormal-paths.zip                     root/bar
    c157a79031e1c40f85931829bc5fc552 tests/denormal-paths.zip                     foo
)
fi

# pyfatfs depends on PyFilesystem2, which only works for Python < 3.12 because of the removed pkg_resources.
# https://github.com/nathanhi/pyfatfs/issues/41
if python3 -c 'import pyfatfs' &>/dev/null; then
tests+=(
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.fat12.bz2               folder-symlink.fat12/foo/fighter/ufo
)
fi

tests+=(
    f47c75614087a8dd938ba4acff252494 tests/simple-file-split.001                  simple-file-split
    f47c75614087a8dd938ba4acff252494 tests/simple-file-split.002                  simple-file-split
    d3b07384d113edec49eaa6238ad5ff00 tests/single-file-split.tar.001              bar
    d3b07384d113edec49eaa6238ad5ff00 'tests/#not-a-good-name! Ör, is it?.tar'     bar

    b026324c6904b2a9cb4b88d6d61c81d1 tests/2k-recursive-tars.tar.bz2              mimi/00001.tar/foo
    3059b91c3562cd29457192eb3c3fe376 tests/2k-recursive-tars.tar.bz2              mimi/01234.tar.versions/1
    8f30b20831bade7a2236edf09a55af60 tests/2k-recursive-tars.tar.bz2              mimi/01333.tar/foo
    f95f8943f6dcf7b3c1c8c2cab5455f8b tests/2k-recursive-tars.tar.bz2              mimi/02000.tar/foo
    c157a79031e1c40f85931829bc5fc552 tests/2k-recursive-tars.tar.bz2              mimi/foo
)

# https://github.com/indygreg/python-zstandard/issues/238
if [[ -n "$python3MinorVersion" && "$python3MinorVersion" -ge 14 ]]; then
pytestedTests+=(
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.zstd.squashfs           foo/fighter/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.zstd.squashfs           foo/jet/ufo
)
fi

pytestedTests+=(
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.gzip.squashfs           foo/fighter/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.lz4.squashfs            foo/fighter/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.lzma.squashfs           foo/fighter/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.lzo.squashfs            foo/fighter/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.no-compression.squashfs foo/fighter/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.xz.squashfs             foo/fighter/ufo

    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.gzip.squashfs           foo/jet/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.lz4.squashfs            foo/jet/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.lzma.squashfs           foo/jet/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.lzo.squashfs            foo/jet/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.no-compression.squashfs foo/jet/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.xz.squashfs             foo/jet/ufo

    2709a3348eb2c52302a7606ecf5860bc tests/file-in-non-existing-folder.rar        foo2/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.rar                     foo/fighter/ufo
    2709a3348eb2c52302a7606ecf5860bc tests/folder-symlink.rar                     foo/jet/ufo

    2709a3348eb2c52302a7606ecf5860bc tests/updated-file-implicitly-with-folder.tar foo/fighter
    2709a3348eb2c52302a7606ecf5860bc tests/updated-file-implicitly-with-folder.tar foo.versions/2/fighter
    c157a79031e1c40f85931829bc5fc552 tests/updated-file-implicitly-with-folder.tar foo.versions/1
    2709a3348eb2c52302a7606ecf5860bc tests/updated-file-implicitly-with-folder.tar bar/par/sora/natsu
    2709a3348eb2c52302a7606ecf5860bc tests/updated-file-implicitly-with-folder.tar bar/par/sora.versions/2/natsu
    cd85c6a5e5053c04f95e1df301c80755 tests/updated-file-implicitly-with-folder.tar bar/par/sora.versions/1

    d3b07384d113edec49eaa6238ad5ff00 tests/single-file.tar                        bar

    d3b07384d113edec49eaa6238ad5ff00 tests/single-file-with-leading-dot-slash.tar bar
    2b87e29fca6ee7f1df6c1a76cb58e101 tests/folder-with-leading-dot-slash.tar      foo/bar
    2709a3348eb2c52302a7606ecf5860bc tests/folder-with-leading-dot-slash.tar      foo/fighter/ufo

    2709a3348eb2c52302a7606ecf5860bc tests/denormal-paths.rar                     ufo
    d3b07384d113edec49eaa6238ad5ff00 tests/denormal-paths.rar                     root/bar
    c157a79031e1c40f85931829bc5fc552 tests/denormal-paths.rar                     foo

    2709a3348eb2c52302a7606ecf5860bc tests/denormal-paths.tar                     ufo
    d3b07384d113edec49eaa6238ad5ff00 tests/denormal-paths.tar                     root/bar
    c157a79031e1c40f85931829bc5fc552 tests/denormal-paths.tar                     foo

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
    19696f24a91fc4e8950026f9c801a0d0 tests/simple.zlib                            simple
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
)

# This is slow and it should not make much of a difference for the different parallelizations.
parallelization=1
checkRemoteSupport

for parallelization in $PARALLELIZATIONS; do

echo "== Testing with -P $parallelization =="
export parallelization

if [[ ! -f tests/2k-recursive-tars.tar ]]; then
    bzip2 -q -d -k tests/2k-recursive-tars.tar.bz2
fi

checkExtendedAttributes || returnError "$LINENO" 'Extended attributes check failed!'
checkStatfs || returnError "$LINENO" 'Statfs failed!'
checkStatfsWriteOverlay || returnError "$LINENO" 'Statfs with write overlay failed!'
checkSymbolicLinkRecursion || returnError "$LINENO" 'Symbolic link recursion failed!'
checkWriteOverlayWithSymbolicLinks || returnError "$LINENO" 'Write overlay tests with symbolic links failed!'
checkWriteOverlayWithNewFiles || returnError "$LINENO" 'Write overlay tests failed!'
checkWriteOverlayWithArchivedFiles || returnError "$LINENO" 'Write overlay tests for archive files failed!'
checkWriteOverlayCommitDelete || returnError "$LINENO" 'Write overlay committing deletions failed!'

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

# Intended for AppImage integration tests, for which the pytest unit tests are decidedly not sufficient
# to detect, e.g., missing libraries in the AppImage.
if [[ $TEST_EXTERNAL_COMMAND -eq 1 ]]; then
    tests+=( "${pytestedTests[@]}" )
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
        TMP_FILES_TO_CLEANUP+=( "${file}.index.sqlite" )
        checkFileInTAR "$file" "$fileName" "$checksum"
        (( ++nFiles ))
    done
    cleanup
    safeRmdir "$( dirname -- "$file" )"
done

checkRecursiveFolderMounting
checkRecursiveFolderMounting --lazy

cleanup

rm -f tests/*.index.*
rmdir tests/*/

done  # for parallelization


if [[ $TEST_EXTERNAL_COMMAND -eq 0 ]]; then
    benchmarkDecoderBackends
fi


echo -e '\e[32mAll tests ran successfully.\e[0m'

#!/usr/bin/env bash

cd -- "$( dirname -- "${BASH_SOURCE[0]}" )/.." || { echo 'Failed to cd to git root!'; exit 1; }

source tests/common.sh


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

    # Check for non-standard extension

    'cp' "$testsFolder/single-nested-file.tar" 'single-nested-file-tar'
    runAndCheckRatarmount -- 'single-nested-file-tar'
    mountPoint='single-nested-file-tar.mounted'
    command grep -q 'iriya' "$mountPoint/foo/fighter/ufo" ||
        returnError "$LINENO" 'Check for auto mount point creation failed!'

    funmount "$mountPoint"
    sleep 1s
    [[ ! -d "$mountPoint" ]] ||
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
    'grep' -Eq '(warning|error|Warning|Error|WARNING|ERROR)' ratarmount.stdout.log ratarmount.stderr.log ||
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

    source tests/create-fixed-archives-list.sh

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
        ! 'grep' -C 5 -E '(warning|error|Warning|Error|WARNING|ERROR)' ratarmount.stdout.log ratarmount.stderr.log ||
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


# 'parallelization' should not matter for most of these tests, therefore skip tests with different values.

rm -f ratarmount.{stdout,stderr}.log

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

checkRecursiveFolderMounting
checkRecursiveFolderMounting --lazy

cleanup

for file in tests/*.index.*; do git ls-files --error-unmatch "$file" &>/dev/null || 'rm' -f "$file"; done
for folder in tests/*/; do safeRmdir "$folder"; done

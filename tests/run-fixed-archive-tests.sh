#!/usr/bin/env bash

cd -- "$( dirname -- "${BASH_SOURCE[0]}" )/.." || { echo 'Failed to cd to git root!'; exit 1; }

source tests/common.sh
source tests/create-fixed-archives-list.sh

rm -f tests/*.*.index.*

# This is slow and it should not make much of a difference for the different parallelizations.
parallelization=1
#checkRemoteSupport

# Mount several archives in a single ratarmount invocation with --disable-union-mount, each in an equally named
# subfolder, instead of one slow FUSE mount per archive. Set this to 1 to mount each archive on its own again,
# e.g., to find out which archive is responsible for a failure.
MOUNT_ARCHIVES_PER_BATCH=${MOUNT_ARCHIVES_PER_BATCH:-100}
export MOUNT_ARCHIVES_PER_BATCH

# Set by the caller before invoking checkBatchedArchives.
BATCH_FILES=()


# Mounts all archives in BATCH_FILES at once and verifies their expected files and checksums from the global 'tests'
# triples. A batch containing a single archive is mounted at the root like checkFileInTAR.
checkBatchedArchives()
{
    local mountFolder
    mountFolder=$( mktemp -d --suffix .test.ratarmount ) || returnError "$LINENO" 'Failed to create temporary directory'
    MOUNT_POINTS_TO_CLEANUP+=( "$mountFolder" )

    local subvolumes=1
    if [[ ${#BATCH_FILES[@]} -eq 1 ]]; then subvolumes=0; fi

    local startTime
    startTime=$( date +%s )

    # Test with forced index recreation first and then with index loading, exactly like checkFileInTAR.
    local forceIndexCreation
    for forceIndexCreation in 1 0; do
        local args=( -P "$parallelization" --detect-gnu-incremental --ignore-zeros --recursive )
        if [[ $forceIndexCreation -eq 1 ]]; then args+=( -c ); fi
        if [[ $subvolumes -eq 1 ]]; then args+=( --disable-union-mount ); fi
        args+=( "${BATCH_FILES[@]}" "$mountFolder" )
        runAndCheckRatarmount "${args[@]}" ||
            returnError "$LINENO" "Failed to mount ${#BATCH_FILES[@]} archives: ${RATARMOUNT_CMD} ${args[*]}"

        local file iTest fileInTar mountFolderPrefix
        for file in "${BATCH_FILES[@]}"; do
            mountFolderPrefix="$mountFolder"
            if [[ $subvolumes -eq 1 ]]; then mountFolderPrefix="$mountFolder/$( basename -- "$file" )"; fi

            for (( iTest = 0; iTest < ${#tests[@]}; iTest += 3 )); do
                [[ "${tests[iTest+1]}" == "$file" ]] || continue
                fileInTar="${tests[iTest+2]}"
                checkStat "$mountFolderPrefix/$fileInTar" ||
                    returnError "$LINENO" "Could not stat '$fileInTar' in '$file'"
                verifyCheckSum "$mountFolderPrefix" "$fileInTar" "$file" "${tests[iTest]}" ||
                    returnError "$LINENO" "Checksum mismatch for '$fileInTar' in '$file'"
            done
        done

        funmount "$mountFolder"
    done

    safeRmdir "$mountFolder"

    echoerr "[${FUNCNAME[0]}] Tested successfully ${#BATCH_FILES[@]} archives in $(( $( date +%s ) - startTime ))s"
}


for parallelization in $PARALLELIZATIONS; do
    echo "== Testing with -P $parallelization =="
    export parallelization

    # Intended for AppImage integration tests, for which the pytest unit tests are decidedly not sufficient
    # to detect, e.g., missing libraries in the AppImage.
    if [[ $TEST_EXTERNAL_COMMAND -eq 1 ]]; then
        for file in tests/*.ext4.bz2; do bzip2 -d -k -f "$file"; done
        tests+=( "${pytestedTests[@]}" )
    fi

    uniqueArchives=()
    for (( iTest = 0; iTest < ${#tests[@]}; iTest += 3 )); do
        tarPath=${tests[iTest+1]}

        seenArchive=0
        for uniqueArchive in "${uniqueArchives[@]}"; do
            if [[ "$uniqueArchive" == "$tarPath" ]]; then
                seenArchive=1
                break
            fi
        done

        if [[ $seenArchive -eq 0 ]]; then
            uniqueArchives+=( "$tarPath" )
        fi
    done

    # Recompression and split files involve several files, so test those on their own to keep the batching simple.
    batchArchives=()
    for tarPath in "${uniqueArchives[@]}"; do
        fileChecks=()
        for (( iTest = 0; iTest < ${#tests[@]}; iTest += 3 )); do
            if [[ "${tests[iTest+1]}" != "$tarPath" ]]; then
                continue
            fi

            fileChecks+=( "${tests[iTest+2]}" "${tests[iTest]}" )
        done

        # Only test some larger files for all compression backends because most of the files are minimal
        # tests which all have the same size of 20*512B. In the first place, the compression backends
        # should be tested more rigorously inside their respective projects not by ratarmount.
        # readarray does not work on macOS!
        #readarray -t files < <( recompressFile "$tarPath" )
        if [[ "$tarPath" =~ 2k-recursive ]]; then
            # readarray does not work on macOS!
            #readarray -t files < <( recompressFile "$tarPath" )
            files=()
            while IFS=$'\n' read -r line; do
                files+=( "$line" )
            done < <( recompressFile "$tarPath" || returnError "$LINENO" 'Something went wrong during recompression.' )
            TMP_FILES_TO_CLEANUP+=( "${files[@]}" )
        elif [[ "$tarPath" =~ [.][0-9]+$ ]]; then
            files=( "$tarPath" )
        else
            batchArchives+=( "$tarPath" )
            continue
        fi

        for file in "${files[@]}"; do
            TMP_FILES_TO_CLEANUP+=( "${file}.index.sqlite" )
            checkFileInTAR "$file" "${fileChecks[@]}"
            nFiles=$(( nFiles + ${#fileChecks[@]} / 2 ))
        done

        cleanup
        for file in "${files[@]}"; do
            safeRmdir "$( dirname -- "$file" )"
        done
    done

    for (( start = 0; start < ${#batchArchives[@]}; start += MOUNT_ARCHIVES_PER_BATCH )); do
        BATCH_FILES=( "${batchArchives[@]:start:MOUNT_ARCHIVES_PER_BATCH}" )
        nFiles=$(( nFiles + ${#BATCH_FILES[@]} ))
        checkBatchedArchives
    done

    cleanup

    for file in tests/*.index.*; do
        if [[ ! -f "$file" ]]; then continue; fi
        git ls-files --error-unmatch "$file" &>/dev/null || 'rm' -f "$file";
    done
    for folder in tests/*/; do safeRmdir "$folder"; done
done

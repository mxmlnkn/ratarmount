#!/usr/bin/env bash

cd -- "$( dirname -- "${BASH_SOURCE[0]}" )/.." || { echo 'Failed to cd to git root!'; exit 1; }

source tests/common.sh
source tests/create-fixed-archives-list.sh

rm -f tests/*.*.index.*

# This is slow and it should not make much of a difference for the different parallelizations.
parallelization=1
#checkRemoteSupport

for parallelization in $PARALLELIZATIONS; do
    echo "== Testing with -P $parallelization =="
    export parallelization

    # Intended for AppImage integration tests, for which the pytest unit tests are decidedly not sufficient
    # to detect, e.g., missing libraries in the AppImage.
    if [[ $TEST_EXTERNAL_COMMAND -eq 1 ]]; then
        for file in tests/*.ext4.bz2; do bzip2 -d -k -f "$file"; done
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

    cleanup

    for file in tests/*.index.*; do
        if [[ ! -f "$file" ]]; then continue; fi
        git ls-files --error-unmatch "$file" &>/dev/null || 'rm' -f "$file";
    done
    for folder in tests/*/; do safeRmdir "$folder"; done
done

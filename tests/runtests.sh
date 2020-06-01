#!/bin/bash

cd -- "$( dirname -- "${BASH_SOURCE[0]}" )"
cd ..

echoerr() { echo "$@" 1>&2; }

error=0

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
    if mountpoint "$mountFolder" &>/dev/null; then
        fusermount -u "$mountFolder"
        while mountpoint "$mountFolder" &>/dev/null; do
            sleep 0.2s
        done
    fi
}

returnError()
{
    echoerr -e "\e[37m$*\e[0m"
    echoerr -e '\e[31mTEST FAILED!\e[0m'
    exit 1
}

checkFileInTAR()
{
    local archive="$1"; shift
    local fileInTar="$1"; shift
    local correctChecksum="$1"

    local mountFolder="$( mktemp -d )"

    funmount "$mountFolder"

    # try with index recreation
    local cmd=( python3 ratarmount.py -c --recursive "$archive" "$mountFolder" )
    "${cmd[@]}" &>/dev/null
    checkStat "$mountFolder" || returnError "${cmd[*]}"
    checkStat "$mountFolder/$fileInTar" || returnError "${cmd[*]}"
    verifyCheckSum "$mountFolder" "$fileInTar" "$archive" "$correctChecksum" || returnError "${cmd[*]}"
    funmount "$mountFolder"

    # retry without forcing index recreation
    local cmd=( python3 ratarmount.py --recursive "$archive" "$mountFolder" )
    "${cmd[@]}" &>/dev/null
    checkStat "$mountFolder" || returnError "${cmd[*]}"
    checkStat "$mountFolder/$fileInTar" || returnError "${cmd[*]}"
    verifyCheckSum "$mountFolder" "$fileInTar" "$archive" "$correctChecksum" || returnError "${cmd[*]}"
    funmount "$mountFolder"

    rmdir "$mountFolder"

    echoerr "Tested succesfully '$fileInTar' in '$archive' for checksum $correctChecksum"

    return 0
}

checkFileInTARPrefix()
{
    local prefix="$1"; shift
    local archive="$1"; shift
    local fileInTar="$1"; shift
    local correctChecksum="$1"

    local mountFolder="$( mktemp -d )"

    funmount "$mountFolder"

    # try with index recreation
    local cmd=( python3 ratarmount.py -c --recursive --prefix "$prefix" "$archive" "$mountFolder" )
    "${cmd[@]}" &>/dev/null
    checkStat "$mountFolder" || returnError "${cmd[*]}"
    checkStat "$mountFolder/$fileInTar" || returnError "${cmd[*]}"
    verifyCheckSum "$mountFolder" "$fileInTar" "$archive" "$correctChecksum" || returnError "${cmd[*]}"
    funmount "$mountFolder"

    rmdir "$mountFolder"

    echoerr "Tested succesfully '$fileInTar' in '$archive' for checksum $correctChecksum"

    return 0
}

checkLinkInTAR()
{
    local archive="$1"; shift
    local fileInTar="$1"; shift
    local correctLinkTarget="$1"

    local mountFolder="$( mktemp -d )"

    funmount "$mountFolder"

    # try with index recreation
    local cmd=( python3 ratarmount.py -c --recursive "$archive" "$mountFolder" )
    "${cmd[@]}" &>/dev/null
    checkStat "$mountFolder" || returnError "${cmd[*]}"
    checkStat "$mountFolder/$fileInTar" || returnError "${cmd[*]}"
    if [[ $( readlink -- "$mountFolder/$fileInTar" ) != $correctLinkTarget ]]; then
        echoerr -e "\e[37mLink target of '$fileInTar' in mounted TAR '$archive' does not match"'!\e[0m'
        returnError "${cmd[*]}"
    fi
    funmount "$mountFolder"

    rmdir "$mountFolder"

    echoerr "Tested succesfully '$fileInTar' in '$archive' for link target $correctLinkTarget"

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
    fileNameDataSizeInMB="$1"
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

    largeTarFolder="$( mktemp -d )"

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
        largeTar="$( createLargeTar $fileNameDataSizeInMB )"
    fi

    # clear up mount folder if already in use
    local mountFolder="$( mktemp -d )"
    if mountpoint "$mountFolder" &>/dev/null; then
        fusermount -u "$mountFolder"
        while mountpoint "$mountFolder" &>/dev/null; do sleep 0.2s; done
    fi

    # benchmark creating the index

    python3 ./ratarmount.py -c -f --recursive "$largeTar" "$mountFolder" &
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

    python3 ./ratarmount.py -f --recursive "$largeTar" "$mountFolder" &
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
        printf '%s %s ' $( getPeakMemoryFromFile "$timeSeriesFile" ) >> "$logFile"
        'mv' "$timeSeriesFile" "$benchmarksFolder/$timeSeriesFile"

        local timeSeriesFile="benchmark-memory-${mib}-MiB-loading.dat"
        printf '%s %s ' $( getPeakMemoryFromFile "$timeSeriesFile" ) >> "$logFile"
        'mv' "$timeSeriesFile" "$benchmarksFolder/$timeSeriesFile"
    done
}

checkAutomaticIndexRecreation()
(
    ratarmountScript=$( realpath -- ratarmount.py )
    cd -- "$( mktemp -d )"

    archive='momo.tar'
    mountFolder='momo'

    # 1. Create a simple TAR
    fileName='meme'
    echo 'mimi' > "$fileName"
    tar -cf "$archive" "$fileName"

    # 1. Check and create index
    python3 "$ratarmountScript" "$archive"
    diff -- "$fileName" "$mountFolder/$fileName" || returnError 'Files differ!'
    funmount "$mountFolder"

    # 2. Check that index does not get recreated normally
    sleep 1 # because we are comparing timestamps with seconds precision ...
    indexFile='momo.tar.index.sqlite'
    [[ -f $indexFile ]] || returnError 'Index file not found!'
    lastModification=$( stat -c %Y -- "$indexFile" )
    python3 "$ratarmountScript" "$archive"
    diff -- "$fileName" "$mountFolder/$fileName" || returnError 'Files differ!'
    funmount "$mountFolder"
    [[ $lastModification -eq $( stat -c %Y -- "$indexFile" ) ]] || returnError 'Index changed even though TAR did not!'

    # 3. Change contents (and timestamp) without changing the size
    #    (Luckily TAR is filled to 10240 Bytes anyways for very small files)
    sleep 1 # because we are comparing timestamps with seconds precision ...
    fileName="${fileName//e/a}"
    echo 'momo' > "$fileName"
    tar -cf "$archive" "$fileName"

    python3 "$ratarmountScript" "$archive"
    diff -- "$fileName" "$mountFolder/${fileName}" || returnError 'Files differ!'
    funmount "$mountFolder"
    [[ $lastModification -ne $( stat -c %Y -- "$indexFile" ) ]] || \
        returnError 'Index did not change even though TAR did!'
    lastModification=$( stat -c %Y -- "$indexFile" )

    # 4. Check that index changes if size changes but modification timestamp does not
    sleep 1 # because we are comparing timestamps with seconds precision ...
    fileName="heho"
    head -c $(( 100 * 1024 )) /dev/urandom > "$fileName"
    tar -cf "$archive" "$fileName"
    touch -d "@$lastModification" "$archive"

    python3 "$ratarmountScript" "$archive"
    diff -- "$fileName" "$mountFolder/${fileName}" || returnError 'Files differ!'
    funmount "$mountFolder"
    [[ $lastModification -ne $( stat -c %Y -- "$indexFile" ) ]] || \
        returnError 'Index did not change even though TAR filesize did!'
)

checkUnionMount()
(
    ratarmountScript=$( realpath -- ratarmount.py )
    testsFolder="$( pwd )/tests"
    cd -- "$( mktemp -d )"
    keyString='EXTRACTED VERSION'

    tarFiles=( 'hardlink' 'nested-symlinks' 'single-nested-file' 'symlinks' )

    for tarFile in "${tarFiles[@]}"; do
    (
        mkdir "$tarFile" &&
        cd -- "$_" &&
        tar -xf "$testsFolder/$tarFile.tar" &&
        find . -type f -execdir bash -c 'echo "$1" >> "$0"' {} "$keyString" \;
    )
    done

    mountPoint=$( mktemp -d )
    for tarFile in "${tarFiles[@]}"; do
        # Check whether a simple bind mount works, which is now an officially supported perversion of ratarmount
        python3 "$ratarmountScript" "$tarFile" "$mountPoint"
        diff -r --no-dereference "$tarFile" "$mountPoint" || returnError 'Bind mounted folder differs!'
        funmount "$mountPoint"

        # Check that bind mount onto the mount point works
        python3 "$ratarmountScript" "$tarFile" "$tarFile"
        [[ $( find "$tarFile" -mindepth 1 | wc -l ) -gt 0 ]] || returnError 'Bind mounted folder is empty!'
        funmount "$mountPoint"

        # Check whether updating a folder with a TAR works
        python3 "$ratarmountScript" "$tarFile" "$testsFolder/$tarFile.tar" "$mountPoint"
        keyContainingFiles=$( find "$mountPoint" -type f -execdir bash -c '
            if command grep -q "$1" "$0"; then printf "%s\n" "$0"; fi' {} "$keyString" \; | wc -l )
        [[ $keyContainingFiles -eq 0 ]] || returnError 'Found file from updated folder even though all files are updated!'
        funmount "$mountPoint"

        # Check whether updating a TAR with a folder works
        python3 "$ratarmountScript" "$testsFolder/$tarFile.tar" "$tarFile" "$mountPoint"
        keyNotContainingFiles=$( find "$mountPoint" -type f -execdir bash -c '
            if ! command grep -q "$1" "$0"; then printf "%s\n" "$0"; fi' {} "$keyString" \; | wc -l )
        [[ $keyNotContainingFiles -eq 0 ]] || returnError 'Found files from TAR even though it was updated with a folder!'
        funmount "$mountPoint"
    done
)

checkUnionMountFileVersions()
(
    ratarmountScript=$( realpath -- ratarmount.py )
    testsFolder="$( pwd )/tests"
    cd -- "$( mktemp -d )"

    tarFiles=( 'updated-file.tar' )

    mkdir -p folder/foo/fighter
    echo 'untarred' > folder/foo/fighter/ufo
    mkdir emptyFolder

    python3 "$ratarmountScript" emptyFolder folder "$testsFolder/updated-file.tar" emptyFolder folder mountPoint

    untarredFileMd5=$( md5sum folder/foo/fighter/ufo 2>/dev/null | sed 's| .*||' )
    verifyCheckSum mountPoint foo/fighter/ufo updated-file.tar "$untarredFileMd5" \
        || returnError "File check failed"
    verifyCheckSum mountPoint foo/fighter/ufo.versions/1 "$( pwd )" "$untarredFileMd5" \
        || returnError "File check failed"
    verifyCheckSum mountPoint foo/fighter/ufo.versions/2 "$( pwd )" 2709a3348eb2c52302a7606ecf5860bc \
        || returnError "File check failed"
    verifyCheckSum mountPoint foo/fighter/ufo.versions/3 "$( pwd )" 9a12be5ebb21d497bd1024d159f2cc5f \
        || returnError "File check failed"
    verifyCheckSum mountPoint foo/fighter/ufo.versions/4 "$( pwd )" b3de7534cbc8b8a7270c996235d0c2da \
        || returnError "File check failed"
    verifyCheckSum mountPoint foo/fighter/ufo.versions/5 "$( pwd )" "$untarredFileMd5" \
        || returnError "File check failed"

    funmount mountPoint
)

checkAutoMountPointCreation()
(
    ratarmountScript=$( realpath -- ratarmount.py )
    testsFolder="$( pwd )/tests"
    cd -- "$( mktemp -d )"

    cp "$testsFolder/single-nested-file.tar" .
    python3 "$ratarmountScript" *.tar
    command grep -q 'iriya' single-nested-file/foo/fighter/ufo ||
    returnError 'Check for auto mount point creation failed!'

    funmount 'single-nested-file'
    sleep 1s
    [[ ! -d 'single-nested-file' ]] || returnError 'Automatically created mount point was not removed after unmount!'
)


checkTarEncoding()
{
    local archive="$1"; shift
    local encoding="$1"; shift
    local fileInTar="$1"; shift
    local correctChecksum="$1"

    local mountFolder="$( mktemp -d )"

    funmount "$mountFolder"

    # try with index recreation
    local cmd=( python3 ratarmount.py -c --encoding "$encoding" --recursive "$archive" "$mountFolder" )
    "${cmd[@]}" &>/dev/null
    checkStat "$mountFolder" || returnError "${cmd[*]}"
    checkStat "$mountFolder/$fileInTar" || returnError "${cmd[*]}"
    verifyCheckSum "$mountFolder" "$fileInTar" "$archive" "$correctChecksum" || returnError "${cmd[*]}"
    funmount "$mountFolder"

    rmdir "$mountFolder"

    echoerr "Tested succesfully '$fileInTar' in '$archive' for encoding $encoding"

    return 0
}


python3 tests/tests.py || returnError "tests/tests.py"

pylint --disable=C0326,C0103 ratarmount.py > pylint.log

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
)

checkTarEncoding tests/single-file.tar utf-8 bar d3b07384d113edec49eaa6238ad5ff00
checkTarEncoding tests/single-file.tar latin1 bar d3b07384d113edec49eaa6238ad5ff00
checkTarEncoding tests/special-char.tar latin1 'Datei-mit-dämlicher-Kodierung.txt' 2709a3348eb2c52302a7606ecf5860bc

checkLinkInTAR tests/symlinks.tar foo ../foo
checkLinkInTAR tests/symlinks.tar python /usr/bin/python

for (( iTest = 0; iTest < ${#tests[@]}; iTest += 3 )); do
    checkFileInTAR "${tests[iTest+1]}" "${tests[iTest+2]}" "${tests[iTest]}"

    tmpBz2=$( mktemp --suffix='.tar.bz2' )
    bzip2 --keep --stdout "${tests[iTest+1]}" > "$tmpBz2"
    checkFileInTAR "$tmpBz2" "${tests[iTest+2]}" "${tests[iTest]}"
    'rm' -- "$tmpBz2"

    tmpGz=$( mktemp --suffix='.tar.gz' )
    gzip --keep --stdout "${tests[iTest+1]}" > "$tmpGz"
    checkFileInTAR "$tmpGz" "${tests[iTest+2]}" "${tests[iTest]}"
    'rm' -- "$tmpGz"
done

checkFileInTARPrefix '' tests/single-nested-file.tar foo/fighter/ufo 2709a3348eb2c52302a7606ecf5860bc
checkFileInTARPrefix foo tests/single-nested-file.tar fighter/ufo 2709a3348eb2c52302a7606ecf5860bc
checkFileInTARPrefix foo/fighter tests/single-nested-file.tar ufo 2709a3348eb2c52302a7606ecf5860bc

checkAutomaticIndexRecreation || returnError 'Automatic index recreation test failed!'
checkAutoMountPointCreation || returnError 'Automatic mount point creation test failed!'
checkUnionMount || returnError 'Union mounting test failed!'
checkUnionMountFileVersions || returnError 'Union mount file version access test failed!'

#benchmarkSerialization # takes quite long, and a benchmark is not a test ...

rm -f tests/*.index.*
rmdir tests/*/

echo -e '\e[32mAll tests ran succesfully.\e[0m'

exit $error

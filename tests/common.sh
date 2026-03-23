#!/usr/bin/env bash

cd -- "$( dirname -- "${BASH_SOURCE[0]}" )/.." || { echo 'Failed to cd to git root!'; exit 1; }

export RATARMOUNT_CHECK_OVERRIDES=1
export RATARMOUNT_SQLITE_DOUBLE_QUOTES=0

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
export TEST_EXTERNAL_COMMAND
RATARMOUNT_CMD="$RATARMOUNT_CMD --index-minimum-file-count 0"
export RATARMOUNT_CMD
echo "RATARMOUNT_CMD: $RATARMOUNT_CMD"

if [[ -z "$PARALLELIZATIONS" ]]; then
    PARALLELIZATIONS="1 2"
fi

python3MinorVersion=$( python3 -c 'import sys; print(sys.version_info.minor)' )
export python3MinorVersion

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
    safeRmdir() { if [[ -d "$1" && -z "$( find "$1" -maxdepth 1 )" ]]; then rmdir "$1"; fi; }
else
    getFileSize() { stat -c %s -- "$1"; }
    getFileMode() { stat -c %a -- "$1"; }
    getFileMtime() { stat -c %Y -- "$1"; }
    setFileMTime() { touch -d "@$1" "$2"; }
    safeRmdir() { if [[ -d "$1" ]]; then rmdir --ignore-fail-on-non-empty -- "$1"; fi; }
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

    for service in httpd ipfs pyftpdlib wsgidav; do pkill -f "$service" || true; done
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
    ! 'grep' -C 5 -E '(warning|error|Warning|Error|WARNING|ERROR)' ratarmount.stdout.log ratarmount.stderr.log ||
        returnError "$LINENO" "Found warnings while executing: $RATARMOUNT_CMD $*"
}


parallelization=1  # Default for checkFileInTAR in case it is not overwritten / defined later.
export parallelization


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
    if [[ "$archive" =~ [.]tar && ! "$archive" =~ tar:: ]]; then
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
    if [[ "$archive" =~ [.]tar && ! "$archive" =~ [.]7z$ && ! "$archive" =~ tar:: ]]; then
        'grep' -q 'Successfully loaded offset dictionary' ratarmount.stdout.log ratarmount.stderr.log ||
            returnError "$LINENO" "Looks like index was not loaded for '$archive' while executing: $RATARMOUNT_CMD ${args[*]}"
    fi

    safeRmdir "$mountFolder"

    local duration
    duration=$(( $( date +%s ) - startTime ))
    echoerr "Tested successfully '$fileInTar' in '$archive' for checksum $correctChecksum in ${duration}s"

    return 0
}


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

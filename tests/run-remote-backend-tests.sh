#!/usr/bin/env bash

cd -- "$( dirname -- "${BASH_SOURCE[0]}" )/.." || { echo 'Failed to cd to git root!'; exit 1; }

source tests/common.sh


checkURLProtocolFile()
{
    checkFileInTAR 'file://tests/single-file.tar' bar d3b07384d113edec49eaa6238ad5ff00 ||
        returnError "$LINENO" 'Failed to read via file:// protocol'
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
    TMP_FILES_TO_CLEANUP+=('ssh_host_key')
    cat <<EOF > 'ssh_host_key'
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
    TMP_FILES_TO_CLEANUP+=("$file")
    if [[ ! -f "$file" ]] || ! 'grep' -q -F "$publicKey" "$file"; then
        echo "$publicKey" >> "$file"
    fi

    killRogueSSH
    port=8022
    python3 'tests/start-asyncssh-server.py' &
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

    checkFileInTARForeground 'github://mxmlnkn:ratarmount@v0.15.2/tests/single-file.tar' bar d3b07384d113edec49eaa6238ad5ff00 ||
        returnError "$LINENO" 'Failed to read from HTTP server'
    checkFileInTARForeground 'github://mxmlnkn:ratarmount@v0.15.2/tests/' single-file.tar 1a28538854d1884e4415cb9bfb7a2ad8 ||
        returnError "$LINENO" 'Failed to read from HTTP server'
    checkFileInTARForeground 'github://mxmlnkn:ratarmount@v0.15.2/tests' single-file.tar 1a28538854d1884e4415cb9bfb7a2ad8 ||
        returnError "$LINENO" 'Failed to read from HTTP server'
    checkFileInTARForeground 'github://mxmlnkn:ratarmount@v0.15.2/' tests/single-file.tar 1a28538854d1884e4415cb9bfb7a2ad8 ||
        returnError "$LINENO" 'Failed to read from HTTP server'
    checkFileInTARForeground 'github://mxmlnkn:ratarmount@v0.15.2' tests/single-file.tar 1a28538854d1884e4415cb9bfb7a2ad8 ||
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
        arch='arm64'
        if [[ $( uname -m ) =~ x86 ]]; then arch='amd64'; fi
        wget -q 'https://github.com/seaweedfs/seaweedfs/releases/download/3.74/linux_'"$arch"'_large_disk.tar.gz'
        tar -xf 'linux_'"$arch"'_large_disk.tar.gz'
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
    local ipfs arch
    if command -v ipfs &>/dev/null; then
        ipfs=ipfs
    elif [[ -f ipfs ]]; then
        ipfs=./ipfs
    else
        arch='arm64'
        if [[ $( uname -m ) =~ x86 ]]; then arch='amd64'; fi
        wget -q -O- 'https://github.com/ipfs/kubo/releases/download/v0.30.0/kubo_v0.30.0_linux-'"$arch"'.tar.gz' |
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
        echoerr "Skipping WebDAV test because wsgidav package is not installed."
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

    TMP_FILES_TO_CLEANUP+=('wsgidav-config.yaml')

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


rm -f tests/*.*.index.*
rm -f ratarmount.{stdout,stderr}.log

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


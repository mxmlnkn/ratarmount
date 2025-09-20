#! /usr/bin/env python3

import argparse
import asyncio
import getpass
import io
import os
import string
import subprocess
import sys
import time
from pathlib import Path
from typing import Optional

import asyncssh
import fsspec
import fsspec.implementations.sftp
import matplotlib.pyplot as plt
import numpy as np
import paramiko
import sshfs


def get_magnitude(x):
    """
    Returns:
        x in [0.1, 1  ) -> -1
        x in [1  , 10 ) ->  0
        x in [10 , 100) -> +1
    """
    return int(np.floor(np.log10(np.abs(x))))


def test_get_magnitude():
    tests = {
        -1: [0.1, 0.2, 0.9],
        0: [1, 2, 9],
        1: [10, 20, 99],
    }
    for magnitude, values in tests.items():
        for value in values:
            assert get_magnitude(value) == magnitude


def get_first_digit(x):
    mag = get_magnitude(x)
    return int(x / 10**mag)


def existing_digits(s):
    """
    Counts the number of significant digits. Examples:
    Returns:
        0.00013 -> 2
        1.3013  -> 5
        0.13    -> 2
        1000.3  -> 5
        1.20e-5 -> 3
    """
    s = str(s).split('e', maxsplit=1)[0].lstrip('+-0.')

    nDigits = 0
    for i in range(len(s)):
        if s[i] in string.digits:
            nDigits += 1

    return nDigits


def round_to_significant(x, n):
    mag = get_magnitude(x)
    # numpy.around can also round to 10 or 100, ... by specifying negative values for the second argument
    return np.around(x, -mag + n - 1)


def round_stddev(sx):
    # Format exponent and error https://www.ncbi.nlm.nih.gov/pmc/articles/PMC4483789/
    # I can't find that particular standard right now, but I thought some standard specified
    # two significant digits for the first digit being < 3 and else one significant digits on the errors.
    # And of course the mean should have as much precision as the error has
    n_digits_err = 2 if get_first_digit(sx) in [1, 2] else 1
    mag_sx = get_magnitude(sx)
    sx_rounded = round_to_significant(sx, n_digits_err)
    if mag_sx + 1 >= n_digits_err:
        sx_short = str(int(sx_rounded))
    else:
        sx_short = str(sx_rounded)
        sx_short += '0' * max(0, n_digits_err - existing_digits(sx_short))

    return n_digits_err, mag_sx, sx_short


def test_round_stddev():
    tests = [
        (3.111, "3"),
        (31.11, "30"),
        (0.1234, "0.12"),
        (0.1434, "0.14"),
        (0.19, "0.19"),
        (0.3111, "0.3"),
        (1.234, "1.2"),
        (1.434, "1.4"),
        (1.9, "1.9"),
        (3.111, "3"),
        (12.34, "12"),
        (14.34, "14"),
        (19.0, "19"),
        (19, "19"),
        (31.11, "30"),
    ]
    for value, expected in tests:
        assert round_stddev(value)[2] == expected


def uncertain_value_to_str(x, sx):
    n_digits_err, mag_sx, sx_short = round_stddev(sx)
    # pad with 0s if necessary, showing the rounding, i.e., change "2.093 +- 0.02" (s = 0.0203...) to "2.093 +- 0.020"
    x_rounded = np.around(x, -mag_sx + n_digits_err - 1)
    if mag_sx + 1 >= n_digits_err:
        x_short = str(int(x_rounded))
    else:
        x_short = str(x_rounded)
        x_short += '0' * max(0, n_digits_err - existing_digits(x_short))
    return x_short, sx_short


def test_uncertain_value_to_str():
    tests = [
        [0.9183782255, 0.00081245, "0.9184", "0.0008"],
        [12.5435892, 1.0234, "12.5", "1.0"],
        [12.0123, 1.0234, "12.0", "1.0"],
        [141, 15, "141", "15"],
        [1.25435892, 0.10234, "1.25", "0.10"],
        [19235198, 310, "19235200", "300"],
        [52349e-15, 4.25315e-12, "5.2e-11", "4e-12"],
        [138.1, 13, "138", "13"],
    ]
    for test in tests:
        sm, ss = uncertain_value_to_str(test[0], test[1])
        assert sm == test[2]
        assert ss == test[3]


def load_config(username=()):
    default_config = os.path.expanduser(os.path.join('~', '.ssh', 'config'))
    config = asyncssh.config.SSHClientConfig.load(
        None,
        [default_config] if os.access(default_config, os.R_OK) else [],
        False,
        getpass.getuser(),
        username,
        hostname,
        port,
    )
    if config.get('Compression') is None:
        config._options['Compression'] = False

    ssh_options = asyncssh.SSHClientConnectionOptions()
    ssh_options.prepare(last_config=config)
    return ssh_options


class BenchmarkFullRead:
    DATA_FILE_NAME = "full-read.timings.csv"

    @staticmethod
    def plot_bar_comparison(data_file_path, xlabel=None, **kwargs):
        data = {}
        with open(data_file_path, encoding='utf-8') as file:
            for line in file:
                if line.startswith('#'):
                    continue
                parts = line.strip().split(',')
                if parts[1] == ".":  # Ignore SSD benchmarks
                    continue

                assert len(parts) == 4

                if parts[0] not in data:
                    data[parts[0]] = []
                data[parts[0]].append(float(parts[2]) / float(parts[3]) / 1e6)

        def compute_statistics(t):
            return np.mean(t), np.std(t, ddof=1)

        results = {label: compute_statistics(np.array(bandwidths)) for label, bandwidths in data.items()}
        bar_labels = sorted(results.keys(), key=lambda x: results[x][0])

        bar_positions = np.arange(len(bar_labels))
        bar_values = [results[x][0] for x in bar_labels]
        bar_errors = [results[x][1] for x in bar_labels]

        fig = plt.figure(figsize=figsize)
        ax = fig.add_subplot(111, xlabel=xlabel)

        ax.barh(bar_positions, bar_values, xerr=bar_errors, capsize=3)
        ax.set_yticks(bar_positions)
        ax.set_yticklabels(bar_labels)

        for position, value, stddev in zip(bar_positions, bar_values, bar_errors):
            # if value < 500:
            x, sx = uncertain_value_to_str(value, stddev)
            plt.text(value + stddev + 10, position, f"({x} ± {sx}) MB/s", ha='left', va='center')

        xlim = ax.get_xlim()
        ax.set_xlim([0, xlim[1] + 0.4 * (xlim[1] - xlim[0])])

        fig.tight_layout()
        for extension in ['png', 'pdf']:
            fig.savefig(data_file_path.rsplit('.', maxsplit=1)[0] + '.' + extension, dpi=plot_dpi)

    @staticmethod
    def transfer_with_command(command, folder, csv_file, label, file_size):
        old_cwd = os.getcwd()
        os.makedirs(folder, exist_ok=True)
        os.chdir(folder)

        if os.path.isfile(test_file):
            os.remove(test_file)

        t0 = time.time()
        print(' '.join(command))
        subprocess.run(command, check=True)
        t1 = time.time()

        size = os.stat(test_file).st_size
        assert size == file_size
        if os.path.isfile(test_file):
            os.remove(test_file)
        os.chdir(old_cwd)

        csv_file.write(','.join([command[0], folder, str(size), str(t1 - t0)]).encode() + b'\n')
        csv_file.flush()

        print(f"[{label}] Transferred {size} in {t1-t0:.2f} s -> {size/(t1 - t0)/1e6:.2f} MB/s")

    @classmethod
    def benchmark(cls, data_file_path, repetitions, **kwargs):
        if not data_file_path:
            data_file_path = cls.DATA_FILE_NAME

        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.connect(hostname, port=port)
        sftp = client.open_sftp()

        sshfs_fs = sshfs.SSHFileSystem(hostname, port=port, options=load_config())
        fsspec_fs = fsspec.implementations.sftp.SFTPFileSystem(hostname, port=port)
        file_size = sshfs_fs.stat(src_path)['size']

        def download(label, command, folder, csv_file):
            cls.transfer_with_command(command, folder, csv_file=csv_file, label=label, file_size=file_size)

        local_folder = "."
        memory_folder = "/dev/shm/sftp_downloads"

        subprocess.run(['fusermount', '-u', 'mounted-sshfs'], check=False)
        os.makedirs('mounted-sshfs', exist_ok=True)
        source_folder = src_path.rsplit('/', maxsplit=1)[0]

        with open(data_file_path, 'wb') as csv_file:
            csv_file.write(b"# tool, target folder, size/B, time/s\n")

            for i in range(repetitions):
                rclone_command = ['rclone', 'copy', f'localssh:{src_path}', '.']
                lftpget_command = ['lftpget', f"sftp://{hostname}:{port}{src_path}"]
                scp_command = ['scp', '-q', '-P', str(port), f'scp://{hostname}/{src_path}', '.']
                sftp_command = ['sftp', '-q', '-P', str(port), f'{hostname}:{src_path}', '.']
                rsync_command = ['rsync', '-e', f'ssh -p {port}', f'{hostname}:{src_path}', '.']

                download("rclone to memory", rclone_command, memory_folder, csv_file)
                download("lftpget to memory", lftpget_command, memory_folder, csv_file)
                download("scp to memory", scp_command, memory_folder, csv_file)
                download("sftp to memory", sftp_command, memory_folder, csv_file)
                download("rsync to memory", rsync_command, memory_folder, csv_file)

                download("rclone to SSD", rclone_command, local_folder, csv_file)
                download("lftpget to SSD", lftpget_command, local_folder, csv_file)
                download("scp to SSD", scp_command, local_folder, csv_file)
                download("sftp to SSD", sftp_command, local_folder, csv_file)
                download("rsync to SSD", rsync_command, local_folder, csv_file)

                subprocess.run(['sshfs', '-p', str(port), f"{hostname}:{source_folder}", 'mounted-sshfs'], check=True)
                t0 = time.time()
                file_path = Path('mounted-sshfs') / test_file
                if i == 0:
                    print("Block size in sshfs mount point:", file_path.stat().st_blksize)
                size = len(file_path.read_bytes())
                assert size == file_size
                t1 = time.time()
                subprocess.run(['fusermount', '-u', 'mounted-sshfs'], check=False)
                csv_file.write(','.join(["sshfs", ":memory:", str(size), str(t1 - t0)]).encode() + b'\n')
                print(f"[sshfs FUSE] Read {size} in {t1-t0:.2f} s -> {size/(t1-t0)/1e6:.2f} MB/s")

                t0 = time.time()
                size = sftp.getfo(src_path, io.BytesIO())
                t1 = time.time()
                csv_file.write(','.join(["Paramiko getfo", ":memory:", str(size), str(t1 - t0)]).encode() + b'\n')
                print(f"[Paramiko getfo] Read {size} in {t1-t0:.2f} s -> {size/(t1-t0)/1e6:.2f} MB/s")

                t0 = time.time()
                size = len(sshfs_fs.open(src_path).read())
                t1 = time.time()
                csv_file.write(','.join(["fsspec/sshfs", ":memory:", str(size), str(t1 - t0)]).encode() + b'\n')
                print(f"[fsspec/sshfs] Read {size} in {t1-t0:.2f} s -> {size/(t1-t0)/1e6:.2f} MB/s")

                # Too slow! We need to use a smaller file for this one.
                t0 = time.time()
                size = len(fsspec_fs.open('/dev/shm/sftp_shared/silesia.tar.gz').read())
                t1 = time.time()
                csv_file.write(','.join(["fsspec.sftp", ":memory:", str(size), str(t1 - t0)]).encode() + b'\n')
                print(f"[fsspec.sftp] Read {size} in {t1-t0:.2f} s -> {size/(t1-t0)/1e6:.2f} MB/s")
                print()

    @classmethod
    def plot(cls, data_file_path, **kwargs):
        cls.plot_bar_comparison(data_file_path or cls.DATA_FILE_NAME, xlabel="Read Bandwidth / (MB/s)")


class BenchmarkFullWrite(BenchmarkFullRead):
    DATA_FILE_NAME = "full-write.timings.csv"

    @classmethod
    def benchmark(cls, data_file_path, repetitions, **kwargs):
        if not data_file_path:
            data_file_path = cls.DATA_FILE_NAME

        client = paramiko.SSHClient()
        client.load_system_host_keys()
        client.connect(hostname, port=port)
        sftp = client.open_sftp()

        sshfs_fs = sshfs.SSHFileSystem(hostname, port=port, options=load_config())
        fsspec_fs = fsspec.implementations.sftp.SFTPFileSystem(hostname, port=port)
        file_size = sshfs_fs.stat(src_path)['size']

        def upload(label, command, folder, csv_file):
            cls.transfer_with_command(command, folder, csv_file=csv_file, label=label, file_size=file_size)

        memory_folder = "/dev/shm/sftp_downloads"

        subprocess.run(['fusermount', '-u', 'mounted-sshfs'], check=False)
        os.makedirs('mounted-sshfs', exist_ok=True)
        source_folder = src_path.rsplit('/', maxsplit=1)[0]

        with sshfs_fs.open(src_path, 'rb') as file:
            data_to_upload = file.read()

        # fsspec.implementations.sftp is so slow that we need a smaller file for benchmarks to not take forever.
        with sshfs_fs.open(src_path, 'rb') as file:
            smaller_data_to_upload = file.read()

        with open(data_file_path, 'wb') as csv_file:
            csv_file.write(b"# tool, target folder, size/B, time/s\n")

            for i in range(repetitions):
                rclone_command = ['rclone', 'copy', src_path, f'localssh:{target_folder}']
                # lftpget_command = ['lftpget', f"sftp://{hostname}:{port}{src_path}"]
                scp_command = ['scp', '-q', '-P', str(port), src_path, f'scp://{hostname}/{target_folder}']
                # sftp_command = ['sftp', '-q', '-P', str(port), src_path, f'{hostname}:{target_folder}']
                rsync_command = ['rsync', '-e', f'ssh -p {port}', src_path, f'{hostname}:{target_folder}']

                upload("rclone to memory", rclone_command, memory_folder, csv_file)
                # upload("lftpget to memory", lftpget_command, memory_folder, csv_file)
                upload("scp to memory", scp_command, memory_folder, csv_file)
                # upload("sftp to memory", sftp_command, memory_folder, csv_file)
                upload("rsync to memory", rsync_command, memory_folder, csv_file)

                subprocess.run(['sshfs', '-p', str(port), f"{hostname}:{source_folder}", 'mounted-sshfs'], check=True)
                t0 = time.time()
                file_path = Path('mounted-sshfs') / test_file
                if i == 0:
                    print("Block size in sshfs mount point:", file_path.stat().st_blksize)
                size = file_path.write_bytes(data_to_upload)
                assert size == file_size
                t1 = time.time()
                subprocess.run(['fusermount', '-u', 'mounted-sshfs'], check=False)
                csv_file.write(','.join(["sshfs", ":memory:", str(size), str(t1 - t0)]).encode() + b'\n')
                print(f"[sshfs FUSE] Read {size} in {t1-t0:.2f} s -> {size/(t1-t0)/1e6:.2f} MB/s")

                t0 = time.time()
                size = sftp.putfo(io.BytesIO(data_to_upload), src_path).st_size
                assert size == file_size
                t1 = time.time()
                csv_file.write(','.join(["Paramiko putfo", ":memory:", str(size), str(t1 - t0)]).encode() + b'\n')
                print(f"[Paramiko putfo] Read {size} in {t1-t0:.2f} s -> {size/(t1-t0)/1e6:.2f} MB/s")

                t0 = time.time()
                with sshfs_fs.open(src_path, 'wb') as file:
                    size = file.write(data_to_upload)
                    assert size == file_size
                t1 = time.time()
                csv_file.write(','.join(["fsspec/sshfs", ":memory:", str(size), str(t1 - t0)]).encode() + b'\n')
                print(f"[fsspec/sshfs] Read {size} in {t1-t0:.2f} s -> {size/(t1-t0)/1e6:.2f} MB/s")

                # Too slow! We need to use a smaller file for this one.
                t0 = time.time()
                with fsspec_fs.open(src_path, 'wb') as file:
                    # Write does not return the number of bytes written:
                    # https://github.com/fsspec/filesystem_spec/issues/1695
                    old_tell = file.tell()
                    size = file.write(smaller_data_to_upload)
                    size = file.tell() - old_tell
                t1 = time.time()
                csv_file.write(','.join(["fsspec.sftp", ":memory:", str(size), str(t1 - t0)]).encode() + b'\n')
                print(f"[fsspec.sftp] Read {size} in {t1-t0:.2f} s -> {size/(t1-t0)/1e6:.2f} MB/s")
                print()

    @classmethod
    def plot(cls, data_file_path, **kwargs):
        cls.plot_bar_comparison(data_file_path or cls.DATA_FILE_NAME, xlabel="Write Bandwidth / (MB/s)")


class BenchmarkSshfsMaxRequests:
    DATA_FILE_NAME = "sshfs.full-read-by-max_requests.timings.csv"

    @classmethod
    def benchmark(cls, data_file_path, repetitions, **kwargs):
        if not data_file_path:
            data_file_path = cls.DATA_FILE_NAME

        sshfs_fs = sshfs.SSHFileSystem(hostname, port=port, options=load_config())

        with open(data_file_path, 'wb') as csv_file:
            csv_file.write(b"# max requests, size/B, time/s\n")
            for _i in range(repetitions):
                for max_requests in [1, 2, 4, 16, 64, 128, 256, 512, 1024, 4096]:
                    t0 = time.time()
                    size = len(sshfs_fs.open(src_path, max_requests=max_requests).read())
                    t1 = time.time()
                    csv_file.write(','.join([str(max_requests), str(size), str(t1 - t0)]).encode() + b'\n')
                    csv_file.flush()
                    print(
                        f"[fsspec/sshfs] Max requests: {max_requests} "
                        f"Read {size} in {t1-t0:.2f} s -> {size/(t1-t0)/1e6:.2f} MB/s"
                    )
                    print()

    @classmethod
    def plot(cls, data_file_path, **kwargs):
        if not data_file_path:
            data_file_path = cls.DATA_FILE_NAME

        data = {}
        with open(data_file_path, encoding='utf-8') as file:
            for line in file:
                if line.startswith('#'):
                    continue
                parts = line.strip().split(',')
                if parts[1] == ".":  # Ignore SSD benchmarks
                    continue

                assert len(parts) == 3

                max_requests = int(parts[0])
                if max_requests not in data:
                    data[max_requests] = []
                data[max_requests].append(float(parts[1]) / float(parts[2]) / 1e6)

        def compute_statistics(t):
            return np.mean(t), np.std(t, ddof=1)

        results = {label: compute_statistics(np.array(bandwidths)) for label, bandwidths in data.items()}
        bar_labels = sorted(results.keys())

        bar_positions = np.arange(len(bar_labels))
        bar_values = [results[x][0] for x in bar_labels]
        bar_errors = [results[x][1] for x in bar_labels]

        fig = plt.figure(figsize=figsize)
        ax = fig.add_subplot(111, xlabel="Maximum Requests", ylabel="Read Bandwidth / (MB/s)")

        ax.bar(bar_positions, bar_values, yerr=bar_errors, capsize=3)
        ax.set_xticks(bar_positions)
        ax.set_xticklabels(bar_labels)

        fig.tight_layout()
        for extension in ['png', 'pdf']:
            fig.savefig(data_file_path.rsplit('.', maxsplit=1)[0] + '.' + extension, dpi=plot_dpi)


class BenchmarkSshfsBlockSize:
    DATA_FILE_NAME = "sshfs.full-read-by-block_size.timings.csv"

    @classmethod
    def benchmark(cls, data_file_path, repetitions, **kwargs):
        if not data_file_path:
            data_file_path = cls.DATA_FILE_NAME

        sshfs_fs = sshfs.SSHFileSystem(hostname, port=port, options=load_config())

        with open(data_file_path, 'wb') as csv_file:
            csv_file.write(b"# block size/B, size/B, time/s\n")
            for _i in range(repetitions):
                for block_size_in_KiB in [1, 2, 4, 8, 16, 64, 128, 256, 512, 1024, 4096]:
                    block_size = block_size_in_KiB * 1024
                    t0 = time.time()
                    size = len(sshfs_fs.open(src_path, block_size=block_size).read())
                    t1 = time.time()
                    csv_file.write(','.join([str(block_size), str(size), str(t1 - t0)]).encode() + b'\n')
                    csv_file.flush()
                    print(
                        f"[fsspec/sshfs] Block size: {block_size_in_KiB} KiB "
                        f"Read {size} in {t1-t0:.2f} s -> {size/(t1-t0)/1e6:.2f} MB/s"
                    )
                    print()

    @classmethod
    def plot(cls, data_file_path, **kwargs):
        pass


class BenchmarkAsyncsshMaxRequests(BenchmarkSshfsMaxRequests):
    DATA_FILE_NAME = "asyncssh.full-read-by-max_requests.timings.csv"

    @staticmethod
    def download(path, **kwargs):
        ##logging.basicConfig(stream=sys.stdout.buffer, level=logging.DEBUG)
        # logging.basicConfig(filename="asyncssh.log", level=logging.DEBUG)
        # asyncssh.set_debug_level(2)

        ssh_options = load_config()

        async def run_client():
            async with asyncssh.connect(hostname, port, options=ssh_options) as conn, conn.start_sftp_client() as sftp:
                file = await sftp.open(path, "rb")
                return await file.read()

        try:
            return asyncio.run(run_client())
        except (OSError, asyncssh.Error) as exc:
            sys.exit('SSH connection failed: ' + str(exc))

    @classmethod
    def benchmark(cls, data_file_path, repetitions, **kwargs):
        if not data_file_path:
            data_file_path = cls.DATA_FILE_NAME

        with open(data_file_path, 'wb') as csv_file:
            csv_file.write(b"# max requests, size/B, time/s\n")
            for _i in range(repetitions):
                for max_requests in [1, 2, 4, 16, 64, 128, 256, 512, 1024, 4096]:
                    t0 = time.time()
                    size = len(BenchmarkAsyncsshMaxRequests.download(src_path, max_requests=max_requests))
                    t1 = time.time()
                    csv_file.write(','.join([str(max_requests), str(size), str(t1 - t0)]).encode() + b'\n')
                    csv_file.flush()
                    print(
                        f"[fsspec/sshfs] Max requests: {max_requests} "
                        f"Read {size} in {t1-t0:.2f} s -> {size/(t1-t0)/1e6:.2f} MB/s"
                    )
                    print()

    @classmethod
    def plot(cls, data_file_path, **kwargs):
        pass


class BenchmarkSshfsOverread:
    DATA_FILE_NAME = "sshfs.sequential-chunk-reads.timings.csv"

    @classmethod
    def benchmark(cls, data_file_path, repetitions, **kwargs):
        if not data_file_path:
            data_file_path = cls.DATA_FILE_NAME

        fs = {
            'fsspec': fsspec.implementations.sftp.SFTPFileSystem(hostname, port=port),
            'sshfs': sshfs.SSHFileSystem(hostname, port=port, options=load_config()),
        }

        file_size = len(fs['sshfs'].open(src_path).read())
        print(f"Test file sized: {file_size} B")

        csv_file = open(data_file_path, 'wb')
        csv_file.write(b"# chunk size/B, size/B, time/s\n")

        for _i in range(repetitions):
            for chunk_size_in_KiB in [-1, 4 << 20, 2 << 20, 1 << 20, 512 * 1024, 128 * 1024, 4 * 1024, 32]:
                chunk_size = chunk_size_in_KiB * 1024 if chunk_size_in_KiB >= 0 else chunk_size_in_KiB
                chunk_count = (file_size + chunk_size - 1) // chunk_size if chunk_size > 0 else 1
                print(f"Read {chunk_count} chunks sized {chunk_size_in_KiB} KiB.")

                for open_file_name in ['sshfs', 'fsspec']:
                    file = fs[open_file_name].open(src_path)

                    t0 = time.time()
                    size = 0
                    for _i in range(chunk_count):
                        read_size = len(file.read(chunk_size))
                        # print(f"Read {read_size} out of {chunk_size} for chunk {i}.")
                        size += read_size
                    t1 = time.time()

                    if size != file_size:
                        print(f"Read {size} B but expected {file_size} B!")
                        assert size == file_size

                    file.close()

                    csv_file.write(','.join([str(chunk_size), str(size), str(t1 - t0)]).encode() + b'\n')
                    csv_file.flush()

                    print(
                        f"Read {size / 1e6:.2f} MB in {chunk_size_in_KiB} KiB chunks with {open_file_name} "
                        f"in {t1-t0:.2f} s -> {size/(t1-t0)/1e6:.2f} MB/s"
                    )

    @classmethod
    def plot(cls, data_file_path, **kwargs):
        pass


class BenchmarkExample:
    DATA_FILE_NAME = ".csv"

    @classmethod
    def benchmark(cls, data_file_path, repetitions, **kwargs):
        if not data_file_path:
            data_file_path = cls.DATA_FILE_NAME

        # ...

    @classmethod
    def plot(cls, data_file_path, **kwargs):
        if not data_file_path:
            data_file_path = cls.DATA_FILE_NAME

        # ...

        # fig.tight_layout()
        # for extension in ['png', 'pdf']:
        #    fig.savefig(data_file_path.rsplit('.', maxsplit=1)[0] + '.' + extension, dpi=plot_dpi)


hostname = "127.0.0.1"
port = 22

target_folder = "/dev/shm/sftp_downloads/"
source_folder = "/dev/shm/sftp_shared/"
test_file = "silesia.tar.gz"
src_path = source_folder + test_file

plot_dpi = 300
figsize = (6, 4)

benchmarks = [
    BenchmarkFullRead,
    BenchmarkAsyncsshMaxRequests,
    BenchmarkFullWrite,
    BenchmarkSshfsMaxRequests,
    BenchmarkSshfsBlockSize,
    BenchmarkSshfsOverread,
]


def _parse_args(raw_args: Optional[list[str]] = None):
    parser = argparse.ArgumentParser(add_help=False, description="SSH backend benchmarking tool")

    parser.add_argument(
        '-h', '--help', action='help', default=argparse.SUPPRESS, help="Show this help message and exit."
    )
    parser.add_argument('--show', action='store_true', help="Display the generated plot before quitting.")
    parser.add_argument('-f', '--file', type=str, help="Output file for benchmark, input file for plot.")
    parser.add_argument('--test-file', type=str, default=test_file, help="File name to download or upload via SFTP.")
    parser.add_argument(
        '-r', '--repetitions', type=int, default=15, help="How often to repeat benchmarks for statistics."
    )
    parser.add_argument('-P', '--port', type=int, default=port, help="The port for the SSH server.")

    parser.add_argument('action', choices=["benchmark", "plot"])
    parser.add_argument('name', choices=[x.__name__ for x in benchmarks])

    return parser.parse_args(raw_args)


def main():
    args = _parse_args(sys.argv[1:])

    global test_file, src_path, port
    test_file = args.test_file
    src_path = os.path.join(source_folder, test_file)
    port = args.port

    getattr(globals()[args.name], args.action)(args.file, repetitions=args.repetitions)
    if args.show:
        plt.show()


if __name__ == '__main__':
    main()

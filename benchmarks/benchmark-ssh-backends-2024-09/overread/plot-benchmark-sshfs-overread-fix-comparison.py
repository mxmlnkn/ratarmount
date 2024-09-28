import pickle
import numpy as np
import matplotlib.pyplot as plt

file_size = 68238807 / 1e6

labels = {
    'fsspec_fs': 'fsspec.implementations.sftp.SFTPFileSystem',
    'sshfs_fs': 'fsspec/sshfs.SSHFileSystem',
}

def format_bytes(size):
    if size < 0:
        return str(size)

    # Format assuming that the value is an integer of KiB or MiB
    if size < 1024:
        return f"{size} B"

    size = size // 1024
    if size < 1024:
        return f"{size} KiB"

    size = size // 1024
    if size < 1024:
        return f"{size} MiB"

    size = size // 1024
    return f"{size} GiB"

labels = {
    'fsspec_fs': 'fsspec.implementations.sftp.SFTPFileSystem',
    'sshfs_fs': 'fsspec/sshfs.SSHFileSystem',
}

with open("benchmark-sshfs.times.pickle", 'rb') as file:
    data = pickle.load(file)

def compute_statistics(t):
    return np.mean(t), np.std(t, ddof=1)

def compute_bandwidths(t):
    return {
        chunk_size: compute_statistics(file_size / np.array(times))
        for chunk_size, times in t.items()
    }

results = {
    label: compute_bandwidths(times_per_chunk_size)
    for label, times_per_chunk_size in data.items()
}
chunk_sizes = [np.sort(list(times_per_chunk_size.keys())) for label, times_per_chunk_size in data.items()]
assert all(np.all(chunk_sizes[0] == sizes) for sizes in chunk_sizes)
chunk_sizes = chunk_sizes[0]
chunk_sizes = np.concatenate([chunk_sizes[chunk_sizes >= 0], chunk_sizes[chunk_sizes < 0]])

fig = plt.figure(figsize=(6, 4))
ax = fig.add_subplot(111, xlabel = "Chunk Size", ylabel = "Bandwidth / (MB/s)", ylim=[0, 45])

width = 0.3
i_bar = 0
bar_positions = np.arange(len(chunk_sizes))
for label, stats_per_chunk_size in results.items():
    times_mean = [stats_per_chunk_size[size][0] for size in chunk_sizes]
    times_std = [stats_per_chunk_size[size][1] for size in chunk_sizes]
    ax.bar(
        bar_positions - width / 2 + i_bar * width, times_mean,
        yerr=times_std, width=width, label=labels[label], capsize=2
    )
    i_bar += 1

ax.set_xticks(bar_positions)
ax.set_xticklabels([format_bytes(size) for size in chunk_sizes])
ax.legend(loc='upper left')

fig.tight_layout()
fig.savefig("plot-benchmark-sshfs.png", dpi=300)
fig.savefig("plot-benchmark-sshfs.pdf", dpi=300)
plt.show()

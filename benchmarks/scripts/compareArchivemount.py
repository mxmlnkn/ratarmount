#!/usr/bin/env python3

import os
import sys

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D
import pandas as pd


lineStyles = ['-', ':', (0, (1, 7)), (0, (4, 3, 1, 3)), '--']
colors = ['tab:blue', 'tab:red', 'tab:purple', 'r']  # https://matplotlib.org/3.1.1/_images/dflt_style_changes-1.png
markers = ['+', 'o', '*', 'x']


def axisValueReduction(ax, axis, reduction, init):
    result = init
    # this is bugged when using axvline or axhline, because it doesn't ignore
    # the huge values set by those functions. Workaround: Call autoRange
    # before ax[v|h]line, but that is not always wanted
    for line in ax.get_lines():
        # have to use numpy here, because of:
        # https://stackoverflow.com/questions/4237914/python-max-min-builtin-functions-depend-on-parameter-order
        x = np.array(line.get_xdata(), dtype=float)
        y = np.array(line.get_ydata(), dtype=float)

        # mask not only NaNs per each x,y, but also mask all y-values whose
        # corresponding x-values are NaN!
        unmasked = np.logical_and(np.logical_not(np.isnan(x)), np.logical_not(np.isnan(y)))
        x = x[unmasked]
        y = y[unmasked]
        unmasked = np.array([True] * len(x), dtype=bool)
        if axis == 'x' and ax.get_xscale() == 'log':
            np.logical_and(unmasked, x > 0)
        if axis == 'y' and ax.get_yscale() == 'log':
            np.logical_and(unmasked, y > 0)

        result = reduction(np.concatenate([[result], (x if axis == 'x' else y)[unmasked]]))
    return result


def readLabelsFromFirstComment(fileName):
    with open(fileName) as file:
        for line in file:
            line = line.strip()
            if line[0] == '#':
                return line[1:].strip().split(' ')


def loadData(fileName):
    """Returns a nested dict with keys in order of dimension: tool, command, compression"""
    labels = None
    data = {}

    with open(fileName) as file:
        for line in file:
            line = line.strip()
            if not line:
                continue
            if line[0] == '#':
                if labels is None:
                    labels = line[1:].strip().split(' ')
            else:
                row = line.split(';')

                tool = row[0].strip('"')
                if tool not in data:
                    data[tool] = {}

                # Not interested in command arguments like cat <file>
                # This assumed that all "tools" are devoid of spaces but now I added "ratarmount -P 24" as a "tool"
                command = row[1].strip('"')
                if command.startswith("ratarmount -P"):
                    command = ' '.join(command.split(' ')[:3])
                else:
                    command = command.split(' ')[0]

                if command.startswith(tool):
                    command = "mount"

                if command not in data[tool]:
                    data[tool][command] = {}

                compression = row[2].strip('"')
                if compression not in data[tool][command]:
                    data[tool][command][compression] = []

                if labels[-1] == 'startTime':
                    data[tool][command][compression] += [np.array(row[3:-1], dtype=float)]
                else:
                    data[tool][command][compression] += [np.array(row[3], dtype=float)]

    for key, value in data.items():
        for command, values in value.items():
            for compression, values2 in values.items():
                data[key][command][compression] = np.array(values2, dtype=float).transpose()

    return labels[3:], data


def plotBenchmark(labels, data, ax, command, metric, tools, scalingFactor=1):
    compressions = None
    fileSizes = None
    xs = np.array([])
    ys = np.array([])

    for i, tool in enumerate(tools):
        # Push ratarmount lines to front because that is what is of interest
        zorder = 3 if 'ratarmount' in tool else 2

        if tool not in data:
            print(f"[Warning] Did not find tool '{tool}' in data!")
            continue

        if command not in data[tool]:
            print(f"[Warning] Did not find command '{command}' in data[{tool}]!")
            continue

        if compressions is None:
            compressions = sorted(data[tool][command].keys())
        else:
            if compressions != sorted(data[tool][command].keys()):
                print("[Warning] Expected same set of compressions for all other parameters but got:")
                print("[Warning]", compressions, "!=", sorted(data[tool][command].keys()))
                print("[Warning] Cannot plot data for:", labels, tool, command, metric)
                continue

        for j, compression in enumerate(compressions):
            values = data[tool][command][compression]

            newFileSizes = np.sort(np.unique(values[labels.index("nBytesPerFile")]))
            if fileSizes is None:
                fileSizes = newFileSizes
            else:
                assert np.all(fileSizes == newFileSizes)

            files = values[labels.index("nFolders")] * values[labels.index("nFilesPerFolder")]
            metricValues = values[labels.index(metric)]

            # Force zero-values for durations, which only have precision 0.01s, to 0.01s
            # to avoid "invisible" values when using log plot.
            if metric == 'duration/s' and ax.get_yscale() == 'log':
                metricValues[metricValues == 0.0] = 0.01

            bytesPerFile = values[labels.index("nBytesPerFile")]

            for k, nBytesPerFile in enumerate(fileSizes):
                iSorted = np.argsort(files)
                iSelected = bytesPerFile[iSorted] == nBytesPerFile

                x = files[iSorted][iSelected]
                y = metricValues[iSorted][iSelected] * scalingFactor

                xu = np.sort(np.unique(x))
                if len(xu) == len(x):
                    ax.plot(x, y, linestyle=lineStyles[j], color=colors[i], marker=markers[k], zorder=zorder)
                else:
                    ymedian = np.array([np.median(y[x == xi]) for xi in xu])
                    ymean = np.array([np.mean(y[x == xi]) for xi in xu])
                    ymin = np.array([np.min(y[x == xi]) for xi in xu])
                    ymax = np.array([np.max(y[x == xi]) for xi in xu])

                    if False:  # command == 'cat':
                        lines = ax.errorbar(
                            xu,
                            ymean,
                            [ymean - ymin, ymax - ymean],
                            capsize=3,
                            linestyle=lineStyles[j],
                            color=colors[i],
                            marker=markers[k],
                            zorder=zorder,
                        )
                        lines[-1][0].set_linestyle(lineStyles[j])
                    else:
                        # ymedian? ymin? ymax? ymean?
                        yToPlot = ymax if command == 'mount' else ymedian
                        lines = ax.plot(
                            xu, yToPlot, linestyle=lineStyles[j], color=colors[i], marker=markers[k], zorder=zorder
                        )
                        lines[0].set_linestyle(lineStyles[j])

                xs = np.append(xs, x)
                ys = np.append(ys, y)

    x = 10 ** np.linspace(np.log10(np.min(xs)), np.log10(np.max(xs)))
    y = x
    y = 5 * y / y[-1] * np.max(ys)
    ax.plot(x[y > np.min(ys)], y[y > np.min(ys)], color='k', label="linear scaling")

    for i, tool in enumerate(tools):
        ax.plot([None], [None], color=colors[i], label=tool)
    for j, compression in enumerate(compressions):
        ax.plot([None], [None], linestyle=lineStyles[j], color='0.5', label=compression)
    for k, nBytesPerFile in enumerate(fileSizes):
        ax.plot(
            [None],
            [None],
            linestyle='',
            marker=markers[k],
            color='0.5',
            label=f"{int(nBytesPerFile)}B per File",
        )


def plotComparison(fileName):
    labels, data = loadData(fileName)

    availableTools = data.keys()
    tools = [
        'archivemount',
        'ratarmount -P 24' if 'ratarmount -P 24' in availableTools else 'ratarmount',
        'fuse-archive',
    ]

    fig = plt.figure(figsize=(10, 8))
    ax = fig.add_subplot(
        221,
        title="Peak Resident Memory Usage During Mounting",
        xlabel="Number of Files in Archive",
        ylabel="Memory Usage / MiB",
        xscale='log',
        yscale='log',
    )

    plotBenchmark(labels, data, ax, "mount", "peakRssMemory/kiB", tools, scalingFactor=1.0 / 1024)

    ax.legend(loc='best')

    ax = fig.add_subplot(
        222,
        title="Time Required for (First) Mounting",
        xlabel="Number of Files in Archive",
        ylabel="Runtime / s",
        xscale='log',
        yscale='log',
    )

    plotBenchmark(labels, data, ax, "mount", "duration/s", tools)

    xmin = axisValueReduction(ax, 'x', np.nanmin, float('+inf'))
    xmax = axisValueReduction(ax, 'x', np.nanmax, float('-inf'))
    ymin = axisValueReduction(ax, 'y', np.nanmin, float('+inf'))
    ymax = axisValueReduction(ax, 'y', np.nanmax, float('-inf'))
    x = 10 ** np.linspace(np.log10(xmin), np.log10(xmax))
    y = x**2
    y = y / y[-1] * ymax / 2000
    ax.plot(x[y > ymin], y[y > ymin], color='k', linestyle='--', label="quadratic scaling")

    ax.legend([Line2D([], [], linestyle='--', color='k')], ['quadratic scaling'], loc='best')

    ax = fig.add_subplot(
        223,
        title="Time Required to Get Contents of One File",
        xlabel="Number of Files in Archive",
        ylabel="Runtime / s",
        xscale='log',
        yscale='log',
    )

    plotBenchmark(labels, data, ax, "cat", "duration/s", tools)

    # ax = fig.add_subplot( 224,
    #    title = "Time Required for Getting Metadata of One File",
    #    xlabel = "Number of Files in Archive",
    #    ylabel = "Runtime / s",
    #    xscale = 'log',
    #    yscale = 'log',
    # )
    #
    # plotBenchmark( fileName, ax, "stat", "duration/s" )

    ax = fig.add_subplot(
        224,
        title="Time Required for Listing All Files",
        xlabel="Number of Files in Archive",
        ylabel="Runtime / s",
        xscale='log',
        yscale='log',
    )

    plotBenchmark(labels, data, ax, "find", "duration/s", tools)

    fig.tight_layout()
    fileName = 'archivemount-comparison.png'
    fig.savefig(fileName, dpi=150)
    print("Written out:", fileName)


def plotRatarmountParallelComparison(fileName, compression):
    suffix = 'tar.' + compression if compression else 'tar'

    fig = plt.figure(figsize=(6, 4))

    ax = fig.add_subplot(
        111,
        title="Speedup for Listing all Files After 'readdir' Improvement"
        if compression == 'find'
        else f"Speedup for ratarmount -P 24 for (First) Mounting {suffix}",
        xlabel="Number of Files in Archive",
        ylabel="Speedup",
        xscale='log',
        yscale='linear',
    )

    df = pd.read_csv(fileName, comment='#', sep=';', names=readLabelsFromFirstComment(fileName))

    if df.empty:
        print(f"[Warning] Did not data for ratarmount tools.")
        return False

    if compression == 'find':
        df = df.loc[df.loc[:, 'command'].str.contains('find')]  #
        if df.empty:
            print("[Warning] Could not find 'find' command")
            return False
        df = df[df.loc[:, 'compression'] == 'tar']
        df = df.loc[df.loc[:, 'tool'].str.startswith('ratarmount')]
        if df.empty:
            return False
    else:
        df = df.loc[(df.loc[:, 'compression'] == suffix) & (df.loc[:, 'nBytesPerFile'] != 4096)]
        if df.empty:
            print(f"[Warning] Did not find data for compression {suffix}.")
            return False
        df = df.loc[df.loc[:, 'command'].str.startswith('ratarmount')]
        if df.empty:
            return False

    def getDurationPerFileCount(df, tool, nBytesPerFile):
        sdf = df.loc[(df.loc[:, 'nBytesPerFile'] == nBytesPerFile) & (df.loc[:, 'tool'] == tool)]
        if sdf.empty:
            return None

        nFiles = sdf.apply(lambda row: row['nFolders'] * row['nFilesPerFolder'], axis=1)
        sdf = sdf.assign(**{'nFiles': nFiles})

        sdf = sdf.loc[:, ['nFiles', 'duration/s']]
        sdf = sdf.groupby('nFiles')
        sdf = sdf.min().sort_values('nFiles')

        return (
            sdf.index.to_numpy(),
            sdf.loc[:, 'duration/s'].to_numpy(),
        )

    for k, nBytesPerFile in enumerate(df.loc[:, 'nBytesPerFile'].unique()):
        resultSerial = getDurationPerFileCount(df, 'ratarmount', nBytesPerFile)
        resultParallel = getDurationPerFileCount(df, 'ratarmount -P 24', nBytesPerFile)
        if resultSerial is None or resultParallel is None:
            continue
        nFilesSerial, durationSerial = resultSerial
        nFilesParallel, durationParallel = resultParallel

        minLen = min(len(nFilesSerial), len(nFilesParallel))
        nFilesSerial = nFilesSerial[:minLen]
        nFilesParallel = nFilesParallel[:minLen]
        durationSerial = durationSerial[:minLen]
        durationParallel = durationParallel[:minLen]

        assert np.all(nFilesSerial == nFilesParallel)

        ax.plot(
            nFilesSerial,
            durationSerial / durationParallel,
            linestyle='--',
            color=colors[1],
            marker=markers[k],
            label=f"{nBytesPerFile}B per file",
        )

    ax.set_ylim([0, ax.get_ylim()[1]])
    ax.set_yticks(list(ax.get_yticks()) + [1])
    ax.grid(axis='y')

    ax.legend(loc='best')

    fig.tight_layout()
    fileName = f'parallel-{compression + "-" if compression else ""}ratarmount-comparison.png'
    fig.savefig(fileName, dpi=150)
    print("Written out:", fileName)
    return True


def plotAccessLatency(fileName):
    fileName = dataFile
    labels, data = loadData(fileName)

    availableTools = data.keys()
    tools = [
        'archivemount',
        'ratarmount -P 24' if 'ratarmount -P 24' in availableTools else 'ratarmount',
        'fuse-archive',
    ]

    fig = plt.figure(figsize=(6, 4))
    ax = fig.add_subplot(
        111,
        title="Time Required to Get Contents of One File",
        xlabel="Number of Files in Archive",
        ylabel="Runtime / s",
        xscale='log',
        yscale='log',
    )

    plotBenchmark(labels, data, ax, "cat", "duration/s", tools)

    ax.legend(loc='best')

    fig.tight_layout()
    fileName = 'cat-file-latency.png'
    fig.savefig(fileName, dpi=150)
    print("Written out:", fileName)


if __name__ == "__main__":
    if len(sys.argv) != 2 or not os.path.isfile(sys.argv[1]):
        print("First argument must be path to data file")
        sys.exit(1)

    dataFile = sys.argv[1]

    plotAccessLatency(dataFile)
    plotComparison(dataFile)
    for compression in ['', 'bz2', 'gz', 'xz', 'find']:
        plotRatarmountParallelComparison(dataFile, compression)

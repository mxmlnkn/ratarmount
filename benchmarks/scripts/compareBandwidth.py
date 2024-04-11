#!/usr/bin/env python3

import os
import sys

import numpy as np
import matplotlib.pyplot as plt
from matplotlib.lines import Line2D


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

                # Not interested in command arguments like cat <file>
                # This assumed that all "tools" are devoid of spaces but now I added "ratarmount -P 24" as a "tool"
                command = row[1].strip('"')

                if '-T 0' in command:
                    tool += ' -T 0'

                if '--decompress' in command or ' -d ' in command or command.startswith( 'cat ' ):
                    command = "read"
                elif '--keep' in command:
                    command = "compress"
                else:
                    if command.startswith("ratarmount -P"):
                        command = ' '.join(command.split(' ')[:3])
                    else:
                        command = command.split(' ')[0]

                if tool not in data:
                    data[tool] = {}
                if command not in data[tool]:
                    data[tool][command] = {}

                compression = row[2].strip('"')
                if compression not in data[tool][command]:
                    data[tool][command][compression] = []

                if labels[-1] == 'startTime':
                    data[tool][command][compression] += [np.array(row[3:-1], dtype='float')]
                else:
                    data[tool][command][compression] += [np.array(row[3], dtype='float')]

    for tool, value in data.items():
        for command, values in value.items():
            for compression, values2 in values.items():
                data[tool][command][compression] = np.array(values2, dtype='float').transpose()

    return labels[3:], data

compressionToIndex = {
    'tar' : 0,
    'tar.bz2' : 1,
    'tar.gz' : 2,
    'tar.xz' : 3,
    'tar.zst' : 4,
}

def plotBenchmark(labels, data, ax, command, metric, tools, scalingFactor=1, xScalingFactor=1):
    fileSizes = None
    xs = np.array([])
    ys = np.array([])

    originalCommand = command
    if command == 'bandwidth':
        command = 'read'

    for i, tool in enumerate(tools):
        # Push ratarmount lines to front because that is what is of interest
        zorder = 3 if 'ratarmount' in tool else 2

        if tool not in data:
            print(f"[Warning] Did not find tool '{tool}' in data!")
            continue

        if command not in data[tool]:
            print(f"[Warning] Did not find command '{command}' in data[{tool}]!")
            continue

        compressions = sorted(data[tool][command].keys())

        for compression in compressions:
            j = compressionToIndex[compression]
            values = data[tool][command][compression]

            metricValues = values[labels.index(metric)]

            # Force zero-values for durations, which only have precision 0.01s, to 0.01s
            # to avoid "invisible" values when using log plot.
            if metric == 'duration/s' and ax.get_yscale() == 'log':
                metricValues[metricValues == 0.0] = 0.01

            fileSizes = values[labels.index("fileSize/B")]
            iSorted = np.argsort(fileSizes)

            x = fileSizes[iSorted]
            y = metricValues[iSorted]
            if originalCommand == 'bandwidth':
                y = x / y
            x *= xScalingFactor
            y *= scalingFactor

            marker = 'o'

            xu = np.sort(np.unique(x))
            if len(xu) == len(x):
                ax.plot(x, y, linestyle=lineStyles[j], color=colors[i], marker=marker, zorder=zorder)
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
                        marker=marker,
                        zorder=zorder,
                    )
                    lines[-1][0].set_linestyle(lineStyles[j])
                else:
                    # ymedian? ymin? ymax? ymean?
                    yToPlot = ymax if command == 'mount' else ymedian
                    lines = ax.plot(
                        xu, yToPlot, linestyle=lineStyles[j], color=colors[i], marker=marker, zorder=zorder
                    )
                    lines[0].set_linestyle(lineStyles[j])

            xs = np.append(xs, x)
            ys = np.append(ys, y)

    for i, tool in enumerate(tools):
        ax.plot([None], [None], color=colors[i], label=tool)
    for j, compression in enumerate(compressions):
        ax.plot([None], [None], linestyle=lineStyles[j], color='0.5', label=compression)


# Copy-paste from plotBenchmark but:
#  - color = green
#  - tools hardcoded
#  - compression determined by tool
def plotStandardTools(labels, data, ax, command, metric, scalingFactor=1, xScalingFactor=1):
    fileSizes = None
    xs = np.array([])
    ys = np.array([])

    originalCommand = command
    if command == 'bandwidth':
        command = 'read'

    tools = [
        # fmt: off
        ( 'tab:brown', 'bzip2'  , '.bz2' ),
        ( 'tab:green', 'lbzip2' , '.bz2' ),
        ( 'tab:brown', 'gzip'   , '.gz'  ),
        ( 'tab:green', 'pigz'   , '.gz'  ),
        ( 'tab:brown', 'xz'     , '.xz'  ),
        ( 'tab:green', 'pixz'   , '.xz'  ),
        ( 'tab:brown', 'zstd'   , '.zst' )
        # fmt: on
    ]

    for color, tool, compression in tools:
        compression = 'tar' + compression

        # Push ratarmount lines to front because that is what is of interest
        zorder = 3 if 'ratarmount' in tool else 2

        if tool not in data:
            print(f"[Info] Did not find tool '{tool}' in data!")
            continue

        if command not in data[tool]:
            print(f"[Warning] Did not find command '{command}' in data[{tool}]!")
            continue

        j = compressionToIndex[compression]
        values = data[tool][command][compression]

        metricValues = values[labels.index(metric)]

        # Force zero-values for durations, which only have precision 0.01s, to 0.01s
        # to avoid "invisible" values when using log plot.
        if metric == 'duration/s' and ax.get_yscale() == 'log':
            metricValues[metricValues == 0.0] = 0.01

        fileSizes = values[labels.index("fileSize/B")]
        iSorted = np.argsort(fileSizes)

        x = fileSizes[iSorted]
        y = metricValues[iSorted]
        if originalCommand == 'bandwidth':
            y = x / y
        x *= xScalingFactor
        y *= scalingFactor

        marker = 'o'

        xu = np.sort(np.unique(x))
        if len(xu) == len(x):
            ax.plot(x, y, linestyle=lineStyles[j], color=color, marker=marker, zorder=zorder, label=tool)
        else:
            ymedian = np.array([np.median(y[x == xi]) for xi in xu])
            #ymean = np.array([np.mean(y[x == xi]) for xi in xu])
            #ymin = np.array([np.min(y[x == xi]) for xi in xu])
            ymax = np.array([np.max(y[x == xi]) for xi in xu])

            # ymedian? ymin? ymax? ymean?
            yToPlot = ymax if command == 'mount' else ymedian
            lines = ax.plot(
                xu, yToPlot, linestyle=lineStyles[j], color=color, marker=marker, zorder=zorder, label=tool
            )
            lines[0].set_linestyle(lineStyles[j])

        xs = np.append(xs, x)
        ys = np.append(ys, y)


def plotReadingComparison(fileName):
    labels, data = loadData(fileName)

    tools = [
        'archivemount',
        'ratarmount -P 24',
        'fuse-archive',
    ]

    fig = plt.figure(figsize=(10, 8))
    ax = fig.add_subplot(
        221,
        title="Peak Resident Memory Usage During Reading",
        xlabel="Decompressed File Size In Archive / kB",
        ylabel="Memory Usage / MiB",
        xscale='log',
        yscale='log',
    )

    plotBenchmark(labels, data, ax, "read", "peakRssMemory/kiB", tools,
                  xScalingFactor = 1.0 / 1000, scalingFactor=1.0 / 1024)
    ax.grid(axis='y')

    ax.legend(loc='best')

    ax = fig.add_subplot(
        222,
        title="Reading Times",
        xlabel="Decompressed File Size In Archive / kB",
        ylabel="Runtime / s",
        xscale='log',
        yscale='log',
    )

    plotBenchmark(labels, data, ax, "read", "duration/s", tools, xScalingFactor = 1.0 / 1000 )
    ax.grid(axis='y')

    xmin = axisValueReduction(ax, 'x', np.nanmin, float('+inf'))
    xmax = axisValueReduction(ax, 'x', np.nanmax, float('-inf'))
    ymin = axisValueReduction(ax, 'y', np.nanmin, float('+inf'))
    ymax = axisValueReduction(ax, 'y', np.nanmax, float('-inf'))

    # add linear scaling
    x = 10 ** np.linspace(np.log10(xmin), np.log10(xmax))
    y = x
    y = y / y[-1] * ymax / 50
    ax.plot(x[y > ymin], y[y > ymin], color='k', linestyle='-', label="linear scaling")

    # add quadratic scaling
    x = 10 ** np.linspace(np.log10(xmin), np.log10(xmax) - 4)
    y = x ** 2
    y = y / y[-1] * ymax
    ax.plot(x[y > ymin], y[y > ymin], color='k', linestyle='--', label="quadratic scaling")
    ax.legend([Line2D([], [], linestyle='-', color='k'),
               Line2D([], [], linestyle='--', color='k')],
              ['linear scaling', 'quadratic scaling'], loc='best')

    ax = fig.add_subplot(
        223,
        title="Reading Speeds",
        xlabel="Decompressed File Size In Archive / kB",
        ylabel="Decompressed Bandwidth / (MB/s)",
        xscale='log',
        yscale='log',
    )

    plotBenchmark(labels, data, ax, "bandwidth", "duration/s", tools, xScalingFactor = 1.0 / 1000,
                  scalingFactor = 1.0 / 1000**2 )

    ax.grid(axis='y')
    axBandwidth = ax

    ax = fig.add_subplot(
        224,
        title="Reading Speeds",
        xlabel="Decompressed File Size In Archive / kB",
        ylabel="Decompressed Bandwidth / (MB/s)",
        xscale='log',
        yscale='log',
    )

    plotStandardTools(labels, data, ax, "bandwidth", "duration/s", xScalingFactor = 1.0 / 1000,
                      scalingFactor = 1.0 / 1000**2 )
    ax.set_ylim(axBandwidth.get_ylim())
    ax.legend(loc='best')
    ax.grid(axis='y')

    fig.tight_layout()
    fileName = 'bandwidth-comparison.png'
    fig.savefig(fileName, dpi=150)
    print("Written out:", fileName)


def plotBandiwdths(fileName):
    labels, data = loadData(fileName)

    tools = [
        'archivemount',
        'ratarmount -P 24',
        'fuse-archive',
    ]

    fig = plt.figure(figsize=(6, 4))

    ax = fig.add_subplot(
        111,
        title="Reading Speeds",
        xlabel="Decompressed File Size In Archive / kB",
        ylabel="Decompressed Bandwidth / (MB/s)",
        xscale='log',
        yscale='log',
    )

    plotBenchmark(labels, data, ax, "bandwidth", "duration/s", tools, xScalingFactor = 1.0 / 1000,
                  scalingFactor = 1.0 / 1000**2 )
    ax.grid(axis='y')
    ax.legend(loc='best')

    fig.tight_layout()
    fileName = 'pure-bandwidth-comparison.png'
    fig.savefig(fileName, dpi=150)
    print("Written out:", fileName)


if __name__ == "__main__":
    if len(sys.argv) != 2 or not os.path.isfile(sys.argv[1]):
        print("First argument must be path to data file")
        sys.exit(1)

    dataFile = sys.argv[1]

    plotBandiwdths(dataFile)
    plotReadingComparison(dataFile)

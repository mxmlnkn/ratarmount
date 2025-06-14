#!/usr/bin/env python3

import collections
import math
import os
import sys
import time

import matplotlib.pyplot as plt
import numpy as np
from scipy.optimize import curve_fit


useNumpyUnique = True


def computeEntropy(data: bytes) -> float:
    if not data:
        return 0.0
    # https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.entropy.html
    probabilities = [byteFrequency / len(data) for byteFrequency in collections.Counter(data).values()]
    return -sum(p * math.log2(p) for p in probabilities)


def computeEntropyForEach(data: bytes, ns):
    # https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.entropy.html
    result = []
    last = 0
    c = collections.Counter()  # type:ignore
    adata = np.frombuffer(data, dtype=np.uint8)
    counts = np.zeros(256)
    for n in ns:
        if useNumpyUnique:
            unique, newCounts = np.unique(adata[last:n], return_counts=True)
            counts[unique] += newCounts
            p = counts / n
            mask = p != 0
            result.append(-np.sum(p[mask] * np.log2(p[mask])))
        else:
            c.update(data[last:n])
            p = np.array(list(c.values())) / n
            result.append(-np.sum(p * np.log2(p)))
        last = n
    return result


def convergenceRate(x, m):
    # Must behave 1/x for for x -> inf as found empirically and f(1)=8
    # because the entropy of a single character is 0 by definition.
    return 8 * (1 + m) / (np.floor(x) + m)


def plotForData(data, saveName: str):
    nStop = len(data)
    stopPower = int(np.log2(nStop))

    x = np.floor(2 ** np.linspace(0, stopPower, stopPower * 5, endpoint=True))
    y = 8 - np.array(computeEntropyForEach(data, [int(n) for n in x]))
    diffData = np.ediff1d(np.frombuffer(data, dtype=np.uint8)).tobytes()
    ydiff = 8 - np.array(computeEntropyForEach(diffData, [int(n) for n in x]))

    fig = plt.figure(figsize=(8, 6))

    ax = fig.add_subplot(111, xscale='log', yscale='log', xlabel="Number of Bytes n", ylabel="8 - Entropy")
    ax.plot(x, y, label='8 - Entropy')
    ax.plot(x, ydiff, label='8 - Consec. Diff. Entropy')
    for m in [16, 24, 32, 40]:
        ax.plot(x, convergenceRate(x, m), label=f"{8 * (1 + m)}/(n+{m})", linestyle='--', alpha=0.5)

    xSmall = np.arange(1, 128)
    ax.plot(xSmall, 8 - np.log2(xSmall), label="8 - log2(n)")
    ax.legend(loc='best')

    fig.tight_layout()
    fig.savefig(saveName + '.png')
    fig.savefig(saveName + '.pdf')


if len(sys.argv) > 1:
    plotForData(open(sys.argv[1], 'rb').read(), sys.argv[1] + ".entropy")
    plt.show()


def plotRandomDataAnalysis():
    # The more often it is repeated, the larger the min,max grow of course.
    # This gives us an estimate to how often the check will fail!
    # I.e., if my estimated function still envelopes the highest (8 - entropy) value,
    # then I can estimate that my check will work except for 1 in a million or less times!
    # However, benchmarking 1k already takes ~1 min. I guess I could reduce nStop somewhat
    # to save me the trouble of analyzing how min/max grows with nRepeat, although,
    # that would be interesting on its own and should be easy to compute with the existing
    # iteration loop and not much overhead. -> do it.
    #  -> It seems to be eerily stable if I am doing nothing wrong ... It only changes
    #     by up to 2x in a range of 1 to 1000 repeats and seems to saturate there,
    #     even in log-log scale!
    nRepeat = 1000
    stopPower = 20
    nStop = 1 << stopPower

    ns = np.floor(2 ** np.linspace(0, int(np.log2(nStop)), (int(np.log2(nStop)) - 0) * 5, endpoint=True))

    ys = []
    ydiffs = []
    ymins = []
    ymaxs = []
    t0 = time.time()
    for _i in range(nRepeat):
        sys.stdout.write(".")
        sys.stdout.flush()
        data = os.urandom(nStop)
        # ys.append([computeEntropy(data[: int(n)]) for n in x])
        ys.append(computeEntropyForEach(data, [int(n) for n in ns]))

        diffData = np.ediff1d(np.frombuffer(data, dtype=np.uint8)).tobytes()
        ydiffs.append(computeEntropyForEach(diffData, [int(n) for n in ns]))

        ymins.append(np.min(ys, axis=0))
        ymaxs.append(np.max(ys, axis=0))
    t1 = time.time()
    print()
    print(f"Computation took: {t1 - t0:.3f} s")
    # np.unique:
    #   nRepeat = 100 -> 4.330 s
    #   nRepeat = 200 -> 8.484 s
    #   nRepeat = 1000 -> 8.484 s
    # collections.counter
    #   nRepeat = 100 -> 5.099 s
    #   nRepeat = 200 -> 10.154 s

    # ! At this point we are beginning to look at the convergence to 8 bits entropy per byte of random data,
    # not the entropy itself. I chose this because it makes the log-log plot show a nice linear for the 1/x convergence.
    ys = 8 - np.array(ys)
    ydiffs = 8 - np.array(ydiffs)
    # Swap min, max because we invert ys with -!
    ymins, ymaxs = 8 - np.array(ymaxs), 8 - np.array(ymins)

    fig = plt.figure(figsize=(8, 6))

    ax1 = fig.add_subplot(111, xscale='log', yscale='log', xlabel="Number of Bytes n", ylabel="8 - Entropy")
    # ax1.errorbar(x, np.mean(ys, axis=0), yerr=np.std(ys, axis=0), label='8 - Entropy')
    ymed = np.median(ys, axis=0)
    ax1.errorbar(ns, ymed, yerr=(np.max(ys, axis=0) - ymed, ymed - np.min(ys, axis=0)), capsize=2, label='8 - Entropy')
    yDiffTopBottom = (np.max(ydiffs, axis=0) - ymed, ymed - np.min(ydiffs, axis=0))
    ax1.errorbar(ns, ymed, yerr=yDiffTopBottom, capsize=2, label='8 - Consec. Diff. Entropy')
    # plt.plot(x, x**-1)
    for m in [16, 24, 32, 40]:
        ax1.plot(ns, convergenceRate(ns, m), label=f"{8 * (1 + m)}/(n+{m})", linestyle='--', alpha=0.5)
    result = curve_fit(convergenceRate, ns, ymed)
    print("M", result[0], "Cov:", result[1])
    # ax1.plot(x, convergenceRate(x, mopt), label=f"{8 * (1+mopt)}/(n+{mopt})")

    xSmall = np.arange(1, 128)
    ax1.plot(xSmall, 8 - np.log2(xSmall), label="8 - log2(n)")
    ax1.legend(loc='best')

    # def log2ConvergenceRate(x, m):
    #    # Must behave 1/x for for x -> inf as found empirically and f(1)=8
    #    # because the entropy of a single character is 0 by definition.
    #    return 8 * (1 + m) / (2**x + m)
    #
    # ax2 = fig.add_subplot(122, xlabel="Log2(Number of Bytes)", ylabel="Log2(8 - Entropy)")
    # lx = np.log2(x)
    # ly = np.log2(ymean)
    # ax2.plot(lx, ly, label = f"{8 * (1+mopt)}/(x+{mopt})")
    # mopt, mcov = curve_fit(convergenceRate, lx, ly)
    # print("M", mopt, "Cov:", mcov)
    # ax2.legend(loc='best')

    fig.tight_layout()
    fname = f"entropy-{stopPower},th-power-of-2,{nRepeat}-repeats,random"
    plt.savefig(fname + ".pdf")
    plt.savefig(fname + ".png")

    fig = plt.figure(figsize=(12, 6))
    ax = fig.add_subplot(121, xscale='log', yscale='log', xlabel="Number of Repetitions")
    ax.plot(np.arange(nRepeat), ymaxs[:, -1] - ymed[-1], label=f"E_max - E_median at {ns[-1] // 1024} KiB")
    ax.plot(np.arange(nRepeat), ymed[-1] - ymins[:, -1], label=f"E_max - E_median at {ns[-1] // 1024} KiB")
    ax.legend(loc='best')

    ax = fig.add_subplot(122, xscale='log', yscale='log', xlabel="Number of Repetitions")
    i = len(ns) // 2
    ax.plot(np.arange(nRepeat), ymaxs[:, i] - ymed[i], label=f"E_max - E_median at {ns[i] // 1024} KiB")
    ax.plot(np.arange(nRepeat), ymed[i] - ymins[:, i], label=f"E_max - E_median at {ns[i] // 1024} KiB")
    ax.legend(loc='best')

    fig.tight_layout()
    fname = f"entropy-variance-scaling,{stopPower}-th-power-of-2,{nRepeat}-repeats,random"
    plt.savefig(fname + ".pdf")
    plt.savefig(fname + ".png")


plt.show()

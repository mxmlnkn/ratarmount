#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time


class ProgressBar:
    """Simple progress bar which keeps track of changes and prints the progress and a time estimate."""

    def __init__(self, maxValue: float):
        # fmt: off
        self.maxValue        = maxValue
        self.lastUpdateTime  = time.time()
        self.lastUpdateValue = 0.
        self.updateInterval  = 2.  # seconds
        self.creationTime    = time.time()
        # fmt: on

    def update(self, value: float) -> None:
        """Should be called whenever the monitored value changes. The progress bar is updated accordingly."""
        if self.lastUpdateTime is not None and (time.time() - self.lastUpdateTime) < self.updateInterval:
            return

        # Use whole interval since start to estimate time
        eta1 = int((time.time() - self.creationTime) / value * (self.maxValue - value))
        # Use only a shorter window interval to estimate time.
        # Accounts better for higher speeds in beginning, e.g., caused by caching effects.
        # However, this estimate might vary a lot while the other one stabilizes after some time!
        eta2 = int((time.time() - self.lastUpdateTime) / (value - self.lastUpdateValue) * (self.maxValue - value))
        print(
            f"Currently at position {value} of {self.maxValue} ({value / self.maxValue * 100.0:.2f}%). "
            f"Estimated time remaining with current rate: {eta2 // 60} min {eta2 % 60} s, "
            f"with average rate: {eta1 // 60} min {eta1 % 60} s.",
            flush=True,
        )

        self.lastUpdateTime = time.time()
        self.lastUpdateValue = value

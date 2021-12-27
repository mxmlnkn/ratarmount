#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import time


class ProgressBar:
    """Simple progress bar which keeps track of changes and prints the progress and a time estimate."""

    def __init__(self, maxValue: float):
        # fmt: off
        # Do not use thread_time here even if it is twice as fast because it does not count the time the thread
        # has been sleeping, which it does when it waits for I/O. This made the times incorrect for slow HDD accesses!
        self._getTime        = time.time
        self.maxValue        = maxValue
        self.lastUpdateTime  = self._getTime()
        self.lastUpdateValue = 0.
        self.updateInterval  = 2.  # seconds
        self.creationTime    = self._getTime()
        # fmt: on

    def update(self, value: float) -> None:
        """Should be called whenever the monitored value changes. The progress bar is updated accordingly."""
        if self.lastUpdateTime is not None and (self._getTime() - self.lastUpdateTime) < self.updateInterval:
            return

        # Use whole interval since start to estimate time
        percent = value / self.maxValue if self.maxValue != 0 else 1.0
        totalTime = self._getTime() - self.creationTime
        eta1 = int(totalTime / percent - totalTime if percent != 0 else 0)
        # Use only a shorter window interval to estimate time.
        # Accounts better for higher speeds in beginning, e.g., caused by caching effects.
        # However, this estimate might vary a lot while the other one stabilizes after some time!
        if value == self.lastUpdateValue:
            eta2 = 99999 * 60
        else:
            eta2 = int(
                (self._getTime() - self.lastUpdateTime) / (value - self.lastUpdateValue) * (self.maxValue - value)
            )
        print(
            f"Position {value} of {self.maxValue} ({percent * 100.0:.2f}%). "
            f"Remaining time: {eta2 // 60} min {eta2 % 60} s (current rate), "
            f"{eta1 // 60} min {eta1 % 60} s (average rate). "
            f"Spent time: {int(totalTime) // 60} min {int(totalTime) % 60} s",
            flush=True,
        )

        self.lastUpdateTime = self._getTime()
        self.lastUpdateValue = value

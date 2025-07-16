import logging
import sys
import time
from typing import Any, Optional

try:
    import rich.progress
    from rich.logging import RichHandler
except ImportError:
    RichHandler = None  # type: ignore


def _logging_uses_rich() -> bool:
    return RichHandler is not None and any(isinstance(handler, RichHandler) for handler in logging.getLogger().handlers)


class ProgressBar:
    """Simple progress bar which keeps track of changes and prints the progress and a time estimate."""

    def __init__(self, maxValue: float):
        # Do not use thread_time here even if it is twice as fast because it does not count the time the thread
        # has been sleeping, which it does when it waits for I/O. This made the times incorrect for slow HDD accesses!
        self._get_time = time.time
        self.value = 0.0
        self.maxValue = maxValue
        self.lastUpdateTime = self._get_time()
        self.lastUpdateValue = 0.0
        self.updateInterval = 2.0  # seconds
        self.creationTime = self._get_time()
        self._richProgress: Optional[rich.progress.Progress] = None
        self._taskID: Optional[Any] = None

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback) -> None:
        if self._richProgress is None:
            return
        if self._taskID is not None:
            self._richProgress.update(self._taskID, completed=self.value)
            self._richProgress.refresh()
            # Calling remove_task would also remove the progress bar, which I don't want.
        self._richProgress.stop()
        self._richProgress.__exit__(exception_type, exception_value, exception_traceback)
        self._richProgress = None
        self._taskID = None

    def __del__(self) -> None:
        if self._richProgress:
            self._richProgress.stop()
            self._richProgress.__exit__(None, None, None)
            self._richProgress = None

    def start(self) -> None:
        if 'rich.progress' in sys.modules and self._richProgress is None and _logging_uses_rich():
            self._richProgress = rich.progress.Progress(
                rich.progress.TextColumn("[progress.description]{task.description}"),
                rich.progress.BarColumn(bar_width=None),
                rich.progress.TaskProgressColumn(),
                rich.progress.DownloadColumn(),
                rich.progress.TimeElapsedColumn(),
                rich.progress.TimeRemainingColumn(elapsed_when_finished=True),
                # Auto-refreshing and capturing is only pain, especially as I have already taken care to call
                # update in compute-intensive loops. There is no reason to fuck the console up only for a progress bar.
                auto_refresh=False,
                redirect_stdout=False,
                redirect_stderr=False,
            )

        if self._richProgress:
            self._richProgress.start()
            if self._taskID is None:
                self._taskID = self._richProgress.add_task("Processing", total=self.maxValue)
            self.updateInterval = 0.2

    def stop(self) -> None:
        if self._richProgress:
            self._richProgress.stop()

    def update(self, value: float) -> None:
        """Should be called whenever the monitored value changes. The progress bar is updated accordingly."""
        self.value = value
        if self.lastUpdateTime is not None and (self._get_time() - self.lastUpdateTime) < self.updateInterval:
            return

        if self._richProgress is None and 'rich.progress' in sys.modules:
            self.start()

        if self._richProgress and self._taskID is not None:
            self._richProgress.update(self._taskID, completed=value)
            self._richProgress.refresh()
        else:
            # Use whole interval since start to estimate time
            percent = value / self.maxValue if self.maxValue != 0 else 1.0
            totalTime = self._get_time() - self.creationTime
            eta1 = int(totalTime / percent - totalTime if percent != 0 else 0)
            # Use only a shorter window interval to estimate time.
            # Accounts better for higher speeds in beginning, e.g., caused by caching effects.
            # However, this estimate might vary a lot while the other one stabilizes after some time!
            if value == self.lastUpdateValue:
                eta2 = 99999 * 60
            else:
                eta2 = int(
                    (self._get_time() - self.lastUpdateTime) / (value - self.lastUpdateValue) * (self.maxValue - value)
                )
            print(
                f"Position {value} of {self.maxValue} ({percent * 100.0:.2f}%). "
                f"Remaining time: {eta2 // 60} min {eta2 % 60} s (current rate), "
                f"{eta1 // 60} min {eta1 % 60} s (average rate). "
                f"Spent time: {int(totalTime) // 60} min {int(totalTime) % 60} s",
                flush=True,
            )

        self.lastUpdateTime = self._get_time()
        self.lastUpdateValue = value

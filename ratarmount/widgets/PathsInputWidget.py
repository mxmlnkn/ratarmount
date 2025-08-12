import os
from pathlib import Path
from typing import Callable, Optional

from qtpy import QtWidgets
from qtpy.QtCore import Qt, QUrl, Signal
from qtpy.QtGui import QIcon
from ratarmountcore.formats import FILE_FORMATS, FileFormatID, detect_formats


def get_file_info(path: str):
    if not path:
        return "Empty"
    path2 = Path(path)
    if path2.is_dir():
        return "Directory"
    if path2.exists() and not os.access(path, os.R_OK):
        return "No read permissions"
    if path2.is_file():
        with open(path, 'rb') as fileobj:
            formats = detect_formats(fileobj)
        if formats:
            format_strings: list[str] = []
            for format_id in formats:
                if format_id == FileFormatID.SQLAR and len(formats) > 1:
                    continue
                if format_id in FILE_FORMATS and FILE_FORMATS[format_id].extensions:
                    label = FILE_FORMATS[format_id].extensions[0]
                    if len(label) <= 5:
                        label = label.upper()
                    format_strings.append(label)
                else:
                    format_strings.append(str(format_id))
            return (
                ", ".join(format_strings[:-1])
                + ("," if len(format_strings) > 2 else "")
                + (" or " if len(format_strings) > 1 else "")
                + format_strings[-1]
            )
        return "File"
    if path2.exists():
        return "Unknown"
    return "Non-existent"


class PathInputWidget(QtWidgets.QWidget):
    """
    A widget representing a single path.
    +-------------------------------------------------------------+
    | [ Path Input ] [ File Type / Invalid ] [Browse...]  [+] [-] |
    +-------------------------------------------------------------+
    """

    opened_additional_paths = Signal(list)

    # TODO create list of extensions from available compression and archive backends in ratarmountcore
    def __init__(self, check_path: Callable[Optional[str], [str]], extensions_selection: str) -> None:
        """
        extensions: A string representing a ;;-separated list of labels followed by space-separated glob expressions
                    in parentheses for showing only matching files and folders, e.g.,
                    "Images (*.png *.xpm *.jpg);;Text files (*.txt);;XML files (*.xml);;All Files (*)".
        """
        super().__init__()

        self.check_path = check_path
        self.extensions_selection = extensions_selection

        self.setAcceptDrops(True)

        layout = QtWidgets.QHBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)

        self.input = QtWidgets.QLineEdit()
        self.input.setPlaceholderText("Enter path or drag-and-drop a file or folder...")
        self.input.setAcceptDrops(False)
        self.input.textChanged.connect(self.check)
        layout.addWidget(self.input)

        self.file_info_label = QtWidgets.QLabel()
        self.file_info_label.setTextInteractionFlags(Qt.TextBrowserInteraction)
        self.file_info_label.setProperty("note", "true")
        layout.addWidget(self.file_info_label)

        self.browse_button = QtWidgets.QPushButton("Browse...")
        self.browse_button.clicked.connect(self.browse_file)
        layout.addWidget(self.browse_button)

        self.browse_button.setContextMenuPolicy(Qt.CustomContextMenu)
        self.browse_button.customContextMenuRequested.connect(self.on_browse_context_menu)

        # Add button
        self.add_button = QtWidgets.QToolButton()
        self.add_button.setIcon(QIcon(":/icons/plus.svg"))
        self.add_button.setToolTip("Add another path")
        self.add_button.setVisible(False)
        self.add_button.setEnabled(False)
        layout.addWidget(self.add_button)

        # Remove button
        self.remove_button = QtWidgets.QToolButton()
        self.remove_button.setIcon(QIcon(":/icons/minus.svg"))
        self.remove_button.setToolTip("Remove path")
        self.remove_button.setVisible(False)
        self.remove_button.setEnabled(False)
        layout.addWidget(self.remove_button)

        self.error_label = QtWidgets.QLabel()
        self.error_label.setWordWrap(True)
        self.error_label.setTextInteractionFlags(Qt.TextBrowserInteraction)
        self.error_label.setProperty("error", "true")
        vertical_layout = QtWidgets.QVBoxLayout()
        vertical_layout.setContentsMargins(0, 0, 0, 0)
        vertical_layout.addLayout(layout)
        vertical_layout.addWidget(self.error_label)

        self.setLayout(vertical_layout)

        self.check()

    def browse_file(self):
        if self.add_button.isEnabled():
            paths, _ = QtWidgets.QFileDialog.getOpenFileNames(
                self, "Select Inputs", self.input.text(), self.extensions_selection
            )
            if paths:
                self.input.setText(paths.pop(0))
                if paths:
                    self.opened_additional_paths.emit(paths)
        else:
            path, _ = QtWidgets.QFileDialog.getOpenFileName(
                self, "Select Input", self.input.text(), self.extensions_selection
            )
            if path:
                self.input.setText(path)

    def browse_folder(self):
        path = QtWidgets.QFileDialog.getExistingDirectory(self, "Select Input", self.input.text())
        if path:
            self.input.setText(path)

    def on_browse_context_menu(self, point):
        menu = QtWidgets.QMenu(self)
        menu.addAction("Open Folder", self.browse_folder)
        menu.exec(self.browse_button.mapToGlobal(point))

    def update_file_info(self) -> None:
        # The checks are partially overlapping with check_input_file_type in CLIHelpers.py
        path = self.input.text()
        if not path:
            return "Empty"

        if '://' in path:
            url = QUrl(path)
            if not url.isLocalFile():
                return "URL"
            path = url.toLocalFile().removeprefix("file://")

        if not os.path.exists(path):
            return "Not found"
        return None

    def check(self) -> None:
        self.update_file_info()

        result = self.check_path(self.input.text())
        self.error_label.setText(result or "")
        self.error_label.setVisible(bool(self.error_label.text()))

        self.file_info_label.setText(get_file_info(self.input.text()))

    def set_path(self, path: str) -> str:
        self.input.setText(path)
        self.check()

    def path(self) -> str:
        return self.input.text()

    def dragEnterEvent(self, event) -> None:
        if event.mimeData().hasUrls():
            event.acceptProposedAction()

    def dropEvent(self, event) -> None:
        urls = [url.toLocalFile() if url.isLocalFile() else url.strip() for url in event.mimeData().urls()]
        if urls:
            self.input.setText(urls[0])
            self.check()
            if len(urls) > 1:
                self.opened_additional_paths.emit(urls[1:])


class PathsInputWidget(QtWidgets.QWidget):
    """
    Widget for one or more input paths to be configured.
    +--------------------------------------------------------------+
    |  [ Path Input ] [ File Type / Invalid ] [Browse...]  [+] [-] |
    |  [ Path Input ] [ File Type / Invalid ] [Browse...]  [+] [-] |
    |  [ Path Input ] [ File Type / Invalid ] [Browse...]  [+] [-] |
    +--------------------------------------------------------------+
    """

    changed = Signal()

    def __init__(
        self,
        min_count: int = 0,
        max_count: Optional[int] = None,
        check_path: Optional[Callable[Optional[str], [str]]] = None,
        extensions_selection: str = "All Files (*)",
    ):
        super().__init__()

        if min_count < 0:
            raise ValueError("The minimum number of paths must be 0 or larger!")
        if max_count is not None:
            if max_count < 1:
                raise ValueError("The maximum number of paths must be 1 or larger!")
            if max_count < min_count:
                raise ValueError("The maximum number of paths must be larger or equal to the minimum!")

        self._min_count = min_count
        self._max_count = max_count
        self._check_path = check_path or (lambda x: None)
        self.extensions_selection = extensions_selection

        layout = QtWidgets.QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        for _i in range(max(1, self._min_count)):
            layout.addWidget(self._create_path_input())

        self.error_label = QtWidgets.QLabel()
        self.error_label.setWordWrap(True)
        self.error_label.setTextInteractionFlags(Qt.TextBrowserInteraction)
        self.error_label.setProperty("error", "true")
        layout.addWidget(self.error_label)

        self.setLayout(layout)

        self.check_inputs()

    def set_range(self, min_count: int = 0, max_count: Optional[int] = None):
        self._min_count = min_count
        self._max_count = max_count
        self.check_inputs()

    def _create_path_input(self) -> PathInputWidget:
        widget = PathInputWidget(self.check_path, self.extensions_selection)
        if self._min_count != self._max_count and (self._max_count is None or self._max_count > 1):
            # Make buttons visible. They are enabled in check_inputs if allowed.
            widget.add_button.setVisible(True)
            widget.remove_button.setVisible(True)
        widget.add_button.clicked.connect(self.add_path_input)
        widget.remove_button.clicked.connect(lambda checked, widget=widget: self.remove_path_input(widget))
        widget.opened_additional_paths.connect(self.add_paths)
        widget.input.textChanged.connect(self.check_inputs)
        widget.input.textChanged.connect(self.changed)
        return widget

    def add_path_input(self) -> Optional[PathInputWidget]:
        if self._max_count is not None and len(self.path_inputs()) >= self._max_count:
            return None

        widget = self._create_path_input()
        index = self.layout().indexOf(self.sender().parent())
        self.layout().insertWidget(index + 1 if index >= 0 else index, widget)
        self.check_inputs()
        return widget

    def remove_path_input(self, widget):
        if len(self.path_inputs()) <= self._min_count:
            return

        self.layout().removeWidget(widget)

        # Because of Python lifetimes it is hard to ensure that the widget is actually deleted. Therefore, hide it.
        widget.disconnect(self)
        widget.setVisible(False)
        del widget

        self.check_inputs()

    def add_paths(self, paths: list[str]) -> None:
        # Set existing inputs to dropped paths.
        for path_input in self.path_inputs():
            if not path_input.path() and paths:
                path_input.set_path(paths.pop(0))

        # Add new inputs as required for the dropped file objects.
        for path in paths:
            if self._max_count is None or len(self.path_inputs()) < self._max_count:
                path_input = self._create_path_input()
                path_input.set_path(path)
                self.layout().insertWidget(self.layout().count() - 1, path_input)

        self.check_inputs()

    def path_inputs(self) -> list[PathInputWidget]:
        if not self.layout():
            return []
        return [
            widget
            for i in range(self.layout().count())
            if (widget := self.layout().itemAt(i).widget()) and isinstance(widget, PathInputWidget)
        ]

    def check_path(self, path):
        # If we have sufficient valid paths, then do not show errors for empty paths.
        valid_path_count = sum(1 for path_input in self.path_inputs() if self._check_path(path_input.path()) is None)
        return None if valid_path_count >= self._min_count and not path else self._check_path(path)

    def check_inputs(self):
        path_inputs = self.path_inputs()
        for path_input in path_inputs:
            path_input.check()
            path_input.add_button.setEnabled(self._max_count is None or len(path_inputs) < self._max_count)
            path_input.remove_button.setEnabled(len(path_inputs) > max(1, self._min_count))

        valid_path_count = sum(1 for path_input in self.path_inputs() if self._check_path(path_input.path()) is None)
        # Exceeding the max count should only be possible when the maximum count has been changed from outside.
        message = ""
        if self._min_count is not None and valid_path_count < self._min_count:
            message = (
                f"At least {self._min_count} valid path{'s are' if valid_path_count > 1 else ' is'} "
                f"required but only got {valid_path_count}."
            )
        elif self._max_count is not None and valid_path_count > self._max_count:
            message = f"At most {self._max_count} valid paths may be specified but got {valid_path_count}."

        self.error_label.setText(message)
        self.error_label.setVisible(bool(self.error_label.text()))

    def paths(self) -> list[str]:
        result = [path_input.path() for path_input in self.path_inputs() if self._check_path(path_input.path()) is None]
        return result[: self._max_count]

#!/usr/bin/env python3

import os
import sys
import threading
from typing import Dict, List, Optional, Tuple

from PyQt6.QtCore import QModelIndex, QSortFilterProxyModel, Qt, QThread, pyqtSignal
from PyQt6.QtGui import QAction, QFileSystemModel, QIcon, QKeySequence
from PyQt6.QtWidgets import (
    QApplication,
    QFileDialog,
    QInputDialog,
    QLineEdit,
    QMainWindow,
    QMenu,
    QMessageBox,
    QProgressBar,
    QStatusBar,
    QToolBar,
    QTreeView,
    QVBoxLayout,
    QWidget,
)

from .actions import processParsedArguments
from .cli import _parseArgs
from .fuse import fuse


class MountWorker(QThread):
    """Worker thread for mounting archives to avoid freezing the GUI."""
    finished = pyqtSignal(bool, str)  # success, message

    def __init__(self, args):
        super().__init__()
        self.args = args

    def run(self):
        try:
            # Create mount point if it doesn't exist
            if not os.path.exists(self.args.mount_point):
                os.makedirs(self.args.mount_point)

            # Mount the archive
            processParsedArguments(self.args)
            self.finished.emit(True, f"Successfully mounted {self.args.mount_source[0]} to {self.args.mount_point}")
        except Exception as e:
            self.finished.emit(False, f"Failed to mount archive: {e!s}")


class UnmountWorker(QThread):
    """Worker thread for unmounting archives."""
    finished = pyqtSignal(bool, str)  # success, message

    def __init__(self, mount_point: str):
        super().__init__()
        self.mount_point = mount_point

    def run(self):
        try:
            # Create args object with unmount flag
            args = _parseArgs(['--unmount', self.mount_point])
            processParsedArguments(args)
            self.finished.emit(True, f"Successfully unmounted {self.mount_point}")
        except Exception as e:
            self.finished.emit(False, f"Failed to unmount: {e!s}")


class RatarmountGUI(QMainWindow):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("Ratarmount GUI")
        self.setGeometry(100, 100, 1200, 800)

        # Store active mount points
        self.active_mounts: Dict[str, str] = {}  # mount_point -> archive_path

        # Create central widget and layout
        central_widget = QWidget()
        self.setCentralWidget(central_widget)
        layout = QVBoxLayout(central_widget)

        # Create file system model and view
        self.model = QFileSystemModel()
        self.model.setRootPath("")

        # Create proxy model for sorting/filtering
        self.proxy_model = QSortFilterProxyModel()
        self.proxy_model.setSourceModel(self.model)

        # Create tree view
        self.tree_view = QTreeView()
        self.tree_view.setModel(self.proxy_model)
        self.tree_view.setSortingEnabled(True)
        self.tree_view.setContextMenuPolicy(Qt.ContextMenuPolicy.CustomContextMenu)
        self.tree_view.customContextMenuRequested.connect(self.show_context_menu)
        layout.addWidget(self.tree_view)

        # Create toolbar
        self.create_toolbar()

        # Create status bar
        self.status_bar = QStatusBar()
        self.setStatusBar(self.status_bar)
        self.progress_bar = QProgressBar()
        self.progress_bar.setMaximumWidth(150)
        self.progress_bar.hide()
        self.status_bar.addPermanentWidget(self.progress_bar)

        # Set initial directory to home
        self.tree_view.setRootIndex(self.proxy_model.mapFromSource(self.model.index(os.path.expanduser("~"))))

        # Connect signals
        self.tree_view.doubleClicked.connect(self.handle_double_click)

    def create_toolbar(self):
        toolbar = QToolBar()
        self.addToolBar(toolbar)

        # Mount action
        mount_action = QAction("Mount Archive", self)
        mount_action.setShortcut(QKeySequence("Ctrl+O"))
        mount_action.triggered.connect(self.mount_archive)
        toolbar.addAction(mount_action)

        # Unmount action
        unmount_action = QAction("Unmount", self)
        unmount_action.setShortcut(QKeySequence("Ctrl+U"))
        unmount_action.triggered.connect(self.unmount_current)
        toolbar.addAction(unmount_action)

        toolbar.addSeparator()

        # Refresh action
        refresh_action = QAction("Refresh", self)
        refresh_action.setShortcut(QKeySequence("F5"))
        refresh_action.triggered.connect(self.refresh_view)
        toolbar.addAction(refresh_action)

    def mount_archive(self):
        """Open file dialog to select and mount an archive."""
        archive_path, _ = QFileDialog.getOpenFileName(
            self,
            "Select Archive to Mount",
            "",
            "Archives (*.tar *.tar.gz *.tgz *.tar.bz2 *.tbz2 *.tar.xz *.txz *.zip *.rar *.7z);;All Files (*)"
        )

        if not archive_path:
            return

        # Get mount point
        mount_point, ok = QInputDialog.getText(
            self,
            "Mount Point",
            "Enter mount point path:",
            QLineEdit.EchoMode.Normal,
            os.path.splitext(os.path.basename(archive_path))[0]
        )

        if not ok or not mount_point:
            return

        mount_point = os.path.abspath(os.path.expanduser(mount_point))

        # Create args for mounting
        args = _parseArgs([archive_path, mount_point])

        # Show progress
        self.progress_bar.setRange(0, 0)  # Indeterminate progress
        self.progress_bar.show()
        self.status_bar.showMessage(f"Mounting {archive_path}...")

        # Start mount worker
        self.mount_worker = MountWorker(args)
        self.mount_worker.finished.connect(self.handle_mount_finished)
        self.mount_worker.start()

        # Store mount info
        self.active_mounts[mount_point] = archive_path

    def handle_mount_finished(self, success: bool, message: str):
        """Handle completion of mount operation."""
        self.progress_bar.hide()
        self.status_bar.showMessage(message)

        if success:
            # Refresh view to show mounted archive
            self.refresh_view()
            # Switch to mount point
            self.tree_view.setRootIndex(
                self.proxy_model.mapFromSource(self.model.index(self.mount_worker.args.mount_point))
            )
        else:
            QMessageBox.critical(self, "Mount Error", message)
            # Remove failed mount from active mounts
            if self.mount_worker.args.mount_point in self.active_mounts:
                del self.active_mounts[self.mount_worker.args.mount_point]

    def unmount_current(self):
        """Unmount the currently selected mount point."""
        current_index = self.tree_view.currentIndex()
        if not current_index.isValid():
            return

        current_path = self.model.filePath(self.proxy_model.mapToSource(current_index))

        # Check if path is a mount point
        if not os.path.ismount(current_path):
            QMessageBox.warning(self, "Unmount Error", "Selected path is not a mount point")
            return

        # Confirm unmount
        reply = QMessageBox.question(
            self,
            "Confirm Unmount",
            f"Are you sure you want to unmount {current_path}?",
            QMessageBox.StandardButton.Yes | QMessageBox.StandardButton.No
        )

        if reply != QMessageBox.StandardButton.Yes:
            return

        # Show progress
        self.progress_bar.setRange(0, 0)
        self.progress_bar.show()
        self.status_bar.showMessage(f"Unmounting {current_path}...")

        # Start unmount worker
        self.unmount_worker = UnmountWorker(current_path)
        self.unmount_worker.finished.connect(self.handle_unmount_finished)
        self.unmount_worker.start()

    def handle_unmount_finished(self, success: bool, message: str):
        """Handle completion of unmount operation."""
        self.progress_bar.hide()
        self.status_bar.showMessage(message)

        if success:
            # Remove from active mounts
            mount_point = self.unmount_worker.mount_point
            if mount_point in self.active_mounts:
                del self.active_mounts[mount_point]
            # Refresh view
            self.refresh_view()
        else:
            QMessageBox.critical(self, "Unmount Error", message)

    def refresh_view(self):
        """Refresh the current view."""
        current_index = self.tree_view.currentIndex()
        if current_index.isValid():
            self.model.refresh(self.proxy_model.mapToSource(current_index))

    def show_context_menu(self, position):
        """Show context menu for the current selection."""
        index = self.tree_view.indexAt(position)
        if not index.isValid():
            return

        path = self.model.filePath(self.proxy_model.mapToSource(index))

        menu = QMenu()

        # Add mount/unmount actions if applicable
        if os.path.ismount(path):
            unmount_action = menu.addAction("Unmount")
            unmount_action.triggered.connect(lambda: self.unmount_current())
        else:
            mount_action = menu.addAction("Mount Archive Here")
            mount_action.triggered.connect(lambda: self.mount_archive())

        menu.exec(self.tree_view.viewport().mapToGlobal(position))

    def handle_double_click(self, index: QModelIndex):
        """Handle double click on an item."""
        path = self.model.filePath(self.proxy_model.mapToSource(index))

        if os.path.ismount(path):
            # If it's a mount point, expand it
            self.tree_view.expand(index)
        elif os.path.isdir(path):
            # If it's a directory, expand it
            self.tree_view.expand(index)
        else:
            # If it's a file, try to open it with the system's default application
            try:
                os.startfile(path) if os.name == 'nt' else os.system(f'xdg-open "{path}"')
            except Exception as e:
                QMessageBox.warning(self, "Open Error", f"Could not open file: {e!s}")


def main():
    """Main entry point for the GUI application."""
    app = QApplication(sys.argv)
    window = RatarmountGUI()
    window.show()
    sys.exit(app.exec())


if __name__ == "__main__":
    main()

import logging
import os
import re
import shlex
import subprocess

from qtpy import QtWidgets
from qtpy.QtCore import Qt, Signal
from qtpy.QtGui import QIcon, QShortcut

from ratarmount.widgets.PathsInputWidget import PathsInputWidget

logger = logging.getLogger(__name__)


class MountPointsWidget(QtWidgets.QWidget):
    command_changed = Signal()

    def __init__(self, parent=None):
        super().__init__(parent)

        self.mount_point_tree = QtWidgets.QTreeWidget()
        self.mount_point_tree.setHeaderLabels(["Unmount", "Mount Point", "Type"])
        self.mount_point_tree.setRootIsDecorated(False)
        self.mount_point_tree.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.ExtendedSelection)
        self.mount_point_tree.setContextMenuPolicy(Qt.CustomContextMenu)
        self.mount_point_tree.customContextMenuRequested.connect(self.on_mountpoint_context_menu)
        self.mount_point_tree.header().resizeSection(1, 400)
        self.mount_point_tree.itemSelectionChanged.connect(self._apply_selection)

        # Avoid ismount: https://github.com/python/cpython/issues/96328#issuecomment-2027458283
        # isdir should work, even on crashed FUSE-providing processes.
        self.mount_point_input = PathsInputWidget()
        self.mount_point_input.changed.connect(self._apply_selection)

        self.run_button = QtWidgets.QPushButton("&Unmount")
        self.run_button.setObjectName("primary-button")
        self.run_button.clicked.connect(self.unmount_selected)

        layout = QtWidgets.QVBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self.mount_point_tree)
        layout.addWidget(self.mount_point_input)
        layout.addWidget(self.run_button)
        self.setLayout(layout)

        self._apply_selection()

        QShortcut("F5", self, activated=self.repopulate)

    def _apply_selection(self):
        self.run_button.setEnabled(bool(self.mount_point_tree.selectedItems()))
        self.command_changed.emit()

    def get_command(self):
        # TODO check that PathsInputWidget.paths only returns valid paths! And make it accept an isValid functor
        #      checking for isdir here.
        mount_points = [path for path in self.mount_point_input.paths() if os.path.isdir(path)]
        if not mount_points:
            mount_points.extend(item.data(0, Qt.UserRole) for item in self.mount_point_tree.selectedItems())
        return "ratarmount -u " + (
            ' '.join(shlex.quote(path) for path in mount_points) if mount_points else "<mount point>"
        )

    def populate(self):
        if self.mount_point_tree.topLevelItemCount() == 0:
            self.repopulate()

    def repopulate(self):
        self.mount_point_tree.clear()

        items = []
        for path in sorted(self.detect_mountpoints()):
            item = QtWidgets.QTreeWidgetItem(["", path, "ratarmount"])
            item.setData(0, Qt.UserRole, path)
            items.append(item)
        self.mount_point_tree.addTopLevelItems(items)

        for item in items:
            path = item.data(0, Qt.UserRole)

            eject_button = QtWidgets.QToolButton()
            eject_button.setIcon(QIcon(":/icons/eject.svg"))
            eject_button.setToolTip("Unmount")
            eject_button.clicked.connect(lambda path=path: self.unmount_by_path(path))

            self.mount_point_tree.setItemWidget(item, 0, eject_button)

    def detect_mountpoints(self) -> set[str]:
        # Check /etc/mtab and /proc/mounts for mountpoints
        #
        # https://linux.die.net/man/5/fstab
        # > If the name of the mount point contains spaces these can be escaped as '\040'.
        #
        # > The proper way to read records from fstab is to use the routines getmntent(3).
        #
        # -> getmntent is not available via Python and I don't know how other special characters aside
        #    from space are handled. I was able to produce \012 by adding a newline into the mount point.
        #    A mountpoint called '\\040' will show up as '\134\134040', i.e., the backslash gets escaped
        #    with an octal code. This makes parsing much easier than escaping it with double-backslash.
        #
        # https://linux.die.net/man/8/mount
        # > When the proc filesystem is mounted (say at /proc), the files /etc/mtab and /proc/mounts
        # > have very similar contents. The former has somewhat more information, such as the mount options used,
        # > but is not necessarily up-to-date (cf. the -n option below).
        #
        # > -n, --no-mtab
        # > Mount without writing in /etc/mtab. This is necessary for example when /etc is on a read-only filesystem.
        #
        # Windows could maybe use QDir.drives, but I would not be able to tell whether they are ratarmount mounts.
        mount_points: set[str] = set()
        for path in ["/etc/mtab", "/proc/mounts"]:
            try:
                with open(path, encoding="utf-8") as file:
                    for line in file:
                        parts = line.split(' ')
                        if len(parts) >= 3 and parts[2] == 'fuse':
                            mount_points.add(
                                re.sub(r'\\([0-7]{1,3})', lambda match: chr(int(match.group(1), 8)), parts[1])
                            )
            except Exception as exception:
                logger.warning(
                    "Failed to read mountpoints because of: %s", exception, exc_info=logger.isEnabledFor(logging.DEBUG)
                )
        return mount_points

    def on_mountpoint_context_menu(self, point):
        menu = QtWidgets.QMenu(self)
        unmount_action = menu.addAction("Unmount", self.unmount_selected)
        unmount_action.setEnabled(bool(self.mount_point_tree.selectedItems()))
        menu.addAction("Refresh", self.repopulate)
        menu.exec(self.mount_point_tree.viewport().mapToGlobal(point))

    def unmount_selected(self):
        for item in self.mount_point_tree.selectedItems():
            self.unmount_by_path(item.data(0, Qt.UserRole))

    def unmount_by_path(self, path: str) -> None:
        current_item = self.mount_point_tree.currentItem()
        if not current_item:
            return

        mount_point = current_item.data(0, Qt.UserRole)
        if not mount_point:
            return

        # TODO Map to: unmount_list_checked([mountPoint for mountPoint in args.mount_source or [] if mountPoint])
        #      instead of calling fuermount -u here.
        # TODO Not sure how to handle errors. Need to fix logging first, I guess.
        try:
            # Try to unmount using fusermount
            result = subprocess.run(['fusermount', '-u', mount_point], check=False, capture_output=True, text=True)

            if result.returncode == 0:
                current_item.setText(2, "Unmounted")
            else:
                QtWidgets.QMessageBox.warning(self, "Error", f"Failed to unmount: {result.stderr}")
        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error", f"Failed to unmount: {e!s}")

        self.repopulate()

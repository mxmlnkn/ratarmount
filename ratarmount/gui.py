import sys
import argparse
import signal
import os
import pathlib
import time
from typing import Any, Optional

from PySide6.QtWidgets import (
    QApplication,
    QAbstractItemView,
    QHeaderView,
    QMainWindow,
    QWidget,
    QVBoxLayout,
    QTabWidget,
    QLabel,
    QFormLayout,
    QLineEdit,
    QPushButton,
    QCheckBox,
    QGroupBox,
    QScrollArea,
    QTextBrowser,
    QTreeWidget,
    QTreeWidgetItem,
    QMenu,
    QToolButton,
    QHBoxLayout,
    QMessageBox,
    QFileDialog,
    QComboBox,
    QColorDialog,
    QStyle,
)
from PySide6.QtGui import QPixmap, QKeySequence, QShortcut, QClipboard, QFont, QDesktopServices
from PySide6.QtCore import Qt, QMimeData, QSize

try:
    import markdown_it
except ImportError:
    markdown_it = None  # type: ignore

from ratarmount.dependencies import gather_versions, get_readme


class VersionsTree(QTreeWidget):
    def __init__(self):
        super().__init__()

        self.setHeaderLabels(["Name", "Version"])
        self.setSelectionMode(QAbstractItemView.SelectionMode.ExtendedSelection)
        self.header().setSectionResizeMode(0, QHeaderView.ResizeToContents)
        self.setContextMenuPolicy(Qt.CustomContextMenu)
        self.customContextMenuRequested.connect(self.on_context_menu)

        version_root_items = []
        for label, versions in gather_versions().items():
            parent_item = None
            if isinstance(label, int):
                if label > 1:
                    parent_item = QTreeWidgetItem([f"Level {label} Dependencies"])
            else:
                parent_item = QTreeWidgetItem([label])

            version_items = []
            for name, version in versions:
                version_items.append(QTreeWidgetItem([str(name), str(version)]))

            if parent_item is None:
                version_root_items.extend(version_items)
            else:
                parent_item.addChildren(version_items)
                version_root_items.append(parent_item)

        self.addTopLevelItems(version_root_items)

        QShortcut(QKeySequence.Copy, self, self.action_copy)

    def action_copy(widget: QAbstractItemView):
        # By default Ctrl+C only copies the "current item", not all selected. Weird behavior :/
        # Fix selectedItems ordering by using selectedIndexes and sorting them
        indexes = widget.selectedIndexes()
        # Sort indexes by row, then by column to maintain display order
        indexes.sort(key=lambda idx: (idx.row(), idx.column()))

        # Group by row to get all items for each row
        items_by_row = {}
        for index in indexes:
            row = index.row()
            if row not in items_by_row:
                items_by_row[row] = []
            items_by_row[row].append(index)

        # Build the text in row order
        lines = []
        for row in sorted(items_by_row.keys()):
            row_items = items_by_row[row]
            line_parts = []
            for col in sorted([idx.column() for idx in row_items]):
                item = widget.itemFromIndex(row_items[0].sibling(row, col))
                if item:
                    line_parts.append(item.text(col))
            lines.append(' '.join(line_parts))

        QApplication.clipboard().setText('\n'.join(lines))

    def on_context_menu(self, point):
        menu = QMenu()

        copy_action = menu.addAction("&Copy")
        select_all_action = menu.addAction("Select &All")
        expand_all_action = menu.addAction("Expand All")
        collapse_all_action = menu.addAction("Collapse All")

        action = menu.exec(self.viewport().mapToGlobal(point))

        if action == copy_action:
            self.action_copy()
        elif action == select_all_action:
            self.selectAll()
        elif action == expand_all_action:
            self.expandAll()
        elif action == collapse_all_action:
            self.collapseAll()


class MountSourceWidget(QWidget):
    """Widget for managing mount sources with extensible list and file type detection."""

    def __init__(self, parent=None):
        super().__init__(parent)
        self.mount_sources = []
        self.setup_ui()

    def setup_ui(self):
        layout = QHBoxLayout()

        # Mount source input
        self.input_combo = QComboBox()
        self.input_combo.setEditable(True)
        self.input_combo.setPlaceholderText("Enter mount source path...")
        self.input_combo.setMinimumWidth(300)

        # Browse button
        self.browse_button = QPushButton("Browse...")
        self.browse_button.setMaximumWidth(80)

        # Add button
        self.add_button = QToolButton()
        self.add_button.setText("+")
        self.add_button.setToolTip("Add mount source")
        self.add_button.setMaximumWidth(30)

        # Remove button
        self.remove_button = QToolButton()
        self.remove_button.setText("-")
        self.remove_button.setToolTip("Remove mount source")
        self.remove_button.setMaximumWidth(30)

        layout.addWidget(self.input_combo)
        layout.addWidget(self.browse_button)
        layout.addWidget(self.add_button)
        layout.addWidget(self.remove_button)

        self.setLayout(layout)

        # Connect signals
        self.browse_button.clicked.connect(self.browse_file)
        self.add_button.clicked.connect(self.add_mount_source)
        self.remove_button.clicked.connect(self.remove_mount_source)
        self.input_combo.lineEdit().textChanged.connect(self.update_file_type)

        # Enable drag and drop
        self.setAcceptDrops(True)

    def browse_file(self):
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Select Mount Source", "",
            "All Files (*);;Archive Files (*.tar *.tar.gz *.tar.bz2 *.tar.xz *.zip *.rar *.7z);;Folders (*)"
        )
        if file_path:
            self.input_combo.setCurrentText(file_path)
            self.update_file_type()

    def add_mount_source(self):
        text = self.input_combo.currentText().strip()
        if text and text not in self.mount_sources:
            self.mount_sources.append(text)
            self.input_combo.clear()
            self.input_combo.setPlaceholderText("Enter mount source path...")

    def remove_mount_source(self):
        current_text = self.input_combo.currentText().strip()
        if current_text in self.mount_sources:
            self.mount_sources.remove(current_text)

    def update_file_type(self):
        text = self.input_combo.currentText().strip()
        if not text:
            self.input_combo.setStyleSheet("")
            return

        # Check if file exists
        if os.path.exists(text):
            if os.path.isdir(text):
                self.input_combo.setStyleSheet("background-color: #90EE90;")  # Light green for folders
            else:
                # Check file type
                if text.endswith(('.tar', '.tar.gz', '.tar.bz2', '.tar.xz', '.zip', '.rar', '.7z')):
                    self.input_combo.setStyleSheet("background-color: #87CEEB;")  # Sky blue for archives
                else:
                    self.input_combo.setStyleSheet("background-color: #FFB6C1;")  # Light pink for other files
        else:
            # Non-existent path - could be created as folder
            if text.endswith('/') or '.' not in text:
                self.input_combo.setStyleSheet("background-color: #FFE4B5;")  # Moccasin for potential folders
            else:
                self.input_combo.setStyleSheet("background-color: #FFA07A;")  # Light salmon for non-existent files

    def get_mount_sources(self):
        return self.mount_sources.copy()

    def dragEnterEvent(self, event):
        if event.mimeData().hasUrls():
            event.acceptProposedAction()

    def dropEvent(self, event):
        urls = event.mimeData().urls()
        for url in urls:
            path = url.toLocalFile()
            if path:
                # Remove file:// prefix if present
                if path.startswith('file://'):
                    path = path[7:]
                self.input_combo.setCurrentText(path)
                self.update_file_type()
                break


class MountWidget(QWidget):
    def __init__(self, parser):
        super().__init__()
        self.parser = parser
        self.input_widgets = {}
        self.setup_ui()

    def setup_ui(self):
        main_layout = QVBoxLayout()

        scroll_area = QScrollArea()
        scroll_area.setWidgetResizable(True)

        options_widget = QWidget()
        options_layout = QVBoxLayout()

        # Mount sources section
        mount_group = QGroupBox("Mount Sources")
        mount_layout = QVBoxLayout()

        self.mount_source_widget = MountSourceWidget()
        mount_layout.addWidget(self.mount_source_widget)

        # Mount point field
        mount_point_layout = QHBoxLayout()
        self.mount_point_input = QLineEdit()
        self.mount_point_input.setPlaceholderText("Auto-inferred mount point...")
        self.browse_mount_point = QPushButton("Browse...")
        self.browse_mount_point.setMaximumWidth(80)

        mount_point_layout.addWidget(QLabel("Mount Point:"))
        mount_point_layout.addWidget(self.mount_point_input)
        mount_point_layout.addWidget(self.browse_mount_point)
        mount_layout.addLayout(mount_point_layout)

        mount_group.setLayout(mount_layout)
        options_layout.addWidget(mount_group)

        # Generically add actions from parsed argparse object to avoid duplication.
        for group in self.parser._action_groups:
            if not group._group_actions or 'commands' in group.title.lower():
                continue

            group_box = QGroupBox(group.title)
            form_layout = QFormLayout()

            for action in group._group_actions:
                name = action.option_strings[-1] if action.option_strings else action.dest
                if action.type == bool or isinstance(action.default, bool):
                    check_box = QCheckBox()
                    check_box.setCheckState(Qt.Checked if action.default else Qt.Unchecked)
                    # Mark required fields
                    if hasattr(action, 'required') and action.required:
                        check_box.setStyleSheet("font-weight: bold; color: red;")
                    form_layout.addRow(f"*{name}" if hasattr(action, 'required') and action.required else name, check_box)
                    self.input_widgets[name] = check_box
                else:
                    line_edit = QLineEdit()
                    if action.default not in (None, argparse.SUPPRESS):
                        line_edit.setPlaceholderText(str(action.default))

                    # Add browse button for pathlib.Path types
                    if hasattr(action, 'type') and action.type == pathlib.Path:
                        browse_layout = QHBoxLayout()
                        browse_layout.addWidget(line_edit)
                        browse_button = QPushButton("Browse...")
                        browse_button.setMaximumWidth(60)
                        browse_layout.addWidget(browse_button)

                        widget = QWidget()
                        widget.setLayout(browse_layout)
                        form_layout.addRow(name, widget)

                        browse_button.clicked.connect(lambda checked, le=line_edit: self.browse_path(le))
                        self.input_widgets[name] = line_edit
                    else:
                        form_layout.addRow(name, line_edit)
                        self.input_widgets[name] = line_edit

                    # Enable drag and drop for path inputs
                    line_edit.setAcceptDrops(True)
                    line_edit.installEventFilter(self)

            group_box.setLayout(form_layout)
            options_layout.addWidget(group_box)

        options_widget.setLayout(options_layout)
        scroll_area.setWidget(options_widget)

        main_layout.addWidget(scroll_area)

        # Command preview
        self.command_preview = QTextBrowser()
        self.command_preview.setMaximumHeight(100)
        self.command_preview.setPlaceholderText("Command preview will appear here...")
        main_layout.addWidget(QLabel("Command Preview:"))
        main_layout.addWidget(self.command_preview)

        # Apply button
        self.apply_button = QPushButton("Apply")
        self.apply_button.clicked.connect(self.update_command_preview)
        main_layout.addWidget(self.apply_button)

        # Run button
        self.run_button = QPushButton("Mount")
        self.run_button.clicked.connect(self.mount)
        main_layout.addWidget(self.run_button)

        self.setLayout(main_layout)

        # Connect signals
        self.browse_mount_point.clicked.connect(self.browse_mount_point_path)
        for widget in self.input_widgets.values():
            if isinstance(widget, QLineEdit):
                widget.textChanged.connect(self.update_command_preview)
            elif isinstance(widget, QCheckBox):
                widget.stateChanged.connect(self.update_command_preview)

    def browse_path(self, line_edit):
        file_path, _ = QFileDialog.getExistingDirectory(
            self, "Select Path", ""
        )
        if file_path:
            line_edit.setText(file_path)

    def browse_mount_point_path(self):
        dir_path = QFileDialog.getExistingDirectory(
            self, "Select Mount Point", ""
        )
        if dir_path:
            self.mount_point_input.setText(dir_path)

    def eventFilter(self, obj, event):
        if isinstance(obj, QLineEdit) and event.type() == event.Type.DragEnter:
            if event.mimeData().hasUrls():
                event.accept()
                return True
        elif isinstance(obj, QLineEdit) and event.type() == event.Type.Drop:
            urls = event.mimeData().urls()
            for url in urls:
                path = url.toLocalFile()
                if path:
                    if path.startswith('file://'):
                        path = path[7:]
                    obj.setText(path)
                    break
            return True
        return super().eventFilter(obj, event)

    def update_command_preview(self):
        """Update the command preview based on current input values."""
        args = []

        # Add mount sources
        mount_sources = self.mount_source_widget.get_mount_sources()
        args.extend(mount_sources)

        # Add mount point if specified
        mount_point = self.mount_point_input.text().strip()
        if mount_point:
            args.append(mount_point)

        # Add other arguments
        for name, widget in self.input_widgets.items():
            if isinstance(widget, QCheckBox):
                if widget.isChecked():
                    # Use the first option string if available
                    action = self._get_action_by_name(name)
                    if action and action.option_strings:
                        args.append(action.option_strings[0])
            elif isinstance(widget, QLineEdit):
                value = widget.text().strip()
                if value:
                    action = self._get_action_by_name(name)
                    if action and action.option_strings:
                        args.append(action.option_strings[0])
                        if value != str(action.default):
                            args.append(value)

        command = "ratarmount " + " ".join(args)
        self.command_preview.setText(command)

    def _get_action_by_name(self, name):
        """Get argparse action by name/destination."""
        for group in self.parser._action_groups:
            for action in group._group_actions:
                if action.dest == name or (action.option_strings and name in action.option_strings):
                    return action
        return None

    def mount(self):
        """Execute the mount command."""
        command = self.command_preview.toPlainText()
        if not command:
            QMessageBox.warning(self, "Warning", "Please configure mount options first.")
            return

        # Here you would normally execute the command
        # For now, just show a message
        QMessageBox.information(self, "Mount", f"Would execute: {command}")


class RatarmountGUI(QMainWindow):
    def __init__(self, parser):
        super().__init__()

        self.setWindowTitle("Ratarmount")

        # Shortcuts for closing
        QShortcut(QKeySequence("Ctrl+W"), self, activated=self.close)
        QShortcut(QKeySequence(Qt.Key_Escape), self, activated=self.close)

        central_widget = QWidget()
        central_layout = QVBoxLayout()

        logo_label = QLabel()
        # Fix logo loading to work outside of module root folder
        import pkg_resources
        try:
            # Try to load from installed package
            logo_path = pkg_resources.resource_filename('ratarmount', 'ratarmount.svg')
            if os.path.exists(logo_path):
                pixmap = QPixmap(logo_path)
            else:
                # Fallback to current directory
                pixmap = QPixmap("ratarmount.svg")
        except Exception:
            # Fallback to current directory
            pixmap = QPixmap("ratarmount.svg")

        if not pixmap.isNull():
            # Scale pixmap to fit while maintaining aspect ratio
            scaled_pixmap = pixmap.scaled(200, 200, Qt.KeepAspectRatio, Qt.SmoothTransformation)
            logo_label.setPixmap(scaled_pixmap)
        logo_label.setAlignment(Qt.AlignCenter)
        central_layout.addWidget(logo_label)

        tabs = QTabWidget()
        tabs.addTab(MountWidget(parser), "Mount")

        # Unmount tab
        unmount_tab = QWidget()
        unmount_layout = QVBoxLayout()

        # FUSE mountpoints detection
        detection_group = QGroupBox("Detected FUSE Mountpoints")
        detection_layout = QVBoxLayout()

        self.mountpoint_tree = QTreeWidget()
        self.mountpoint_tree.setHeaderLabels(["Mount Point", "Type", "Status"])
        self.mountpoint_tree.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.mountpoint_tree.setContextMenuPolicy(Qt.CustomContextMenu)
        self.mountpoint_tree.customContextMenuRequested.connect(self.on_mountpoint_context_menu)

        refresh_button = QPushButton("Refresh Mountpoints")
        refresh_button.clicked.connect(self.refresh_mountpoints)

        detection_layout.addWidget(self.mountpoint_tree)
        detection_layout.addWidget(refresh_button)
        detection_group.setLayout(detection_layout)
        unmount_layout.addWidget(detection_group)

        # Unmount controls
        unmount_controls = QGroupBox("Unmount Controls")
        controls_layout = QVBoxLayout()

        self.unmount_button = QPushButton("Unmount Selected")
        self.unmount_button.clicked.connect(self.unmount_selected)
        self.unmount_button.setEnabled(False)

        self.force_unmount_button = QPushButton("Force Unmount")
        self.force_unmount_button.clicked.connect(self.force_unmount)

        controls_layout.addWidget(self.unmount_button)
        controls_layout.addWidget(self.force_unmount_button)
        unmount_controls.setLayout(controls_layout)
        unmount_layout.addWidget(unmount_controls)

        unmount_tab.setLayout(unmount_layout)
        tabs.addTab(unmount_tab, "Unmount")

        # Commit tab
        commit_tab = QWidget()
        commit_layout = QVBoxLayout()

        commit_group = QGroupBox("Commit Overlay")
        commit_form = QFormLayout()

        self.commit_archive_input = QLineEdit()
        self.commit_archive_input.setPlaceholderText("Path to archive...")
        self.commit_archive_browse = QPushButton("Browse...")
        self.commit_archive_browse.setMaximumWidth(80)

        commit_archive_layout = QHBoxLayout()
        commit_archive_layout.addWidget(self.commit_archive_input)
        commit_archive_layout.addWidget(self.commit_archive_browse)
        commit_form.addRow("Archive:", commit_archive_layout)

        self.commit_overlay_input = QLineEdit()
        self.commit_overlay_browse = QPushButton("Browse...")
        self.commit_overlay_browse.setMaximumWidth(80)

        commit_overlay_layout = QHBoxLayout()
        commit_overlay_layout.addWidget(self.commit_overlay_input)
        commit_overlay_layout.addWidget(self.commit_overlay_browse)
        commit_form.addRow("Overlay:", commit_overlay_layout)

        self.commit_encoding_input = QLineEdit()
        self.commit_encoding_input.setPlaceholderText("tarfile.ENCODING")
        commit_form.addRow("Encoding:", self.commit_encoding_input)

        self.commit_debug_checkbox = QCheckBox()
        self.commit_debug_checkbox.setChecked(False)
        commit_form.addRow("Debug:", self.commit_debug_checkbox)

        commit_button = QPushButton("Commit Overlay")
        commit_button.clicked.connect(self.commit_overlay)

        commit_group.setLayout(commit_form)
        commit_layout.addWidget(commit_group)
        commit_layout.addWidget(commit_button)

        commit_tab.setLayout(commit_layout)
        tabs.addTab(commit_tab, "Commit")
        tabs.addTab(VersionsTree(), "Versions")

        # ReadMe tab
        readme_widget = QWidget()
        readme_layout = QHBoxLayout()

        # Section tree view for navigation
        self.section_tree = QTreeWidget()
        self.section_tree.setHeaderHidden(True)
        self.section_tree.setMaximumWidth(200)
        self.section_tree.itemClicked.connect(self.navigate_to_section)

        # Main content area
        readme_content = QTextBrowser()
        readme_content.setOpenExternalLinks(True)

        # Improve image handling and display
        if markdown_it is None:
            readme_content.setPlainText(get_readme("ratarmount"))
        else:
            readme_content.setHtml(
                markdown_it.MarkdownIt('commonmark', {'breaks': False, 'html': True})
                .enable('table')
                .render(get_readme("ratarmount"))
            )

        # Enable external image link support
        readme_content.setOpenLinks(False)
        readme_content.sourceChanged.connect(self.handle_source_changed)

        # Layout
        readme_layout.addWidget(self.section_tree)
        readme_layout.addWidget(readme_content)
        readme_widget.setLayout(readme_layout)

        # Build section tree from content
        self.build_section_tree(readme_content.toHtml())

        tabs.addTab(readme_widget, "ReadMe")

        # Open-Source Software tab
        oss_widget = QWidget()
        oss_layout = QHBoxLayout()

        # OSS tree view
        self.oss_tree = QTreeWidget()
        self.oss_tree.setHeaderLabels(["Package", "Version", "License"])
        self.oss_tree.setSelectionMode(QAbstractItemView.SelectionMode.SingleSelection)
        self.oss_tree.itemClicked.connect(self.show_oss_details)
        self.oss_tree.setMaximumWidth(300)

        # OSS details panel
        self.oss_details = QTextBrowser()
        self.oss_details.setOpenExternalLinks(True)
        self.oss_details.setPlaceholderText("Select a package to view details...")

        # Layout
        oss_layout.addWidget(self.oss_tree)
        oss_layout.addWidget(self.oss_details)
        oss_widget.setLayout(oss_layout)

        # Load OSS attributions
        self.load_oss_attributions()

        tabs.addTab(oss_widget, "OSS")

        # Connect tab switching to update command previews
        tabs.currentChanged.connect(self.on_tab_changed)

        central_layout.addWidget(tabs)

        # Command preview widget for all tabs
        self.command_preview = QTextBrowser()
        self.command_preview.setMaximumHeight(80)
        self.command_preview.setStyleSheet("background-color: #2b2b2b; color: #ffffff; font-family: monospace;")
        self.command_preview.setPlaceholderText("Command preview will appear here...")

        command_group = QGroupBox("Command Preview")
        command_layout = QVBoxLayout()
        command_layout.addWidget(self.command_preview)
        command_group.setLayout(command_layout)
        central_layout.addWidget(command_group)

        # Terminal-like output widget
        self.terminal_output = QTextBrowser()
        self.terminal_output.setMaximumHeight(150)
        self.terminal_output.setStyleSheet("background-color: #1e1e1e; color: #00ff00; font-family: monospace; font-size: 10px;")
        self.terminal_output.setPlaceholderText("Terminal output will appear here...")
        self.terminal_output.setOpenLinks(False)

        terminal_group = QGroupBox("Terminal Output")
        terminal_layout = QVBoxLayout()
        terminal_layout.addWidget(self.terminal_output)
        terminal_group.setLayout(terminal_layout)
        central_layout.addWidget(terminal_group)

        # Update command preview for all tabs
        self.update_all_command_previews()

        central_widget.setLayout(central_layout)
        self.setCentralWidget(central_widget)

        # Connect signals for commit tab
        self.commit_archive_browse.clicked.connect(lambda: self.browse_file(self.commit_archive_input))
        self.commit_overlay_browse.clicked.connect(lambda: self.browse_directory(self.commit_overlay_input))

        # Initialize mountpoints
        self.refresh_mountpoints()

    def browse_file(self, line_edit):
        file_path, _ = QFileDialog.getOpenFileName(
            self, "Select File", "",
            "All Files (*);;Archive Files (*.tar *.tar.gz *.tar.bz2 *.tar.xz *.zip *.rar *.7z)"
        )
        if file_path:
            line_edit.setText(file_path)

    def browse_directory(self, line_edit):
        dir_path = QFileDialog.getExistingDirectory(
            self, "Select Directory", ""
        )
        if dir_path:
            line_edit.setText(dir_path)

    def refresh_mountpoints(self):
        """Refresh the list of FUSE mountpoints."""
        self.mountpoint_tree.clear()

        try:
            # Check /etc/mtab for mountpoints
            with open('/etc/mtab', 'r') as f:
                for line in f:
                    parts = line.strip().split()
                    if len(parts) >= 2:
                        mount_point = parts[1]
                        fs_type = parts[2]

                        # Check if it's a FUSE filesystem
                        if 'fuse' in fs_type.lower() or 'ratarmount' in fs_type.lower():
                            item = QTreeWidgetItem([mount_point, fs_type, "Mounted"])
                            item.setData(0, Qt.UserRole, mount_point)
                            self.mountpoint_tree.addTopLevelItem(item)

            # Also check /proc/mounts as fallback
            if not self.mountpoint_tree.topLevelItemCount():
                with open('/proc/mounts', 'r') as f:
                    for line in f:
                        parts = line.strip().split()
                        if len(parts) >= 3:
                            mount_point = parts[1]
                            fs_type = parts[2]

                            if 'fuse' in fs_type.lower() or 'ratarmount' in fs_type.lower():
                                item = QTreeWidgetItem([mount_point, fs_type, "Mounted"])
                                item.setData(0, Qt.UserRole, mount_point)
                                self.mountpoint_tree.addTopLevelItem(item)

        except Exception as e:
            item = QTreeWidgetItem(["Error reading mountpoints", "", str(e)])
            self.mountpoint_tree.addTopLevelItem(item)

        # Check for ratarmount-specific indicators
        self.detect_ratarmount_mountpoints()

    def detect_ratarmount_mountpoints(self):
        """Detect ratarmount-specific mountpoints."""
        try:
            # Check common mount points for ratarmount indicators
            common_mounts = ['/mnt', '/media', '/home', '/tmp']

            for mount_point in common_mounts:
                if os.path.exists(mount_point):
                    for root, dirs, files in os.walk(mount_point, topdown=True):
                        # Stop searching too deep
                        if root.count(os.sep) - mount_point.count(os.sep) > 3:
                            break

                        for dir_name in dirs:
                            full_path = os.path.join(root, dir_name)
                            # Check for ratarmount indicators
                            indicators = ['.ratarmount-cli', '.versions', '.ratarmount-data']
                            if any(os.path.exists(os.path.join(full_path, indicator)) for indicator in indicators):
                                item = QTreeWidgetItem([full_path, "ratarmount", "Mounted"])
                                item.setData(0, Qt.UserRole, full_path)
                                self.mountpoint_tree.addTopLevelItem(item)

        except Exception as e:
            item = QTreeWidgetItem(["Error detecting ratarmount mounts", "", str(e)])
            self.mountpoint_tree.addTopLevelItem(item)

    def on_mountpoint_context_menu(self, point):
        """Handle context menu for mountpoints."""
        item = self.mountpoint_tree.itemAt(point)
        if not item:
            return

        menu = QMenu()

        unmount_action = menu.addAction("Unmount")
        refresh_action = menu.addAction("Refresh")
        properties_action = menu.addAction("Properties")

        action = menu.exec(self.mountpoint_tree.viewport().mapToGlobal(point))

        if action == unmount_action:
            self.unmount_selected()
        elif action == refresh_action:
            self.refresh_mountpoints()
        elif action == properties_action:
            self.show_mountpoint_properties(item)

    def show_mountpoint_properties(self, item):
        """Show properties of a mountpoint."""
        mount_point = item.data(0, Qt.UserRole)
        QMessageBox.information(self, "Mountpoint Properties",
                              f"Mount Point: {mount_point}\nType: {item.text(1)}\nStatus: {item.text(2)}")

    def unmount_selected(self):
        """Unmount the selected mountpoint."""
        current_item = self.mountpoint_tree.currentItem()
        if not current_item:
            return

        mount_point = current_item.data(0, Qt.UserRole)
        if not mount_point:
            return

        try:
            # Try to unmount using fusermount
            import subprocess
            result = subprocess.run(['fusermount', '-u', mount_point],
                                  capture_output=True, text=True)

            if result.returncode == 0:
                current_item.setText(2, "Unmounted")
                QMessageBox.information(self, "Success", f"Successfully unmounted {mount_point}")
            else:
                QMessageBox.warning(self, "Error", f"Failed to unmount: {result.stderr}")

        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to unmount: {str(e)}")

    def force_unmount(self):
        """Force unmount selected mountpoint."""
        current_item = self.mountpoint_tree.currentItem()
        if not current_item:
            return

        mount_point = current_item.data(0, Qt.UserRole)
        if not mount_point:
            return

        reply = QMessageBox.question(self, "Force Unmount",
                                   f"Are you sure you want to force unmount {mount_point}?",
                                   QMessageBox.Yes | QMessageBox.No)

        if reply == QMessageBox.Yes:
            try:
                import subprocess
                result = subprocess.run(['fusermount', '-z', mount_point],
                                      capture_output=True, text=True)

                if result.returncode == 0:
                    current_item.setText(2, "Force Unmounted")
                    QMessageBox.information(self, "Success", f"Force unmounted {mount_point}")
                else:
                    QMessageBox.warning(self, "Error", f"Failed to force unmount: {result.stderr}")

            except Exception as e:
                QMessageBox.critical(self, "Error", f"Failed to force unmount: {str(e)}")

    def commit_overlay(self):
        """Commit overlay to archive."""
        archive_path = self.commit_archive_input.text().strip()
        overlay_path = self.commit_overlay_input.text().strip()
        encoding = self.commit_encoding_input.text().strip() or 'tarfile.ENCODING'
        debug = self.commit_debug_checkbox.isChecked()

        if not archive_path or not overlay_path:
            QMessageBox.warning(self, "Warning", "Please specify both archive and overlay paths.")
            return

        try:
            # Import the commit_overlay function
            from ratarmount.actions import commit_overlay

            # This would normally call the actual function
            # commit_overlay(overlay_path, archive_path, encoding=encoding, printDebug=debug)

            QMessageBox.information(self, "Commit",
                                  f"Would commit overlay from {overlay_path} to {archive_path}\n"
                                  f"Encoding: {encoding}\nDebug: {debug}")

        except Exception as e:
            QMessageBox.critical(self, "Error", f"Failed to commit overlay: {str(e)}")

    def load_oss_attributions(self):
        """Load and display OSS attributions."""
        self.oss_tree.clear()

        try:
            # Import the print_oss_attributions function to extract data
            from ratarmount.actions import print_oss_attributions

            # Create a temporary buffer to capture the output
            import io
            from contextlib import redirect_stdout

            output = io.StringIO()
            with redirect_stdout(output):
                print_oss_attributions()

            content = output.getvalue()

            # Parse the content to build the tree
            self.parse_oss_content(content)

        except Exception as e:
            item = QTreeWidgetItem(["Error loading OSS attributions", "", str(e)])
            self.oss_tree.addTopLevelItem(item)

    def parse_oss_content(self, content):
        """Parse OSS attributions content and build tree."""
        lines = content.strip().split('\n')

        current_package = None
        current_item = None

        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Package header (starts with #)
            if line.startswith('# '):
                package_name = line[2:].strip()
                current_item = QTreeWidgetItem([package_name, "", ""])
                current_item.setData(0, Qt.UserRole, package_name)
                self.oss_tree.addTopLevelItem(current_item)
                current_package = package_name

            # License information
            elif line.startswith('License:'):
                if current_item:
                    license_info = line[8:].strip()
                    current_item.setText(2, license_info)

            # Project URLs
            elif line.startswith('https://'):
                if current_item:
                    # Add as child item
                    url_item = QTreeWidgetItem(["Project URL", "", ""])
                    url_item.setData(0, Qt.UserRole, line.strip())
                    current_item.addChild(url_item)

            # Authors
            elif line.startswith('Authors:'):
                if current_item:
                    authors_info = line[8:].strip()
                    # Add as child item
                    author_item = QTreeWidgetItem(["Authors", "", authors_info])
                    current_item.addChild(author_item)

            # License content
            elif line.startswith('```'):
                if current_item:
                    # License content block
                    license_content = []
                    continue

        # Expand all top-level items
        for i in range(self.oss_tree.topLevelItemCount()):
            self.oss_tree.topLevelItem(i).setExpanded(True)

    def show_oss_details(self, item, column):
        """Show detailed information for selected OSS package."""
        package_name = item.data(0, Qt.UserRole)
        if not package_name:
            return

        # Build detailed information
        details = f"<h3>{package_name}</h3>"

        # Add version information if available
        if item.text(1):
            details += f"<p><strong>Version:</strong> {item.text(1)}</p>"

        # Add license information
        if item.text(2):
            details += f"<p><strong>License:</strong> {item.text(2)}</p>"

        # Add child information (URLs, Authors, etc.)
        for i in range(item.childCount()):
            child = item.child(i)
            child_type = child.text(0)

            if child_type == "Project URL":
                url = child.data(0, Qt.UserRole)
                if url:
                    details += f"<p><strong>{child_type}:</strong> <a href='{url}'>{url}</a></p>"
            elif child_type == "Authors":
                authors = child.text(2)
                if authors:
                    details += f"<p><strong>{child_type}:</strong> {authors}</p>"
            else:
                details += f"<p><strong>{child_type}:</strong> {child.text(2)}</p>"

        self.oss_details.setHtml(details)

    def build_section_tree(self, html_content):
        """Build a tree of sections from HTML content for navigation."""
        self.section_tree.clear()

        import re
        from bs4 import BeautifulSoup

        try:
            soup = BeautifulSoup(html_content, 'html.parser')

            # Find all headers (h1, h2, h3, etc.)
            headers = soup.find_all(['h1', 'h2', 'h3', 'h4', 'h5', 'h6'])

            root_item = QTreeWidgetItem(["Sections"])
            self.section_tree.addTopLevelItem(root_item)

            for header in headers:
                level = int(header.name[1])  # Get number from h1, h2, etc.
                text = header.get_text().strip()

                # Create item with proper indentation
                item = QTreeWidgetItem(["  " * (level - 1) + text])
                item.setData(0, Qt.UserRole, header.get('id', ''))

                # Add to appropriate parent
                if level == 1:
                    root_item.addChild(item)
                else:
                    # Find the parent at level-1
                    parent = self.find_section_parent(root_item, level - 1)
                    if parent:
                        parent.addChild(item)

            root_item.setExpanded(True)

        except Exception as e:
            item = QTreeWidgetItem(["Error building section tree"])
            self.section_tree.addTopLevelItem(item)

    def find_section_parent(self, root_item, target_level):
        """Find the parent item for a section at the given level."""
        def find_in_item(item, current_level):
            if current_level == target_level:
                return item

            for i in range(item.childCount()):
                child = item.child(i)
                result = find_in_item(child, current_level + 1)
                if result:
                    return result
            return None

        return find_in_item(root_item, 0)

    def navigate_to_section(self, item, column):
        """Navigate to a section when clicked."""
        section_id = item.data(0, Qt.UserRole)
        if section_id:
            # Find anchor in content and scroll to it
            # We need to find the QTextBrowser widget in the layout
            for i in range(self.section_tree.parent().layout().count()):
                widget = self.section_tree.parent().layout().itemAt(i).widget()
                if isinstance(widget, QTextBrowser):
                    widget.scrollToAnchor(section_id)
                    break

    def handle_source_changed(self, url):
        """Handle external link clicks."""
        if url and not url.isEmpty():
            # Open external links in browser
            if url.scheme() == "http" or url.scheme() == "https":
                QDesktopServices.openUrl(url)
                return True
        return False

    def update_all_command_previews(self):
        """Update command preview for all tabs."""
        # Mount tab command is handled by MountWidget
        # Unmount tab command
        self.update_unmount_command_preview()

        # Commit tab command
        self.update_commit_command_preview()

        # Versions tab command
        self.update_versions_command_preview()

        # OSS tab command
        self.update_oss_command_preview()

    def update_unmount_command_preview(self):
        """Update command preview for unmount tab."""
        command = "# fusermount -u <mountpoint>"
        self.command_preview.setText(command)

    def update_commit_command_preview(self):
        """Update command preview for commit tab."""
        archive_path = self.commit_archive_input.text().strip()
        overlay_path = self.commit_overlay_input.text().strip()
        encoding = self.commit_encoding_input.text().strip() or 'tarfile.ENCODING'
        debug = "--debug" if self.commit_debug_checkbox.isChecked() else ""

        if archive_path and overlay_path:
            command = f"ratarmount --commit-overlay --write-overlay {overlay_path} {archive_path}"
            if encoding != 'tarfile.ENCODING':
                command += f" --encoding {encoding}"
            if debug:
                command += f" {debug}"
            self.command_preview.setText(command)
        else:
            self.command_preview.setText("# ratarmount --commit-overlay --write-overlay <overlay> <archive>")

    def update_versions_command_preview(self):
        """Update command preview for versions tab."""
        command = "ratarmount --version"
        self.command_preview.setText(command)

    def update_oss_command_preview(self):
        """Update command preview for OSS tab."""
        command = "ratarmount --oss-attributions"
        self.command_preview.setText(command)

    def log_to_terminal(self, message, level="INFO"):
        """Log a message to the terminal output widget."""
        import time
        timestamp = time.strftime("%H:%M:%S")

        # Color coding based on log level
        if level == "ERROR":
            color = "#ff4444"
        elif level == "WARNING":
            color = "#ffaa44"
        elif level == "DEBUG":
            color = "#4444ff"
        else:
            color = "#00ff00"

        html_message = f'<span style="color: {color}">[{timestamp}] {level}: {message}</span><br>'
        current_content = self.terminal_output.toHtml()
        self.terminal_output.setHtml(current_content + html_message)

        # Auto-scroll to bottom
        scrollbar = self.terminal_output.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def on_tab_changed(self, index):
        """Handle tab switching to update command previews."""
        self.update_all_command_previews()


def handle_sigint(signum, frame):
    QApplication.quit()


if __name__ == "__main__":
    # Else, PySide6 ignored SIGINT! https://stackoverflow.com/a/4939113/2191065
    signal.signal(signal.SIGINT, signal.SIG_DFL)

    app = QApplication(sys.argv)
    from ratarmount.cli import create_parser

    parser = create_parser(useColor=False)
    gui = RatarmountGUI(parser)
    gui.show()
    sys.exit(app.exec())

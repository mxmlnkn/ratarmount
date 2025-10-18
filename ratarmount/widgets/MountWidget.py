import argparse
import logging
import os
import shlex
from pathlib import Path
from typing import Any, Callable, Optional

from qtpy import QtWidgets
from qtpy.QtCore import Qt, Signal

from ratarmount.actions import determine_mount_point, process_parsed_arguments
from ratarmount.widgets.PathsInputWidget import PathsInputWidget

logger = logging.getLogger(__name__)


def check_input_folder(path: str) -> Optional[str]:
    return "The path must point to an existing directory." if not os.path.isdir(path) else None


def check_output_folder(path: str) -> Optional[str]:
    if not path:
        return "Path must not be empty."
    for parent in [Path(path), *Path(path).parents]:
        if parent.exists() and not parent.is_dir():
            return (
                "The path must point to an existing directory or to some non-existent path, "
                f"in which case a directory will be created, but {shlex.quote(str(parent))} is not a directory."
            )
    return None


def check_input_file(path: str) -> Optional[str]:
    return "The path must point to an existing file." if not os.path.isfile(path) else None


def check_output_file(path: str) -> Optional[str]:
    if not path:
        return "Path must not be empty."
    path2 = Path(path)
    if path2.exists() and path2.is_dir():
        return (
            "The path must point to an existing file or to some non-existent path, "
            "in which case a file will be created."
        )
    for parent in path2.parents:
        if parent.exists() and not parent.is_dir():
            return f"All parents must be directories, but {shlex.quote(str(parent))} is not a directory."
    return None


class NamespaceObject(dict):
    def __getattr__(self, key, default=None):
        return self[key] if key in self else default

    def __setattr__(self, key, value):
        self[key] = value


class MountWidget(QtWidgets.QWidget):
    command_changed = Signal()

    # TODO Apply all input widget values to a copy of the parser object similarly to parser.parse.
    #      Use that to build the original command line and display it. See idea in RatarmountGUI.__init__
    #      Then, call actions.process_parsed_arguments with it.
    # TODO Some kind of search/filtering, e.g., by label, or placeholder, or value, would be epic!
    def __init__(self, parser, parsed=None):
        super().__init__()
        self._parser = parser
        self._parsed = parsed if parsed else NamespaceObject()
        # Stores getters, normally for each input widget, that return list of command line arguments, e.g.,
        # ["--index-folder", "~/.cache/ratarmount-indexes"].
        self._get_option_arguments: list[Callable[[], list[str]]] = []
        # Stores getters for values keyed by the action target destination to build some argparse parsed
        # object to be used with process_parsed_arguments.
        self._get_option_values: dict[str, Callable[[], Any]] = {}
        self.setup_ui()

    def setup_ui(self):
        mount_source_group = QtWidgets.QGroupBox("Mount Sources")
        mount_source_group.setFlat(True)
        mount_source_layout = QtWidgets.QVBoxLayout()
        self.mount_source_widget = PathsInputWidget(
            1, check_path=lambda path: ("Input path must exist." if not os.path.exists(path) else None)
        )
        self.mount_source_widget.changed.connect(self.command_changed)
        mount_source_layout.addWidget(self.mount_source_widget)
        mount_source_group.setLayout(mount_source_layout)
        self._get_option_values['mount_source'] = self.mount_source_widget.paths
        value = getattr(self._parsed, 'mount_source', None)
        if isinstance(value, list):
            self.mount_source_widget.add_paths(value)

        mount_point_group = QtWidgets.QGroupBox("Mount Point")
        mount_point_group.setFlat(True)
        mount_point_layout = QtWidgets.QVBoxLayout()
        # Empty mount point results in automatically chosen one.
        self.mount_point_widget = PathsInputWidget(
            1, 1, check_path=lambda path: (check_output_folder(path) if path else None)
        )
        self.mount_point_widget.changed.connect(self.command_changed)
        mount_point_layout.addWidget(self.mount_point_widget)
        mount_point_group.setLayout(mount_point_layout)
        self._get_option_values['mount_point'] = lambda: (
            self.mount_point_widget.paths()[0] if self.mount_point_widget.paths() else None
        )
        value = getattr(self._parsed, 'mount_point', None)
        if isinstance(value, str):
            self.mount_point_widget.add_paths([value])

        def update_default_mount_point():
            path_inputs = self.mount_point_widget.path_inputs()
            if not path_inputs:
                return
            path_input = path_inputs[0]
            mount_sources = self.mount_source_widget.paths()
            mount_point = determine_mount_point(mount_sources[0]) if mount_sources else None
            path_input.input.setPlaceholderText(mount_point or "Enter path or drag-and-drop a file or folder...")

        self.mount_source_widget.changed.connect(update_default_mount_point)

        options_layout = QtWidgets.QVBoxLayout()
        options_layout.addWidget(mount_source_group)
        options_layout.addWidget(mount_point_group)

        path_option_checks = {
            '--index-file': check_output_file,
            '--password-file': check_input_file,
            '--log-file': check_output_file,
            '--write-overlay': check_output_folder,
        }

        # Generically add actions from parsed argparse object to avoid duplication.
        # Unfortunately, there is no public API to get the actions back, but argparse-tui also uses
        # parser._action and parser._subparsers._actions.
        for group in self._parser._action_groups:
            filtered_actions = [
                action
                for action in group._group_actions
                if action.dest not in {'mount_source', 'mount_point', 'foreground'}
            ]
            if not filtered_actions or 'commands' in group.title.lower():
                continue

            # Necessary because QLayout has no setVisible to hide everything when the group box is collapsed.
            group_widget = QtWidgets.QWidget()

            group_box = QtWidgets.QGroupBox(group.title)
            group_box.toggled.connect(lambda checked, g=group_widget, b=group_box: g.setVisible(checked))
            group_box.setCheckable(True)
            group_box.setChecked(False)
            form_layout = QtWidgets.QFormLayout()

            # Note: Should I use better labels? Currently, it looks like a command line arguments compositor,
            #       which is also kind of fine, but might deter GUI users...
            for action in filtered_actions:
                name = action.dest
                # Use the first option. Important for the --no- variants!
                if action.option_strings:
                    name = action.option_strings[0]
                # Prefer the long-form options.
                if len(action.option_strings) >= 2 and len(action.option_strings[0]) == 2:
                    name = action.option_strings[1]

                value = getattr(self._parsed, action.dest) if hasattr(self._parsed, action.dest) else action.default

                if action.type is bool or isinstance(action.default, bool):
                    input_widget = QtWidgets.QCheckBox()
                    input_widget.setCheckState(Qt.Checked if value else Qt.Unchecked)
                    QtWidgets.QLabel(f"*{name}" if hasattr(action, 'required') and action.required else name)
                    input_widget.stateChanged.connect(self.command_changed)

                    def get_argument(w=input_widget, a=action):
                        if w.isChecked() == a.default:
                            return []

                        if w.isChecked():
                            for option in a.option_strings[::-1]:
                                if not option.startswith('--no-'):
                                    return option

                        for option in a.option_strings[::-1]:
                            if option.startswith('--no-'):
                                return option

                        if a.option_strings:
                            return a.option_strings[0]

                        logger.warning("No fitting command line option for checked box found!")
                        return []

                    self._get_option_arguments.append(
                        lambda w=input_widget, n=name, a=action: (
                            [n, w.isChecked()] if w.isChecked() != a.default else []
                        )
                    )
                    self._get_option_values[action.dest] = input_widget.isChecked

                    if action.dest == 'control_interface':
                        input_widget.stateChanged.connect(
                            lambda checked: self.mount_source_widget.set_range(0 if checked else 1, None)
                        )

                elif action.type is str and name in path_option_checks:
                    input_widget = PathsInputWidget(1 if action.required else 0, 1, check_path=path_option_checks[name])
                    if isinstance(value, str):
                        input_widget.add_paths([value])
                    input_widget.changed.connect(self.command_changed)

                    self._get_option_arguments.append(
                        lambda w=input_widget, n=name: ([n, *w.paths()] if w.paths() else [])
                    )
                    self._get_option_values[action.dest] = lambda w=input_widget, n=name: (
                        w.paths()[0] if w.paths() else None
                    )
                elif name == '--index-folders':
                    input_widget = PathsInputWidget(check_path=check_output_folder)
                    input_widget.changed.connect(self.command_changed)

                    # TODO index-folders has some custom comma-separated path string, which we need to map to!
                    self._get_option_arguments.append(
                        lambda w=input_widget, n=name: ([n, *w.paths()] if w.paths() else [])
                    )
                    self._get_option_values[action.dest] = input_widget.paths
                elif action.type is int:
                    # TODO make the 'debug' option a combobox, or remove it completely in favor of manual GUI filtering
                    # TODO How to implement recursion depth default of None? An extra checkbox do enable it?
                    #      Did I not have some idea about a whole pane for recursion options, including extensions?
                    input_widget = QtWidgets.QSpinBox()
                    input_widget.setRange(0, 1_000_000_000)
                    if isinstance(value, int):
                        input_widget.setValue(value)
                    input_widget.valueChanged.connect(self.command_changed)

                    self._get_option_arguments.append(
                        lambda w=input_widget, n=name, a=action: ([n, w.value()] if w.value() != a.default else [])
                    )
                    self._get_option_values[action.dest] = input_widget.value
                elif action.type is str or action.type is float:
                    input_widget = QtWidgets.QLineEdit()
                    if isinstance(action.default, float):
                        input_widget.setPlaceholderText(str(action.default))
                    if isinstance(value, float):
                        input_widget.setText(str(value))
                    input_widget.textChanged.connect(self.command_changed)

                    self._get_option_arguments.append(
                        lambda w=input_widget, n=name: ([n, w.text()] if w.text() else [])
                    )

                    def get_value(w=input_widget, default=action.default):
                        try:
                            return float(w.text())
                        except ValueError:
                            return default
                    self._get_option_values[action.dest] = get_value
                else:
                    logger.warning("Ignoring unknown argparse action '%s' with type: %s", name, action.type)
                    continue

                description = QtWidgets.QLabel(action.help)
                description.setWordWrap(True)
                description.setProperty("note", "true")
                description.setTextInteractionFlags(Qt.TextBrowserInteraction)
                form_layout.addRow(description)

                form_layout.addRow(name, input_widget)
                label = form_layout.itemAt(form_layout.rowCount() - 1, QtWidgets.QFormLayout.LabelRole).widget()
                label.setTextInteractionFlags(Qt.TextBrowserInteraction)

            group_widget.setLayout(form_layout)
            layout = QtWidgets.QVBoxLayout()
            layout.setContentsMargins(0, 0, 0, 0)
            layout.addWidget(group_widget)
            group_box.setLayout(layout)
            options_layout.addWidget(group_box)

        # Add positional arguments to getters in correct order for the command line.
        self._get_option_arguments.append(self.mount_source_widget.paths)
        self._get_option_arguments.append(
            lambda: (
                [self.mount_point_widget.paths()[0]]
                if self.mount_point_widget.paths() and self.mount_point_widget.paths()[0]
                else []
            )
        )

        options_widget = QtWidgets.QWidget()
        options_widget.setLayout(options_layout)

        scroll_area = QtWidgets.QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setWidget(options_widget)

        main_layout = QtWidgets.QVBoxLayout()
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.addWidget(scroll_area)

        # Should have some icon. Check mark, drive icon? Play icon?
        self.run_button = QtWidgets.QPushButton("&Mount")
        self.run_button.setObjectName("primary-button")
        self.run_button.clicked.connect(self.mount)
        main_layout.addWidget(self.run_button)

        self.setLayout(main_layout)

    def get_command(self) -> str:
        return "ratarmount " + " ".join(
            shlex.quote(str(argument)) for getter in self._get_option_arguments for argument in getter()
        )

    def mount(self):
        for name, getter in self._get_option_values.items():
            setattr(self._parsed, name, getter())
        process_parsed_arguments(self._parsed)
        # TODO Disable mount button when there are errors
        # TODO Actually execute.
        # TODO show error outputs, maybe in terminal output and/or a log tab
        # TODO !!! Currently, this hangs the whole GUI here and closes the comamnd line but not the GUI!?
        #QtWidgets.QMessageBox.information(self, "Mount", "Would mount if it could.")

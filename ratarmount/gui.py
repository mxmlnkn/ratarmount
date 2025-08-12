import sys
import argparse
import signal

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
)
from PySide6.QtGui import QPixmap, QKeySequence, QShortcut, QClipboard
from PySide6.QtCore import Qt

try:
    import markdown_it
except ImportError:
    markdown_it = None  # type: ignore

from ratarmount.actions import gather_versions, get_readme


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
        # TODO For some dumb reason seletedItems is not in display order, but in children-first-order Oo
        #      Maybe use selectedIndexes, or try to sort them ourselves somehow.
        QApplication.clipboard().setText(
            '\n'.join(' '.join(item.text(i) for i in range(item.columnCount())) for item in widget.selectedItems())
        )

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


class MountWidget(QWidget):
    def __init__(self, parser):
        super().__init__()

        main_layout = QVBoxLayout()

        scroll_area = QScrollArea()
        # Make the width of the contained groupb boxes and form layout fit the scroll area.
        scroll_area.setWidgetResizable(True)

        options_widget = QWidget()
        options_layout = QVBoxLayout()

        # TODO add extensible (QToolButton with '+' icon) list of mount sources as input.
        # TODO Implement a "Browse ..." button with QFileDialog. And show the detected file type somehow,
        #      e.g., as a label. Color it if it does not exist? Also detect folders. Some options expect
        #      folders, in that case, folder type would be green and existing non-folder type red.
        #      If non-existing, folders can simply be created automatically.
        #      TODO check if all folders, or parent folders to files (log!), are automatically created!
        # TODO Mark required fields.
        # TODO Add mount point field. Set placeholder text to automatically inferred location

        # Generically add actions from parsed argparse object to avoid duplication.
        # Unfortunately, there is no public API to get the actions back, but argparse-tui also uses
        # parser._action and parser._subparsers._actions.
        for group in parser._action_groups:
            if not group._group_actions or 'commands' in group.title.lower():
                continue

            group_box = QGroupBox(group.title)
            form_layout = QFormLayout()

            # TODO Better labels. But I guess at that point, I am manually writing everything anyway ...
            # TODO Drag&drop of file into QLineEdit creates something like:
            #      "file:///media/folder". The prefix file:// should be handled
            #      since adding fsspec, but still could be removed. The suffixed double spaces or newline or
            #      whatever it is, is worse! Might have to implement mimeDropAccept on a custom QLineEdit or
            #      whatever it was called.
            for action in group._group_actions:
                name = action.option_strings[-1] if action.option_strings else action.dest
                if action.type == bool or isinstance(action.default, bool):
                    check_box = QCheckBox()
                    check_box.setCheckState(Qt.Checked if action.default else Qt.Unchecked)
                    form_layout.addRow(name, check_box)
                else:
                    line_edit = QLineEdit()
                    if action.default not in (None, argparse.SUPPRESS):
                        line_edit.setPlaceholderText(str(action.default))
                    # if action.type == pathlib.Path # TODO Add Browse... Button
                    form_layout.addRow(name, line_edit)

            group_box.setLayout(form_layout)
            options_layout.addWidget(group_box)

        options_widget.setLayout(options_layout)
        scroll_area.setWidget(options_widget)

        main_layout.addWidget(scroll_area)
        # TODO Apply all input widget values to a copy of the parser object similarly to parser.parse.
        #      Use that to build the original command line and display it. See idea in RatarmountGUI.__init__
        #      Then, call actions.process_parsed_arguments with it.
        run_button = QPushButton("Mount")
        main_layout.addWidget(run_button)

        self.setLayout(main_layout)


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
        # TODO Will not work outside of module root folder in git checkout!
        #      Need to bundle ratarmount.svg, e.g., in pyproject.toml as data file?
        #      Need to either download it from script-dir/../ or maybe use qrc files:
        #      https://doc.qt.io/qtforpython-6/tutorials/basictutorial/qrcfiles.html
        pixmap = QPixmap("ratarmount.svg")
        if not pixmap.isNull():
            logo_label.setPixmap(pixmap)
        logo_label.setAlignment(Qt.AlignCenter)
        central_layout.addWidget(logo_label)

        tabs = QTabWidget()
        tabs.addTab(MountWidget(parser), "Mount")
        # TODO Show a widget to unmount arbitrary paths and try to find fuse mountpoints programmatically
        #      and show them as list with eject icons to unmount them when clicked.
        #      Ratarmount mounts could be recognized if some of the hidden folders such as .ratarmount-cli
        #      or .versions exists even if not in parent directory listing.
        # TODO Map to: unmount_list_checked([mountPoint for mountPoint in args.mount_source or [] if mountPoint])
        tabs.addTab(QLabel("TODO"), "Unmount")
        # TODO Map to --commit-overlay
        #      commit_overlay(args.write_overlay, args.mount_source[0], encoding=args.encoding, printDebug=args.debug)
        #      Only 4 options supported, but unfortunately partially duplicate with "Mount". Would be nice
        #      to keep all of them in sync with the input fields in "Mount" when either of them are changed.
        #
        tabs.addTab(QLabel("TODO"), "Commit")
        tabs.addTab(VersionsTree(), "Versions")

        # ReadMe tab
        # TODO The image given by relative path will only work when executed in the root of the ratarmount module.
        # TODO The benchmark plot image is shown in original size instead of adjusting to the window width!
        # TODO Anchor links to sections do not work -.- Might be a problem with markdown-it-py
        # TODO A tree view to jump to sections would be nice to have, the manual TOC can be removed then.
        # TODO External image links are broken -> Need to implement parents QTextEdit.loadResource interface.
        readme_tab = QTextBrowser()
        # Without this, it tries to open external links in QTextBrowser and fails with "QTextBrowser: No document for"
        # and shows a gray background that cannot be returned to the ReadMe without restart.
        readme_tab.setOpenExternalLinks(True)
        # Qt does not work with Github's "markdown" dialect with HTML mixed in. Neither does Python-markdown work.
        if markdown_it is None:
            readme_tab.setPlainText(get_readme("ratarmount"))
        else:
            readme_tab.setHtml(
                markdown_it.MarkdownIt('commonmark', {'breaks': False, 'html': True})
                .enable('table')
                .render(get_readme("ratarmount"))
            )
        tabs.addTab(readme_tab, "ReadMe")

        # Open-Source Software tab
        # TODO Just as in print_oss_attributions query the metadata with importlib and show it as a QTeeWidget with
        #      Authors, project URL, and when clicking on an entry show the ReadMe in a widget on the right side.
        # TODO Similarly to gather_versions and print_versions, split print_oss_attributions off into a
        #      gather_oss_attributions
        tabs.addTab(QLabel("TODO"), "OSS")

        central_layout.addWidget(tabs)

        # TODO Add command-line like widget that shows the ratarmount command that the action woudl map to.
        #      This should be possible for all the tabs, even the versions and OSS and unmount tab!

        # TODO Add a terminal-like output widget with black background that dynamically shows all log output
        #      in monospace font. Would be really cool if it could be colorized, but I doubt Qt has some Rich-like
        #      support.

        central_widget.setLayout(central_layout)
        self.setCentralWidget(central_widget)


def handle_sigint(signum, frame):
    QApplication.quit()


if __name__ == "__main__":
    # Else, PySide6 ignored SIGINT! https://stackoverflow.com/a/4939113/2191065
    signal.signal(signal.SIGINT, signal.SIG_DFL)

    app = QApplication(sys.argv)
    from cli import create_parser

    parser = create_parser(useColor=False)
    gui = RatarmountGUI(parser)
    gui.show()
    sys.exit(app.exec())

import importlib.metadata
import logging
import os
import signal
import sys
import tempfile
import time

logger = logging.getLogger(__name__)

# Set QT_API environment variable to pyside6 if not set because the default would be pyqt5.
if not os.environ.get('QT_API'):
    for backend in ['PySide6', 'PyQt6', 'PySide2', 'PyQt5']:
        try:
            importlib.import_module(backend)
            os.environ['QT_API'] = backend
            break
        except Exception as exception:
            logger.info("Skip prioritized Qt backend %s because of: %s", backend, exception)

from qtpy import QtWidgets
from qtpy.QtCore import QResource, Qt
from qtpy.QtGui import QCursor, QFontMetrics, QIcon, QKeySequence, QPixmap, QShortcut

from ratarmount.widgets import styles
from ratarmount.widgets.CommitOverlayWidget import CommitOverlayWidget
from ratarmount.widgets.HoverLabel import HoverLabel
from ratarmount.widgets.MarkdownViewer import MarkdownViewer
from ratarmount.widgets.MountPointsWidget import MountPointsWidget
from ratarmount.widgets.MountWidget import MountWidget
from ratarmount.widgets.resources.resources import load_resource
from ratarmount.widgets.StyleEditorWidget import StyleEditorWidget
from ratarmount.widgets.VersionsTreeWidget import VersionsTreeWidget


# TODO Move logging into extra tab and always record all logs but then filter them in that tab for showing?
#      Except maybe debug logs which are so frequent that they might slow things down? Especially fuse logs :/
# TODO Logs might also have to be per-mount! Maybe enable the control interface by default?
#      And warn against disabling it?
# TODO Does it even make sense to offer the --foreground option? I guess in that case we would use ratarmountcore
#      directly! Might even be a better way to start because we can control it much easier.
# TODO Background mounts without the control interface would be the hardest to manage. There is still the output
#      of the subprocess call though.
# TODO How do I even show a progress bar :/? A subprocess might not work, especially with --foreground, because
#      the resulting MountSource object and SQLite connection might not be serializable!
#      Even without --foreground, I would have to periodically check the subprocess output somehow and parse
#      the ASCII-art progress bar to extract the progress?!
class RatarmountGUI(QtWidgets.QMainWindow):
    def __init__(self, argument_parser, parsed_arguments):
        super().__init__()

        self.setWindowTitle("Ratarmount")

        # Shortcuts for closing
        QShortcut(QKeySequence("Ctrl+W"), self, activated=self.close)
        QShortcut(QKeySequence(Qt.Key_Escape), self, activated=self.close)

        central_widget = QtWidgets.QWidget()
        central_layout = QtWidgets.QVBoxLayout()

        if logo_svg := load_resource("ratarmount-static.svg"):
            logo_pixmap = QPixmap()
            logo_pixmap.loadFromData(logo_svg)
            logo_pixmap = logo_pixmap.scaled(500, 200, Qt.KeepAspectRatio, Qt.SmoothTransformation)

            logo_glow_pixmap = QPixmap()
            logo_glow_pixmap.loadFromData(
                logo_svg.replace(b'class="gloweffect"', b'filter="url(#glow)"')
                .replace(b'#BB00CC', b'#DD00EE')
                .replace(b'#00AA00', b'#00CC00')
            )
            logo_glow_pixmap = logo_glow_pixmap.scaled(500, 200, Qt.KeepAspectRatio, Qt.SmoothTransformation)

            logo_label = HoverLabel(logo_pixmap, logo_glow_pixmap, text="ratarmount")
            logo_label.setAlignment(Qt.AlignCenter)

            central_layout.addWidget(logo_label)

        tabs = QtWidgets.QTabWidget()
        self._tabs_widget = tabs
        self._mount_widget = MountWidget(argument_parser, parsed_arguments)
        tabs.addTab(self._mount_widget, "Mount")

        self._unmount_widget = MountPointsWidget()
        tabs.addTab(self._unmount_widget, "Unmount")

        self._commit_widget = CommitOverlayWidget()
        tabs.addTab(self._commit_widget, "Commit")
        tabs.addTab(VersionsTreeWidget(), "Versions")
        tabs.addTab(MarkdownViewer(), "ReadMe")

        tabs.currentChanged.connect(self.update_all_command_previews)
        QShortcut(
            QKeySequence(Qt.ControlModifier | Qt.Key_PageUp),
            tabs,
            activated=lambda: tabs.setCurrentIndex((tabs.currentIndex() - 1) % tabs.count()),
        ).setContext(Qt.WindowShortcut)
        QShortcut(
            QKeySequence(Qt.ControlModifier | Qt.Key_PageDown),
            tabs,
            activated=lambda: tabs.setCurrentIndex((tabs.currentIndex() + 1) % tabs.count()),
        ).setContext(Qt.WindowShortcut)

        # Command preview widget for all tabs
        self.command_preview = QtWidgets.QTextBrowser()
        self.command_preview.setObjectName("terminal")
        self.command_preview.setPlaceholderText("Command preview will appear here...")

        # Create toggleable Command Preview group
        command_group = QtWidgets.QGroupBox("Command Preview")
        command_group.setCheckable(True)
        command_group.setChecked(True)  # Expanded by default
        command_group.toggled.connect(self.command_preview.setVisible)

        # Create layout with icon label
        command_layout = QtWidgets.QVBoxLayout()
        command_layout.addWidget(self.command_preview)
        command_group.setLayout(command_layout)
        command_group.setMaximumHeight(int(QFontMetrics(self.font()).height() * 7.5))

        # Terminal-like output widget
        self.terminal_output = QtWidgets.QTextBrowser()
        self.terminal_output.setObjectName("terminal")
        self.terminal_output.setPlaceholderText("Terminal output will appear here for supporting tabs...")
        self.terminal_output.setOpenLinks(False)

        # Create toggleable Terminal Output group
        terminal_group = QtWidgets.QGroupBox("Terminal Output")
        terminal_group.setCheckable(True)
        terminal_group.setChecked(False)  # Collapsed by default
        terminal_group.toggled.connect(self.terminal_output.setVisible)
        self.terminal_output.setVisible(terminal_group.isChecked())

        # Create layout with icon label
        terminal_layout = QtWidgets.QVBoxLayout()
        terminal_layout.addWidget(self.terminal_output)
        terminal_group.setLayout(terminal_layout)

        terminals_layout = QtWidgets.QVBoxLayout()
        terminals_layout.addWidget(command_group)
        terminals_layout.addWidget(terminal_group)
        terminals_widget = QtWidgets.QWidget()
        terminals_widget.setLayout(terminals_layout)

        splitter = QtWidgets.QSplitter(Qt.Vertical)
        splitter.addWidget(tabs)
        splitter.addWidget(terminals_widget)
        splitter.setStretchFactor(0, 1)
        central_layout.addWidget(splitter)

        self.theme_button = QtWidgets.QToolButton()
        self.theme_button.setToolTip("Toggle theme")
        self.theme_button.clicked.connect(self.toggle_theme)
        self.update_theme_button()

        self.style_button = QtWidgets.QToolButton()
        self.style_button.setToolTip("Edit style")
        self.style_button.setIcon(QIcon(":/icons/palette.svg"))
        self.style_button.clicked.connect(self.open_style_editor)

        # Credits
        credits_label = QtWidgets.QLabel(
            f"""
            <style>
            a:link {{
                color: {styles.PARAMETERS[styles.MODE]['GREEN_PRIMARY']};
                text-decoration: none;
            }}
            </style>
            by <a href="https://github.com/mxmlnkn">mxmlnkn</a>
            """
        )
        credits_label.setAlignment(Qt.AlignRight)
        credits_label.setOpenExternalLinks(True)

        font = credits_label.font()
        font.setPointSizeF(font.pointSizeF() * 0.7)
        credits_label.setFont(font)

        # Add theme toggle and credits label into the status bar.
        status_bar_widget = QtWidgets.QWidget()
        layout = QtWidgets.QHBoxLayout(status_bar_widget)
        layout.setContentsMargins(0, 0, 0, 0)
        layout.setSpacing(2)
        layout.addWidget(self.theme_button, alignment=Qt.AlignCenter)
        layout.addWidget(self.style_button, alignment=Qt.AlignCenter)
        layout.addWidget(credits_label, alignment=Qt.AlignCenter)
        self.statusBar().addPermanentWidget(status_bar_widget)

        # Shortcuts for tab switching
        for i in range(tabs.count()):
            shortcut = QShortcut(QKeySequence(f"Alt+{i+1}"), tabs, activated=lambda i=i: tabs.setCurrentIndex(i))
            shortcut.setContext(Qt.WindowShortcut)

        for i in range(tabs.count()):
            tab = tabs.widget(i)
            if tab and hasattr(tab, 'command_changed'):
                tab.command_changed.connect(self.update_all_command_previews)
        self.update_all_command_previews()

        central_widget.setLayout(central_layout)
        self.setCentralWidget(central_widget)

    def update_all_command_previews(self) -> None:
        current_tab = self._tabs_widget.currentWidget()
        if hasattr(current_tab, 'populate'):
            current_tab.populate()

        if hasattr(current_tab, 'get_command'):
            self.command_preview.setText(current_tab.get_command())
        else:
            self.command_preview.clear()

    def log_to_terminal(self, message, level="INFO"):
        """Log a message to the terminal output widget."""
        timestamp = time.strftime("%H:%M:%S")

        # Color coding based on log level (using theme colors)
        from ratarmount.widgets.styles import get_color

        if level == "ERROR":
            color = get_color('red_primary')
        elif level == "WARNING":
            color = get_color('orange')
        elif level == "DEBUG":
            color = get_color('purple')
        else:
            color = get_color('green_primary')

        html_message = f'<span style="color: {color}">[{timestamp}] {level}: {message}</span><br>'
        current_content = self.terminal_output.toHtml()
        self.terminal_output.setHtml(current_content + html_message)

        # Auto-scroll to bottom
        scrollbar = self.terminal_output.verticalScrollBar()
        scrollbar.setValue(scrollbar.maximum())

    def open_style_editor(self):
        editor = StyleEditorWidget(self)
        editor.style_changed.connect(self.setStyleSheet)
        editor.show()

    def update_theme_button(self):
        self.theme_button.setIcon(QIcon(f":/icons/{'moon' if styles.MODE == 'dark' else 'sun'}.svg"))

    def toggle_theme(self):
        styles.MODE = 'dark' if styles.MODE == 'light' else 'light'
        self.setStyleSheet(styles.get_stylesheet())
        self.update_theme_button()


class CustomStyle(QtWidgets.QProxyStyle):
    def polish(self, widget):
        # Add translucent background so that we can use rounded corners for context menus.
        # For some dumb reason, this trick does not work with QToolTip -.-.
        if isinstance(widget, (QtWidgets.QMenu, QtWidgets.QToolTip)):
            widget.setAttribute(Qt.WA_TranslucentBackground)
        return super().polish(widget)


def main(parsed_arguments=None):
    # Else, PySide6 ignored SIGINT! https://stackoverflow.com/a/4939113/2191065
    signal.signal(signal.SIGINT, signal.SIG_DFL)

    app = QtWidgets.QApplication(sys.argv)
    app.setStyle(CustomStyle())
    app.setStyleSheet(styles.get_stylesheet())

    # QResource.registerResourceData is something different and does not work and leads to spurious segfaults!
    with tempfile.NamedTemporaryFile(suffix=".rcc", delete=True) as file:
        file.write(load_resource("qresources.rcc"))
        file.flush()
        QResource.registerResource(file.name)

    # from qtpy.QtCore import QDirIterator
    # it = QDirIterator(":/", QDirIterator.Subdirectories)
    # while it.hasNext():
    #     print(it.next())

    from ratarmount.cli import create_parser

    parser = create_parser(useColor=False)
    gui = RatarmountGUI(parser, parsed_arguments)
    gui.show()

    gui.resize(650, 900)
    frame = gui.frameGeometry()
    screen = QtWidgets.QApplication.screenAt(QCursor.pos())
    frame.moveCenter(screen.availableGeometry().center())
    gui.move(frame.topLeft())

    return app.exec()


if __name__ == "__main__":
    sys.exit(main())

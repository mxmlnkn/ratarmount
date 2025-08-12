from qtpy import QtWidgets
from qtpy.QtCore import Qt, Signal
from qtpy.QtGui import QFont, QTextCursor

from ratarmount.widgets import styles
from ratarmount.widgets.resources.resources import load_resource


class StyleEditorWidget(QtWidgets.QDialog):
    """A dialog for editing the application's stylesheet."""

    style_changed = Signal(str)

    def __init__(self, parent=None):
        super().__init__(parent)
        self.setWindowTitle("Style Editor")
        self.resize(800, 1000)

        layout = QtWidgets.QVBoxLayout()

        # Text editor for CSS
        self.editor = QtWidgets.QTextEdit()
        self.editor.setFont(QFont("Consolas", 10))
        self.editor.setPlainText(load_resource("ratarmount.css").decode('utf-8'))
        self.editor.setPlaceholderText("Enter QSS styles here...")

        layout.addWidget(self.editor)

        # Color reference section
        color_group = QtWidgets.QGroupBox("Color Variables")
        color_layout = QtWidgets.QVBoxLayout()

        # Create scrollable area for color reference
        scroll_area = QtWidgets.QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_content = QtWidgets.QWidget()
        color_grid = QtWidgets.QGridLayout(scroll_content)

        # Add color swatches
        self.color_inputs = {}  # Store references to color input fields
        for row, color in enumerate(styles.PARAMETERS[styles.MODE].items()):
            color_name, color_value = color

            # Color swatch
            swatch = QtWidgets.QLabel()
            swatch.setMinimumSize(30, 20)
            swatch.setStyleSheet(f"background-color: {color_value};")
            swatch.setObjectName(f"swatch_{color_name}")
            color_grid.addWidget(swatch, row, 0)

            # Color name
            name_label = QtWidgets.QLabel(f"<<<{color_name}>>>")
            name_label.setStyleSheet("font-family: monospace;")
            color_grid.addWidget(name_label, row, 1)

            # Color value input field
            color_input = QtWidgets.QLineEdit(color_value)
            color_input.setStyleSheet("font-family: monospace;")
            color_input.setPlaceholderText("#RRGGBB")
            color_input.textChanged.connect(
                lambda text, name=color_name, swatch=swatch: self.update_color_value(text, swatch, name)
            )
            self.color_inputs[color_name] = color_input
            color_grid.addWidget(color_input, row, 2)

        scroll_content.setLayout(color_grid)
        scroll_area.setWidget(scroll_content)
        color_layout.addWidget(scroll_area)
        color_group.setLayout(color_layout)
        layout.addWidget(color_group)

        # Buttons
        button_layout = QtWidgets.QHBoxLayout()

        # Reset button
        reset_button = QtWidgets.QPushButton("Reset")
        reset_button.clicked.connect(
            lambda: self.editor.setPlainText(load_resource("ratarmount-dark.css").decode('utf-8'))
        )
        button_layout.addWidget(reset_button)

        # Apply button
        apply_button = QtWidgets.QPushButton("Apply")
        apply_button.clicked.connect(self.apply_stylesheet)
        apply_button.setObjectName("primary-button")
        button_layout.addWidget(apply_button)

        # Cancel button
        cancel_button = QtWidgets.QPushButton("Cancel")
        cancel_button.clicked.connect(self.reject)
        button_layout.addWidget(cancel_button)

        layout.addLayout(button_layout)

        self.setLayout(layout)

    def apply_stylesheet(self):
        data = self.editor.toPlainText()
        for key, value in styles.PARAMETERS[styles.MODE].items():
            data = data.replace(f"<<<{key}>>>", value)

        try:
            self.style_changed.emit(data)
        except Exception as exception:
            QtWidgets.QMessageBox.warning(self, "Error", f"Failed to apply stylesheet:\n{exception}")

    def update_color_value(self, text, swatch, name):
        if not text.startswith('#') or len(text) not in [4, 7]:
            return

        swatch.setStyleSheet(f"background-color: {text};")
        if name in styles.PARAMETERS[styles.MODE]:
            styles.PARAMETERS[styles.MODE][name] = text

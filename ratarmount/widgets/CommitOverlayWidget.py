from qtpy import QtWidgets


class CommitOverlayWidget(QtWidgets.QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)

        commit_group = QtWidgets.QGroupBox("Commit Overlay")
        commit_group.setFlat(True)
        commit_form = QtWidgets.QFormLayout()

        self.commit_archive_input = QtWidgets.QLineEdit()
        self.commit_archive_input.setPlaceholderText("Path to archive...")
        self.commit_archive_browse = QtWidgets.QPushButton("Browse...")
        self.commit_archive_browse.setMaximumWidth(80)

        # TODO Use PathsInput and only allow non-compressed TAR!
        commit_archive_layout = QtWidgets.QHBoxLayout()
        commit_archive_layout.addWidget(self.commit_archive_input)
        commit_archive_layout.addWidget(self.commit_archive_browse)
        commit_form.addRow("Archive", commit_archive_layout)

        self.commit_overlay_input = QtWidgets.QLineEdit()
        self.commit_overlay_browse = QtWidgets.QPushButton("Browse...")
        self.commit_overlay_browse.setMaximumWidth(80)

        # TODO In contrast to "Mount", Overlay must be an existing non-empty folder!
        # TODO abstract not only is_valid check but also add custom tooltip if not valid,
        #      maybe "check_input(path) -> Optional[str]:", which returns an error message if it fails.
        commit_overlay_layout = QtWidgets.QHBoxLayout()
        commit_overlay_layout.addWidget(self.commit_overlay_input)
        commit_overlay_layout.addWidget(self.commit_overlay_browse)
        commit_form.addRow("Overlay", commit_overlay_layout)

        self.commit_encoding_input = QtWidgets.QLineEdit()
        self.commit_encoding_input.setPlaceholderText("tarfile.ENCODING")
        commit_form.addRow("Encoding", self.commit_encoding_input)

        options_layout = QtWidgets.QVBoxLayout()
        # commit_layout.setContentsMargins(0, 0, 0, 0)
        commit_group.setLayout(commit_form)
        options_layout.addWidget(commit_group)

        options_widget = QtWidgets.QWidget()
        options_widget.setLayout(options_layout)

        scroll_area = QtWidgets.QScrollArea()
        scroll_area.setWidgetResizable(True)
        scroll_area.setWidget(options_widget)

        main_layout = QtWidgets.QVBoxLayout()
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.addWidget(scroll_area)

        commit_button = QtWidgets.QPushButton("Commit Overlay")
        commit_button.setObjectName("primary-button")
        commit_button.clicked.connect(self.commit_overlay)
        main_layout.addWidget(commit_button)

        self.setLayout(main_layout)

        # Connect signals for commit tab
        self.commit_archive_browse.clicked.connect(lambda: self.browse_file(self.commit_archive_input))
        self.commit_overlay_browse.clicked.connect(lambda: self.browse_directory(self.commit_overlay_input))

    # TODO Remove in favor of PathsInputWidget
    def browse_file(self, line_edit):
        file_path, _ = QtWidgets.QFileDialog.getOpenFileName(
            self, "Select File", "", "All Files (*);;Archive Files (*.tar *.tar.gz *.tar.bz2 *.tar.xz *.zip *.rar *.7z)"
        )
        if file_path:
            line_edit.setText(file_path)

    # TODO Remove in favor of PathsInputWidget
    def browse_directory(self, line_edit):
        dir_path = QtWidgets.QFileDialog.getExistingDirectory(self, "Select Directory", "")
        if dir_path:
            line_edit.setText(dir_path)

    def get_command(self) -> str:
        # TODO get a properly quoted and escaped version. Sounds like something for a standalone function.
        archive_path = self.commit_archive_input.text()
        overlay_path = self.commit_overlay_input.text()
        encoding = self.commit_encoding_input.text().strip() or 'tarfile.ENCODING'

        if archive_path and overlay_path:
            command = f"ratarmount --commit-overlay --write-overlay {overlay_path} {archive_path}"
            if encoding != 'tarfile.ENCODING':
                command += f" --encoding {encoding}"
            return command
        return "ratarmount --commit-overlay --write-overlay <overlay> <archive>"

    def commit_overlay(self):
        archive_path = self.commit_archive_input.text()
        overlay_path = self.commit_overlay_input.text()
        encoding = self.commit_encoding_input.text().strip() or 'tarfile.ENCODING'

        if not archive_path or not overlay_path:
            # TODO Instead of this warning, only enable the button when all arguments are correct.
            QtWidgets.QMessageBox.warning(self, "Warning", "Please specify both archive and overlay paths.")
            return

        try:
            # TODO Call this
            # TODO Use subprocess and show a progress/spinner bar?
            # https://stackoverflow.com/questions/75856341/pyside6-concurrent-information-window-with-delay-from-long-process
            # TODO Need to add option so that it does not wait for the "commit" input
            #      Or show a confirmation dialog, but that would require quite a bit of refactoring to avoid code
            #      duplication
            # TODO Add warning about this being an experimental feature?
            # TODO Basically everything that the printDebug argument controls would be served better as popups
            from ratarmount.actions import commit_overlay

            # This would normally call the actual function
            # commit_overlay(overlay_path, archive_path, encoding=encoding, printDebug=debug)

            QtWidgets.QMessageBox.information(
                self,
                "Commit",
                f"Would commit overlay from {overlay_path} to {archive_path}\nEncoding: {encoding}",
            )

        except Exception as e:
            QtWidgets.QMessageBox.critical(self, "Error", f"Failed to commit overlay: {e!s}")

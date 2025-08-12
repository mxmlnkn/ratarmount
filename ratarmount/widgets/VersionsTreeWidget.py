from qtpy import QtWidgets
from qtpy.QtCore import Qt
from qtpy.QtGui import QAction, QFontMetrics, QIcon, QKeySequence, QShortcut
from qtpy.QtWidgets import QTreeWidgetItem, QTreeWidgetItemIterator

from ratarmount.dependencies import create_oss_markdown, gather_versions

from .MarkdownViewer import MarkdownViewer


class VersionsTreeWidget(QtWidgets.QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)

        self.tree = QtWidgets.QTreeWidget()
        self.tree.setHeaderLabels(["Name", "Version", "License"])
        self.tree.setSelectionMode(QtWidgets.QAbstractItemView.SelectionMode.ExtendedSelection)
        self.tree.setContextMenuPolicy(Qt.CustomContextMenu)
        self.tree.customContextMenuRequested.connect(self.on_context_menu)
        self.tree.currentItemChanged.connect(self.show_oss_details)
        self.tree.setProperty("tree-style", "dark")
        QShortcut(QKeySequence.Copy, self.tree, self.action_copy)

        # Add "Copy All" button.
        actions_layout = QtWidgets.QHBoxLayout()
        copy_all_button = QtWidgets.QToolButton()
        copy_all_button.setDefaultAction(self.create_copy_action())
        actions_layout.addStretch()
        actions_layout.addWidget(copy_all_button)

        tree_layout = QtWidgets.QVBoxLayout()
        tree_layout.setContentsMargins(0, 0, 0, 0)
        tree_layout.addWidget(self.tree)
        tree_layout.addLayout(actions_layout)

        tree_and_actions = QtWidgets.QWidget()
        tree_and_actions.setLayout(tree_layout)

        # OSS details panel
        self.oss_details = MarkdownViewer()
        self.oss_details.toc_tree.setVisible(False)
        self.oss_details.markdown_widget.setPlaceholderText("Select a dependency to view details...")

        self.splitter = QtWidgets.QSplitter(Qt.Horizontal)
        self.splitter.addWidget(tree_and_actions)
        self.splitter.addWidget(self.oss_details)

        actions_layout.setContentsMargins(0, 0, 0, self.splitter.handleWidth())

        main_layout = QtWidgets.QVBoxLayout()
        main_layout.setContentsMargins(0, 0, 0, 0)
        main_layout.addWidget(self.splitter)
        self.setLayout(main_layout)

    def get_command(self) -> str:
        return "ratarmount --version; ratarmount --oss-attributions"

    def show_oss_details(self, item):
        if info := item.data(0, Qt.UserRole):
            self.oss_details.setMarkdown(create_oss_markdown(info))
        else:
            self.oss_details.clear()

    def populate(self):
        if self.tree.topLevelItemCount() > 0:
            return

        top_level_items = []
        for label, versions in gather_versions().items():
            parent_item = None
            if isinstance(label, int):
                if label > 1:
                    parent_item = QTreeWidgetItem([f"Level {label} Dependencies"])
            else:
                parent_item = QTreeWidgetItem([label])

            version_items = []
            for name, info in versions:
                item = QTreeWidgetItem([str(name), str(info.version), str(info.license_short)])
                item.setData(0, Qt.UserRole, info)
                version_items.append(item)

            if parent_item is None:
                top_level_items.extend(version_items)
            else:
                parent_item.addChildren(version_items)
                top_level_items.append(parent_item)

        self.tree.clear()
        if top_level_items:
            self.tree.addTopLevelItems(top_level_items)
            self.tree.setCurrentItem(top_level_items[0])

        font_metrics = QFontMetrics(self.tree.font())
        # First column width also includes the triangle indicator!
        column_placeholders = ["D|> Level 0 Depe", "_Version_", "_License_"]
        tree_width = 0
        for column, placeholder in enumerate(column_placeholders):
            column_width = int(font_metrics.boundingRect(placeholder).width())
            self.tree.setColumnWidth(column, column_width)
            tree_width += column_width
        tree_width = int(tree_width * 1.05)
        self.splitter.setSizes([tree_width, self.width() - tree_width])

    def create_copy_action(self):
        action = QAction(QIcon.fromTheme("edit-copy"), "Copy All")
        action.setToolTip("Copy versions to clipboard.")
        action.triggered.connect(lambda: self.action_copy(QTreeWidgetItemIterator.All))
        return action

    def action_copy(self, flags=QTreeWidgetItemIterator.Selected):
        # By default Ctrl+C only copies the "current item", not all selected. Weird behavior :/
        # For another weird reason, selectedItems and selectedIndexes return in children first instead of display
        # order. Therefore, use QTreeWidgetItemIterator. Performance should not be a problem for this small tree
        lines = []
        iterator = QTreeWidgetItemIterator(self.tree, flags=flags)
        while item := iterator.value():
            if item.parent() is None and item.childCount() > 0:
                lines.append('')

            lines.append(' '.join(item.text(column) for column in range(item.columnCount())))

            if item.parent() is None and item.childCount() > 0:
                lines.append('')

            iterator += 1

        QtWidgets.QApplication.clipboard().setText('\n'.join(lines).strip())

    def on_context_menu(self, point):
        menu = QtWidgets.QMenu(self)

        menu.addAction(QIcon.fromTheme("edit-copy"), "&Copy", self.action_copy)
        menu.addAction(QIcon.fromTheme("edit-copy"), "Copy All", lambda: self.action_copy(QTreeWidgetItemIterator.All))
        menu.addAction("Select &All", self.tree.selectAll)
        menu.addAction("Expand All", self.tree.expandAll)
        menu.addAction("Collapse All", self.tree.collapseAll)

        menu.exec(self.tree.viewport().mapToGlobal(point))

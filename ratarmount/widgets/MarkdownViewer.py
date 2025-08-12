import base64
import contextlib
import logging
import re
import urllib

from qtpy.QtCore import QFile, QIODevice, Qt
from qtpy.QtWidgets import (
    QHBoxLayout,
    QSplitter,
    QTextBrowser,
    QTreeWidget,
    QTreeWidgetItem,
    QWidget,
)

try:
    import markdown_it
except ImportError:
    markdown_it = None  # type: ignore

from ratarmount.widgets import styles

from .resources.resources import load_resource

logger = logging.getLogger(__name__)
if logger.getEffectiveLevel() >= logging.DEBUG:
    # There is too much output, namely for every markdown token. Imo that should have been logged with TRACING level.
    logging.getLogger("markdown_it.rules_block").setLevel(logging.WARNING)


from ratarmount.dependencies import get_readme


class MarkdownViewer(QWidget):
    def __init__(self, parent=None):
        super().__init__(parent)

        # TODO The relative path links to BENCHMARK.md will only work when executed in the root of the ratarmount module
        # TODO Show multiple markdown files in a tree:
        # ratarmount/
        #    ./README.md
        #    ./benchmarks/BENCHMARKS.md
        #    ./CHANGELOG.md
        # ratarmountcore/
        #    ./core/README.md
        #    ./core/CHANGELOG.md
        # librapidarchive/ (indexed_bzip2 / rapidgzip)
        #    ./README.md
        # ... because why not -> because bundling files (markdown, images) and checking correct appearance
        #                        is cumbersome :/

        self.download_images = False

        self.splitter = QSplitter(Qt.Horizontal)

        # TOC tree view for navigation
        self.toc_tree = QTreeWidget()
        self.toc_tree.setHeaderHidden(True)
        self.toc_tree.currentItemChanged.connect(
            lambda item: self.markdown_widget.scrollToAnchor(item.data(0, Qt.UserRole))
        )

        self.markdown_widget = QTextBrowser()
        self.markdown_widget.setOpenLinks(True)
        self.markdown_widget.setOpenExternalLinks(True)

        self.splitter.addWidget(self.toc_tree)
        self.splitter.addWidget(self.markdown_widget)

        layout = QHBoxLayout()
        layout.setContentsMargins(0, 0, 0, 0)
        layout.addWidget(self.splitter)
        self.setLayout(layout)

    def populate(self):
        if self.toc_tree.topLevelItemCount() > 0:
            return

        # The built-in markdown rendering does not work at all with the GitHub-flavored CommonMark dialect I use.
        # Remove the centered header including all the badges, which we do not want to download!
        markdown_contents = get_readme("ratarmount").split('</div>', maxsplit=1)[-1]
        markdown_contents = re.sub('^.*/repology.org/badge/.*', '', markdown_contents, flags=re.MULTILINE)
        self.setMarkdown(markdown_contents)

    def setMarkdown(self, markdown_contents):
        self.toc_tree.clear()

        if markdown_it is None:
            self.markdown_widget.setPlainText(markdown_contents)
            self.toc_tree.setVisible(False)
            return

        markdown = markdown_it.MarkdownIt('commonmark', {'breaks': False, 'html': True})
        markdown.enable('table')  # For some reason, markdown tables are not parsed by default.

        headings: list[tuple[int, str, str]] = []

        def render_heading(self_markdown, tokens, idx, options, env):
            heading = tokens[idx]

            level = int(heading.tag[1])  # Tag is something like h1, h2, ... So, grab the number.
            # Look ahead for inline token containing the actual text.
            inline = tokens[idx + 1]
            title = inline.content if inline.type == "inline" else ""

            # Add id attributes to headings so that links to anchors work.
            anchor_id = re.sub(r"[^\w\u4e00-\u9fff\- ]", "", title.strip().lower().replace(" ", "-"))
            heading.attrSet('id', anchor_id)
            headings.append((level, title, anchor_id))

            return self_markdown.renderToken(tokens, idx, options, env)

        # Note that this sequential downloading is pretty slow
        def render_embedded_image(self_markdown, tokens, idx, options, env):
            image = tokens[idx]
            source = image.attrGet('src')
            if not source.startswith('https://'):
                return self_markdown.renderToken(tokens, idx, options, env)

            image_data = load_resource(source.replace('https://', ''))
            # try:
            # except Exception as exception:
            #    return f"Error loading README: {exception}"

            if not image_data:
                file = QFile(source.replace('https://', ':/'))
                if file.exists() and file.open(QIODevice.ReadOnly):
                    image_data = file.readAll()

            if not image_data and self.download_images and source.startswith('https://'):
                with contextlib.suppress(urllib.error.HTTPError):
                    image_data = urllib.request.urlopen(source).read()

            if image_data:
                image.attrSet('src', "data:image;base64," + base64.b64encode(image_data).decode("utf-8"))

            return self_markdown.renderToken(tokens, idx, options, env)

        markdown.add_render_rule("heading_open", render_heading)
        markdown.add_render_rule("image", render_embedded_image)

        # Make large images shrink with the QTextBrowser width to avoid horizontal scroll bars!
        # QTextBrowser links cannot be styled via an application QSS, so we need to apply the theme here,
        # or with setDefaultStyleSheet. https://stackoverflow.com/a/33197907/2191065
        # Damn QTextBrowser... a:hover does not seem to work, neither do any of the other selectors.
        # a:links seems to be the only thing working.
        style = f"""
            <style>
            img {{
                max-width: 1000%;
                width: auto;
            }}
            a:link {{
                color: {styles.PARAMETERS[styles.MODE]['GREEN_PRIMARY']};
                text-decoration: none;
            }}
            </style>
            """
        self.markdown_widget.setHtml(style + markdown.render(markdown_contents))

        # There are lines starting with '#' that are not headers, namely inside code blocks.
        # Therefore, try to naively parse out the code blocks.
        parents = [QTreeWidgetItem()]
        for level, heading, anchor_id in headings:
            item = QTreeWidgetItem([heading])
            item.setData(0, Qt.UserRole, anchor_id)

            parents = parents[: max(1, level)]
            parents[-1].addChild(item)
            parents.append(item)

        self.toc_tree.addTopLevelItems(parents[0].takeChildren())
        self.toc_tree.expandAll()

        width = self.splitter.width()
        self.splitter.setSizes([int(0.25 * width), int(0.75 * width)])

    def clear(self) -> None:
        self.toc_tree.clear()
        self.markdown_widget.clear()

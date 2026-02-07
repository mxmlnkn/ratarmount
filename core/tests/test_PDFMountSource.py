# pylint: disable=wrong-import-position
# pylint: disable=redefined-outer-name

import io
import os
import sys

import pypdf

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from helpers import find_test_file
from ratarmountcore.mountsource.formats.pdf import PDFMountSource


def test_pdf_with_embedded_file():
    # Create a simple PDF with embedded files.
    pdf_writer = pypdf.PdfWriter()
    pdf_writer.add_blank_page(width=612, height=792)
    data = b"This is a test embedded file"
    pdf_writer.add_attachment("test.bin", data)
    pdf_writer.add_attachment("test2.bin", data + b"?")
    pdf_writer.add_attachment("test2.bin", data + b"!")
    pdf_buffer = io.BytesIO()
    pdf_writer.write(pdf_buffer)

    pdf_buffer.seek(0)
    with PDFMountSource(pdf_buffer) as mount_source:
        files = mount_source.list('/')
        assert files
        assert len(files) == 2

        toTest = [("test.bin", 0, data), ("test2.bin", 0, data + b"!"), ("test2.bin", 1, data + b"?")]
        for name, version, contents in toTest:
            assert name in files
            file_info = mount_source.lookup(name, version)
            if version == 0:
                assert files[name] == file_info
            else:
                assert files[name] != file_info

            with mount_source.open(file_info) as file:
                assert file.read() == contents


def test_latex_file():
    with PDFMountSource(find_test_file('example.pdf')) as mountSource:
        files = mountSource.list('/')
        assert files
        assert 'example.tex' in files
        assert 'single-file.tar' in files

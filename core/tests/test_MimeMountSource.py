# pylint: disable=wrong-import-position
# pylint: disable=protected-access

import email
import os
import stat
import sys
import tempfile
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

import pytest  # noqa: E402
from ratarmountcore.mountsource.formats.mime import MIMEMountSource  # noqa: E402


def create_test_mime_message():
    """Create a test MIME message with various parts for testing."""
    message = MIMEMultipart('mixed')
    message['Subject'] = "Test MIME Message"
    message['From'] = "sender@example.com"
    message['To'] = "recipient@example.com"

    # Add text part
    text_part = MIMEText("This is the main text content", 'plain')
    message.attach(text_part)

    # Add HTML part
    html_part = MIMEText("<html><body>This is HTML content</body></html>", 'html')
    message.attach(html_part)

    # Add attachment
    attachment = MIMEApplication(b"This is an attachment", _subtype='txt')
    attachment.add_header("Content-Disposition", "attachment", filename='test.txt')
    message.attach(attachment)

    # Add nested multipart
    nested = MIMEMultipart('related')
    nested['Subject'] = "Nested Message"
    nested_text = MIMEText("This is nested content", 'plain')
    nested.attach(nested_text)
    nested_attachment = MIMEApplication(b"This is a nested attachment", _subtype='txt')
    nested_attachment.add_header('Content-Disposition', 'attachment', filename="nested.txt")
    nested.attach(nested_attachment)
    message.attach(nested)

    return message


def create_test_mht():
    """Create a test MHT file with various parts for testing."""
    message = MIMEMultipart('related')
    message['Subject'] = "Test MHT"
    message['From'] = "sender@example.com"
    message['To'] = "recipient@example.com"
    message['Content-Type'] = 'multipart/related; type="text/html"'

    # Add HTML part
    html_part = MIMEText("<html><body>This is MHT content</body></html>", 'html')
    message.attach(html_part)

    # Add image
    image = MIMEApplication(b"fake image data", _subtype='png')
    image.add_header('Content-Disposition', 'inline', filename='test.png')
    image.add_header('Content-ID', '<test.png>')
    message.attach(image)

    return message


class TestMIMEMountSource:
    @staticmethod
    def test_simple_mime():
        message = create_test_mime_message()
        with tempfile.NamedTemporaryFile(suffix='.eml', delete=False) as tmp:
            tmp.write(message.as_bytes())
            tmp_path = tmp.name

        try:
            with MIMEMountSource(tmp_path) as mountSource:
                # Test root directory
                root_files = mountSource.listDir('/')
                assert root_files
                assert 'text_plain' in root_files
                assert 'text_html' in root_files
                assert 'test.txt' in root_files
                assert 'nested_0' in root_files

                # Test file contents
                with mountSource.open(mountSource.getFileInfo('/text_plain')) as file:
                    assert file.read() == b"This is the main text content"

                with mountSource.open(mountSource.getFileInfo('/text_html')) as file:
                    assert file.read() == b"<html><body>This is HTML content</body></html>"

                with mountSource.open(mountSource.getFileInfo('/test.txt')) as file:
                    assert file.read() == b"This is an attachment"

                # Test nested content
                nested_files = mountSource.listDir('/nested_0')
                assert nested_files
                assert 'text_plain' in nested_files
                assert 'nested.txt' in nested_files

                with mountSource.open(mountSource.getFileInfo('/nested_0/text_plain')) as file:
                    assert file.read() == b"This is nested content"

                with mountSource.open(mountSource.getFileInfo('/nested_0/nested.txt')) as file:
                    assert file.read() == b"This is a nested attachment"

                # Test file metadata
                for path in ['/text_plain', '/text_html', '/test.txt', '/nested_0/text_plain', '/nested_0/nested.txt']:
                    fileInfo = mountSource.getFileInfo(path)
                    assert fileInfo
                    assert not stat.S_ISDIR(fileInfo.mode)
                    assert mountSource.fileVersions(path) == 1
                    assert not mountSource.listDir(path)

                for path in ['/', '/nested_0']:
                    fileInfo = mountSource.getFileInfo(path)
                    assert fileInfo
                    assert stat.S_ISDIR(fileInfo.mode)
                    assert mountSource.fileVersions(path) == 1
                    assert mountSource.listDir(path)

        finally:
            os.unlink(tmp_path)

    @staticmethod
    def test_simple_mht():
        message = create_test_mht()
        with tempfile.NamedTemporaryFile(suffix='.mht', delete=False) as tmp:
            tmp.write(message.as_bytes())
            tmp_path = tmp.name

        try:
            with MIMEMountSource(tmp_path) as mountSource:
                # Test root directory
                root_files = mountSource.listDir('/')
                assert root_files
                assert 'text_html' in root_files
                assert 'test.png' in root_files

                # Test file contents
                with mountSource.open(mountSource.getFileInfo('/text_html')) as file:
                    assert file.read() == b"<html><body>This is MHT content</body></html>"

                with mountSource.open(mountSource.getFileInfo('/test.png')) as file:
                    assert file.read() == b"fake image data"

                # Test file metadata
                for path in ['/text_html', '/test.png']:
                    fileInfo = mountSource.getFileInfo(path)
                    assert fileInfo
                    assert not stat.S_ISDIR(fileInfo.mode)
                    assert mountSource.fileVersions(path) == 1
                    assert not mountSource.listDir(path)

                root_info = mountSource.getFileInfo('/')
                assert root_info
                assert stat.S_ISDIR(root_info.mode)
                assert mountSource.fileVersions('/') == 1
                assert mountSource.listDir('/')

        finally:
            os.unlink(tmp_path)

    @staticmethod
    def test_empty_message():
        message = MIMEMultipart('mixed')
        message['Subject'] = "Empty Message"
        message['From'] = "sender@example.com"
        message['To'] = "recipient@example.com"

        with tempfile.NamedTemporaryFile(suffix='.eml', delete=False) as tmp:
            tmp.write(message.as_bytes())
            tmp_path = tmp.name

        try:
            with MIMEMountSource(tmp_path) as mountSource:
                root_files = mountSource.listDir('/')
                assert not root_files  # Empty message should have no files

                root_info = mountSource.getFileInfo('/')
                assert root_info
                assert stat.S_ISDIR(root_info.mode)
                assert mountSource.fileVersions('/') == 1

        finally:
            os.unlink(tmp_path)

    @staticmethod
    def test_invalid_message():
        with tempfile.NamedTemporaryFile(suffix='.eml', delete=False) as tmp:
            tmp.write(b'This is not a valid MIME message')
            tmp_path = tmp.name

        try:
            with pytest.raises(ValueError, match='valid'), MIMEMountSource(tmp_path):
                pass
        finally:
            os.unlink(tmp_path)

    @staticmethod
    def test_file_operations():
        message = create_test_mime_message()
        with tempfile.NamedTemporaryFile(suffix='.eml', delete=False) as tmp:
            tmp.write(message.as_bytes())
            tmp_path = tmp.name

        try:
            with MIMEMountSource(tmp_path) as mountSource:
                # Test seeking
                with mountSource.open(mountSource.getFileInfo('/text_plain')) as file:
                    assert file.seek(5) == 5
                    assert file.read() == b"is the main text content"
                    assert file.seek(0) == 0
                    assert file.read(5) == b"This "

                # Test non-existent file
                assert mountSource.getFileInfo('/nonexistent.txt') is None
                assert not mountSource.listDir('/nonexistent.txt')

                # Test directory as file
                assert mountSource.getFileInfo('/') is not None
                assert mountSource.listDir('/') is not None
                #with pytest.raises(Exception):
                #    mountSource.open(mountSource.getFileInfo('/'))
                mountSource.open(mountSource.getFileInfo('/'))

        finally:
            os.unlink(tmp_path)

# pylint: disable=wrong-import-order
# pylint: disable=wrong-import-position

import hashlib
import io
import os
import stat
import sys

import pytest
from helpers import copy_test_file

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from ratarmountcore.mountsource.formats.html import DataURLFile, HTMLMountSource, gather_embedded_files

HTML_FILES_WITH_SINGLE_DATA_URL = [
    '<html><body><img src="{}"></body></html>',
    '<!-- アドマネージャー--><html><body><img src="{}"></body></html>',
    '''
    <html>
    <body>
        <img src="{}">
    </body>
    </html>
    ''',
    '''<!DOCTYPE html><html><head><style>
        body {{
            background: url({});
        }}
    </style></head><body></body></html>
    ''',
]


class TestHTMLDataURLParser:
    @pytest.mark.parametrize('i_html', list(range(len(HTML_FILES_WITH_SINGLE_DATA_URL))))
    def test_base64(self, i_html):
        data_url = "data:image/webp;base64,UklGRiQAAABXRUJQVlA4IBgAAAAwAQCdASoBAAEAAQAcJaQAA3AA/v3AgAA="
        html_file = io.StringIO(HTML_FILES_WITH_SINGLE_DATA_URL[i_html].format(data_url))

        files = gather_embedded_files(html_file)
        assert len(files) == 1
        embedded_file = files[0]
        assert not embedded_file.original_url

        html_file.seek(embedded_file.span[0])
        extracted_data_url = html_file.read(embedded_file.span[1] - embedded_file.span[0])
        assert extracted_data_url == data_url

        file = DataURLFile(extracted_data_url)
        assert file.mime_type == 'image/webp'
        assert file.encoding == 'ascii'
        assert file.is_base64

    @pytest.mark.parametrize('i_html', list(range(len(HTML_FILES_WITH_SINGLE_DATA_URL))))
    def test_utf8(self, i_html):
        data_url = "data:text/css;utf8,body {&#37;20font-family: Arial, sans-serif };"
        html_file = io.StringIO(HTML_FILES_WITH_SINGLE_DATA_URL[i_html].format(data_url))

        files = gather_embedded_files(html_file)
        assert len(files) == 1
        embedded_file = files[0]
        assert not embedded_file.original_url

        html_file.seek(embedded_file.span[0])
        extracted_data_url = html_file.read(embedded_file.span[1] - embedded_file.span[0])
        assert extracted_data_url == data_url

        file = DataURLFile(extracted_data_url)
        assert file.mime_type == 'text/css'
        assert file.encoding == 'utf8'
        assert not file.is_base64
        assert file.read() == b"body { font-family: Arial, sans-serif };"

    @staticmethod
    def test_medium_html():
        files = [
            ("/https:/example.com/background.png", "63a4c91aad60561f58c7ccf84fe56828"),
            ("/https:/example.com/style.css", "a6185e9bfab7c22dd29a0465ef458ec1"),
            ("/https:/example.com/images/logo.png", "b357a19c87624c7c4d131aeeb4ae677f"),
            ("/https:/example.com/css/style.css", "9d37fa9c078df306ef2c13475344aa78"),
            (
                "/5d5ff3dc6082c27a2fa39d51f057e44867daa4f658b49af32e21100fb2ec080d.js",
                "7f19e452de5689d420dc85dc6b088b49",
            ),
            ("/https:/example.com/docs/readme.txt", "835a667e70862458346dcd66a7d94db7"),
        ]
        with copy_test_file('save_page_we.html') as path, HTMLMountSource(path) as mountSource:
            for path, md5sum in files:
                path_split = path.split('/')
                for n in range(len(path_split) - 1):
                    folder = '/'.join(path_split[:n])

                    fileInfo = mountSource.lookup(folder)
                    assert fileInfo
                    assert stat.S_ISDIR(fileInfo.mode)

                    assert mountSource.versions(folder) == 1
                    assert mountSource.list(folder)

                fileInfo = mountSource.lookup(path)
                assert fileInfo
                assert not stat.S_ISDIR(fileInfo.mode)
                assert stat.S_ISREG(fileInfo.mode)

                assert mountSource.versions(path) == 1
                assert not mountSource.list(path)
                with mountSource.open(fileInfo) as file:
                    assert hashlib.md5(file.read()).hexdigest() == md5sum

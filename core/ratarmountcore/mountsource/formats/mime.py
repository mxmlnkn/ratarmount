"""
This module provides a MountSource implementation for MIME messages (EML and MHT files).
It uses Python's email module to parse the messages and StenciledFile for efficient access.
"""

import email
import os
import stat
import time
from email.message import Message
from typing import IO, Iterable, Dict, Optional, Tuple, Union

from ratarmountcore.mountsource.MountSource import FileInfo, MountSource, createRootFileInfo
from ratarmountcore.StenciledFile import StenciledFile
from ratarmountcore.utils import overrides


def isMimeFile(file: IO[bytes]) -> bool:
    """
    Check if a file is a MIME message (EML or MHT) by examining its headers.
    Returns True if the file appears to be a valid MIME message.
    """
    try:
        msg = email.message_from_binary_file(file)
        if not isinstance(msg, Message):
            return False

        # Check for common MIME message content types
        content_type = msg.get_content_type()
        return content_type in ('message/rfc822', 'multipart/related', 'multipart/mixed')
    except Exception:
        return False
    finally:
        file.seek(0)


class MIMEMountSource(MountSource):
    """
    A MountSource implementation for MIME messages (EML and MHT files).
    Provides access to the message parts as files in a virtual filesystem.
    """

    def __init__(self, fileOrPath: Union[str, bytes, os.PathLike]):
        self._file = None
        self._filePath = None
        self._message = None
        self._fileTree = {}

        if isinstance(fileOrPath, (str, bytes, os.PathLike)):
            self._filePath = str(fileOrPath)
            self._file = open(self._filePath, 'rb')
        else:
            self._file = fileOrPath

        if not isMimeFile(self._file):
            raise ValueError("Not a valid MIME message file")

        self._message = email.message_from_binary_file(self._file)
        self._buildFileTree()

    def _buildFileTree(self):
        """Build the virtual file tree from the MIME message structure."""
        self._fileTree = {}
        self._processMessagePart(self._message, '/')

    def _processMessagePart(self, part: Message, path: str):
        """Process a MIME message part and add it to the file tree."""
        if part.is_multipart():
            # For multipart messages, create a directory and process each part
            if path != '/':
                self._fileTree[path] = createRootFileInfo(path, stat.S_IFDIR | 0o755)

            for i, subpart in enumerate(part.get_payload()):
                if isinstance(subpart, Message):
                    # Use content type as directory name for nested messages
                    if subpart.get_content_type() == 'message/rfc822':
                        subpath = os.path.join(path, f'nested_{i}')
                    else:
                        subpath = os.path.join(path, subpart.get_content_type().replace('/', '_'))
                    self._processMessagePart(subpart, subpath)
        else:
            # For single parts, create a file
            filename = None
            if 'Content-Disposition' in part:
                # Try to get filename from Content-Disposition
                for param in part.get_params():
                    if param[0].lower() == 'filename':
                        filename = param[1]
                        break

            if not filename and 'Content-Type' in part:
                # Use content type as filename if no filename in Content-Disposition
                filename = part.get_content_type().replace('/', '_')

            if not filename:
                filename = f'part_{len(self._fileTree)}'

            filepath = os.path.join(path, filename)
            size = len(part.get_payload(decode=True)) if part.get_payload() else 0

            self._fileTree[filepath] = FileInfo(
                filepath,
                stat.S_IFREG | 0o644,
                size,
                int(time.time()),
                userdata=part
            )

    @overrides(MountSource)
    def __enter__(self):
        return self

    @overrides(MountSource)
    def __exit__(self, exc_type, exc_val, exc_tb):
        if self._file and self._filePath:
            self._file.close()
            self._file = None

    @overrides(MountSource)
    def listDir(self, path: str) -> Optional[Union[Iterable[str], Dict[str, FileInfo]]]:
        if path == '/':
            return {name: info for name, info in self._fileTree.items() if '/' not in name[1:]}

        if path not in self._fileTree or not stat.S_ISDIR(self._fileTree[path].mode):
            return None

        prefix = path + '/'
        return {
            name[len(prefix):]: info
            for name, info in self._fileTree.items()
            if name.startswith(prefix) and '/' not in name[len(prefix):]
        }

    @overrides(MountSource)
    def getFileInfo(self, path: str, fileVersion: int = 0) -> Optional[FileInfo]:
        return self._fileTree.get(path)

    @overrides(MountSource)
    def fileVersions(self, path: str) -> int:
        return 1 if path in self._fileTree else 0

    @overrides(MountSource)
    def open(self, fileInfo: FileInfo) -> IO[bytes]:
        if not fileInfo or stat.S_ISDIR(fileInfo.mode):
            raise IsADirectoryError(f"'{fileInfo.name}' is a directory")

        part = fileInfo.userdata
        if not part:
            raise FileNotFoundError(f"File not found: {fileInfo.name}")

        content = part.get_payload(decode=True)
        if not content:
            content = b''

        return StenciledFile(content)

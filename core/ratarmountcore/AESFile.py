#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import hashlib
import io

from typing import IO, Union

import cryptography.hazmat.primitives.ciphers as ciphers

from .utils import overrides


# https://github.com/pvizeli/securetar


def generateSecuretarInitializationVector(key: bytes, salt: bytes) -> bytes:
    result = key + salt
    for _ in range(100):
        result = hashlib.sha256(result).digest()
    return result[:16]


class AESFile(io.RawIOBase):
    """A file abstraction layer giving a view to an AES encrypted file."""

    def __init__(
        self,
        fileOrPath: Union[str, IO[bytes]],
        aesKey: bytes,
        fileObjectLock=None,
    ) -> None:
        """
        fileObject: Python file-like object to be decrypted on the fly.
        aesKey: Must be 128-bit long (32 B).
        """
        io.RawIOBase.__init__(self)

        self.offset = 0
        self.fileObjectLock = fileObjectLock
        self.fileObject = open(fileOrPath, 'rb') if isinstance(fileOrPath, str) else fileOrPath
        self.aesKey = aesKey

        if not self.fileObject.readable():
            raise ValueError("All file objects to be decrypted must be readable!")

        # Read initialization vector for cipher block cipher.
        self.fileObject.seek(0)
        randomness = self.fileObject.read(16)

        # Create Cipher
        self._aes = ciphers.Cipher(
            ciphers.algorithms.AES(self.aesKey),
            ciphers.modes.CBC(generateSecuretarInitializationVector(self.aesKey, randomness)),
        )

        self._decrypt = self._aes.decryptor()

        self._buffer = self._decrypt.update(self.fileObject.read())
        self.size = len(self._buffer)

        # Seek to the first stencil offset in the underlying file so that "read" will work out-of-the-box
        self.seek(0)

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        pass

    @overrides(io.RawIOBase)
    def close(self) -> None:
        # Don't close the file objects given to us.
        pass

    @overrides(io.RawIOBase)
    def fileno(self) -> int:
        # This is a virtual Python level file object and therefore does not have a valid OS file descriptor!
        raise io.UnsupportedOperation()

    @overrides(io.RawIOBase)
    def seekable(self) -> bool:
        return self.fileObject.seekable()

    @overrides(io.RawIOBase)
    def readable(self) -> bool:
        return True

    @overrides(io.RawIOBase)
    def writable(self) -> bool:
        return False

    @overrides(io.RawIOBase)
    def readinto(self, buffer):
        """Generic implementation which uses read."""
        with memoryview(buffer) as view, view.cast("B") as byteView:  # type: ignore
            readBytes = self.read(len(byteView))
            byteView[: len(readBytes)] = readBytes
        print("readinto return:", len(readBytes))
        return len(readBytes)

    @overrides(io.RawIOBase)
    def read(self, size: int = -1) -> bytes:
        if size == -1:
            size = self.size

        assert self.offset <= self.size
        readableSize = min(size, self.size - self.offset)
        result = self._buffer[self.offset : self.offset + readableSize]
        self.offset += len(result)
        return result

        # result = b''
        # i = self._findStencil(self.offset)
        # if i >= len(self.sizes):
        #    return result
        #
        ## Iterate over AES cipher blocks?
        # with self.fileObjectLock if self.fileObjectLock else _DummyContext():
        #    # Note that seek and read of the file object itself do not seem to check against this and
        #    # instead lead to a segmentation fault in the multithreading tests.
        #    if self.fileObjects[i].closed:
        #        raise ValueError("A closed file can't be read from!")
        #
        #    offset = 0  # TODO
        #    readableSize = 0  # TODO
        #    self.fileObjects[i].seek(offset, io.SEEK_SET)
        #    tmp = self.fileObjects[i].read(readableSize)
        #    self.offset += len(tmp)
        #    result += tmp
        #
        # return result

    @overrides(io.RawIOBase)
    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_CUR:
            self.offset += offset
        elif whence == io.SEEK_END:
            self.offset = self.size + offset
        elif whence == io.SEEK_SET:
            self.offset = offset

        if self.offset < 0:
            raise ValueError("Trying to seek before the start of the file!")
        if self.offset >= self.size:
            return self.offset

        return self.offset

    @overrides(io.RawIOBase)
    def tell(self) -> int:
        return self.offset

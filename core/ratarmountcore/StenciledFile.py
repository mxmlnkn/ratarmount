import bisect
import contextlib
import io
from collections.abc import Sequence
from typing import IO, Callable, Optional

from .utils import FixedRawIOBase, overrides


class RawStenciledFile(FixedRawIOBase):
    # For a reference implementation based on RawIOBase, see "class SocketIO(io.RawIOBase)" in:
    #   https://github.com/python/cpython/blob/00ffdf27367fb9aef247644a96f1a9ffb5be1efe/Lib/socket.py#L683
    # or others implementations inside cpython:
    #   https://github.com/python/cpython/blob/00ffdf27367fb9aef247644a96f1a9ffb5be1efe/Lib/_compression.py#L33
    # For the internals of RawIOBase and others, see:
    #   https://github.com/python/cpython/tree/main/Lib/_pyio.py#L619
    """A file abstraction layer giving a stenciled view to an underlying file."""

    def __init__(
        self,
        fileStencils: Sequence[tuple[IO, int, int]],
        fileObjectLock=None,
    ) -> None:
        """
        stencils: A list tuples specifying the offset and length of the underlying file to use.
                  The order of these tuples will be kept.
                  The offset must be non-negative and the size must be positive.
        fileobj: (deprecated) Only either fileobj and stencils or fileStencils may be specified
        stencils: (deprecated) Only either fileobj and stencils or fileStencils may be specified
        fileStencils: Contains a list of (file object, offset, size) tuples. The offset and size
                      can be seen as a cut-out of the file object. All cut-outs are joined
                      together in the order of the list. Note that multiple cut-outs into the
                      same file object may be given by simply specifying the file object multiple
                      times in the list.

        Examples:
            stencil = [(5,7)]
                Makes a new 7B sized virtual file starting at offset 5 of fileobj.
            stencil = [(0,3),(5,3)]
                Make a new 6B sized virtual file containing bytes [0,1,2,5,6,7] of fileobj.
            stencil = [(0,3),(0,3)]
                Make a 6B size file containing the first 3B of fileobj twice concatenated together.
        """
        super().__init__()

        self.offset = 0
        self.fileObjectLock = fileObjectLock
        self.offsets: list[int] = []
        self.sizes: list[int] = []
        self.fileObjects: list[IO] = []

        if fileStencils:
            self.fileObjects, self.offsets, self.sizes = zip(*fileStencils)

        # Check whether values make sense
        for offset in self.offsets:
            assert offset >= 0
        for size in self.sizes:
            assert size >= 0

        # Filter out zero-sized regions (or else we would have to skip them inside 'readinto' in order to not
        # return an empty reply even though the end of file has not been reached yet!)
        selectedStencils = [i for i, size in enumerate(self.sizes) if size > 0]
        self.offsets = [self.offsets[i] for i in selectedStencils]
        self.sizes = [self.sizes[i] for i in selectedStencils]
        self.fileObjects = [self.fileObjects[i] for i in selectedStencils]

        # Check for readability
        for fileObject in self.fileObjects:
            if not fileObject.readable():
                raise ValueError("All file objects to be joined must be readable")

        # Calculate cumulative sizes
        self.cumsizes = [0]
        for size in self.sizes:
            self.cumsizes.append(self.cumsizes[-1] + size)

        # Seek to the first stencil offset in the underlying file so that "read" will work out-of-the-box
        self.seek(0)

    def _find_stencil(self, offset: int) -> int:
        """
        Return index to stencil where offset belongs to. E.g., for stencils [(3,5),(8,2)], offsets 0 to
        and including 4 will still be inside stencil (3,5), i.e., index 0 will be returned. For offset 6,
        index 1 would be returned because it now is in the second contiguous region / stencil.
        """
        # bisect_left( value ) gives an index for a lower range: value < x for all x in list[0:i]
        # Because value >= 0 and list starts with 0 we can therefore be sure that the returned i>0
        # Consider the stencils [(11,2),(22,2),(33,2)] -> cumsizes [0,2,4,6]. Seek to offset 2 should seek to 22.
        assert offset >= 0
        i = bisect.bisect_left(self.cumsizes, offset + 1) - 1
        assert i >= 0
        return i

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        pass

    @overrides(io.RawIOBase)
    def seekable(self) -> bool:
        return all(fobj.seekable() for fobj in self.fileObjects)

    @overrides(io.RawIOBase)
    def readable(self) -> bool:
        return True

    @overrides(io.RawIOBase)
    def readinto(self, buffer):
        """Generic implementation which uses read."""
        with memoryview(buffer) as view, view.cast("B") as byteView:  # type: ignore
            readBytes = self.read(len(byteView))
            byteView[: len(readBytes)] = readBytes
        return len(readBytes)

    def _read1_unlocked(self, size):
        assert size >= 0

        i = self._find_stencil(self.offset)
        if i >= len(self.sizes):
            return b''

        # Note that seek and read of the file object itself do not seem to check against this and
        # instead lead to a segmentation fault in the multithreading tests.
        if self.fileObjects[i].closed:
            raise ValueError("A closed file cannot be read from!")

        offsetInsideStencil = self.offset - self.cumsizes[i]
        assert offsetInsideStencil >= 0
        assert offsetInsideStencil < self.sizes[i]
        self.fileObjects[i].seek(self.offsets[i] + offsetInsideStencil, io.SEEK_SET)

        # Read as much as requested or as much as the current contiguous region / stencil still contains.
        readableSize = min(size, self.sizes[i] - (self.offset - self.cumsizes[i]))
        tmp = self.fileObjects[i].read(readableSize)
        self.offset += len(tmp)
        return tmp

    @overrides(io.RawIOBase)
    def read(self, size: int = -1) -> bytes:
        if size == -1:
            size = self.cumsizes[-1] - self.offset
        elif size == 0:
            return b''

        result = b''
        with self.fileObjectLock or contextlib.nullcontext():
            while len(result) < size:
                tmp = self._read1_unlocked(size - len(result))
                if not tmp:
                    break
                result += tmp

        return result

    @overrides(io.RawIOBase)
    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        if whence == io.SEEK_CUR:
            self.offset += offset
        elif whence == io.SEEK_END:
            self.offset = self.cumsizes[-1] + offset
        elif whence == io.SEEK_SET:
            self.offset = offset

        if self.offset < 0:
            raise ValueError("Trying to seek before the start of the file!")
        if self.offset >= self.cumsizes[-1]:
            return self.offset

        return self.offset

    @overrides(io.RawIOBase)
    def tell(self) -> int:
        return self.offset


class RawJoinedFileFromFactory(io.RawIOBase):
    def __init__(self, file_object_factories: Sequence[Callable[[], IO[bytes]]], file_lock=None) -> None:
        """
        Similar to StenciledFile but instead of joining a list of file objects, which neccessitates keeping all
        files open, this class opens each file on demand and only keeps one file open. This is useful to avoid
        exceeding the system limit for the number of opened I/O handles.
        """
        io.RawIOBase.__init__(self)

        self.offset = 0
        self.fileObjectLock = file_lock
        # Stores index and file object of currently opened file object. It basically is a cache for the factories.
        self.fileObject: Optional[tuple[int, IO[bytes]]] = None

        self.sizes: list[int] = []
        self.factories: list[Callable[[], IO[bytes]]] = []
        self.cumsizes = [0]
        self._seekable = True
        for factory in file_object_factories:
            with factory() as file:
                if not file.seekable():
                    self._seekable = False
                if not file.readable():
                    raise ValueError("All file objects to be joined must be readable")

                size = file.seek(0, io.SEEK_END)
                if size is None or size < 0:
                    raise ValueError(f"Failed to query size for factory: {factory}")

                # Filter out zero-sized regions (or else we would have to skip them inside 'readinto' in order to not
                # return an empty reply even though the end of file has not been reached yet!)
                if size > 0:
                    self.sizes.append(size)
                    self.factories.append(factory)
                    self.cumsizes.append(self.cumsizes[-1] + size)

    def _find_stencil(self, offset: int) -> int:
        # See StenciledFile._find_stencil
        assert offset >= 0
        i = bisect.bisect_left(self.cumsizes, offset + 1) - 1
        # i might be 0 when self.cumsizes is empty but even for no fileStencils, it is initialized with [0]
        assert i >= 0
        return i

    def _get_file_object(self, index: int) -> IO[bytes]:
        if self.fileObject and self.fileObject[0] == index:
            return self.fileObject[1]

        self._close_fileobj()
        self.fileObject = index, self.factories[index]()
        return self.fileObject[1]

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        self.close()

    def _close_fileobj(self) -> None:
        if self.fileObject and not self.fileObject[1].closed:
            self.fileObject[1].close()

    @overrides(io.RawIOBase)
    def close(self) -> None:
        self._close_fileobj()
        super().close()

    @overrides(io.RawIOBase)
    def seekable(self) -> bool:
        return self._seekable

    @overrides(io.RawIOBase)
    def readable(self) -> bool:
        return True

    @overrides(io.RawIOBase)
    def readinto(self, buffer):
        """Generic implementation which uses read."""
        with memoryview(buffer) as view, view.cast("B") as byteView:  # type: ignore
            readBytes = self.read(len(byteView))
            byteView[: len(readBytes)] = readBytes
        return len(readBytes)

    @overrides(io.RawIOBase)
    def read(self, size: int = -1) -> bytes:
        # See StenciledFile.read
        if size == -1:
            size = self.cumsizes[-1] - self.offset

        # This loop works in a kind of leapfrog fashion. On each even loop iteration it seeks to the next stencil
        # and on each odd iteration it reads the data and increments the offset inside the stencil!
        result = b''
        i = self._find_stencil(self.offset)
        if i >= len(self.sizes):
            return result

        with self.fileObjectLock or contextlib.nullcontext():
            fileObject = self._get_file_object(i)

            # Note that seek and read of the file object itself do not seem to check against this and
            # instead lead to a segmentation fault in the multithreading tests.
            if fileObject.closed:
                raise ValueError("A closed file can't be read from!")

            offsetInsideStencil = self.offset - self.cumsizes[i]
            assert offsetInsideStencil >= 0
            assert offsetInsideStencil < self.sizes[i]
            fileObject.seek(offsetInsideStencil, io.SEEK_SET)

            # Read as much as requested or as much as the current contiguous region / stencil still contains
            readableSize = min(size, self.sizes[i] - (self.offset - self.cumsizes[i]))
            tmp = fileObject.read(readableSize)
            self.offset += len(tmp)
            result += tmp

        return result

    @overrides(io.RawIOBase)
    def seek(self, offset: int, whence: int = io.SEEK_SET) -> int:
        """Generic implementation that simply updates self.offset nothing else."""
        if whence == io.SEEK_CUR:
            self.offset += offset
        elif whence == io.SEEK_END:
            self.offset = self.cumsizes[-1] + offset
        elif whence == io.SEEK_SET:
            self.offset = offset

        # Same behavior for seeking before the file begin and after the file end as io.BytesIO.
        self.offset = max(self.offset, 0)
        if self.offset >= self.cumsizes[-1]:
            return self.offset

        return self.offset

    @overrides(io.RawIOBase)
    def tell(self) -> int:
        return self.offset


class StenciledFile(io.BufferedReader):
    def __init__(
        self, fileStencils: Sequence[tuple[IO, int, int]], fileObjectLock=None, bufferSize=io.DEFAULT_BUFFER_SIZE
    ) -> None:
        """
        bufferSize: Gets forwarded to io.BufferedReader.__init__ buffer_size argument and has the same semantic,
                    i.e., must be > 0. If it should be unbuffered, use RawStenciledFile directly instead.
        """
        super().__init__(RawStenciledFile(fileStencils, fileObjectLock), buffer_size=bufferSize)


class JoinedFile(io.BufferedReader):
    def __init__(self, file_objects: Sequence[IO], file_lock=None, buffer_size: int = io.DEFAULT_BUFFER_SIZE) -> None:
        sizes = [fobj.seek(0, io.SEEK_END) for fobj in file_objects]
        for fobj, size in zip(file_objects, sizes):
            if size is None:
                raise ValueError("Failed to query size of file object:", fobj)

        fileStencils = [(fobj, 0, size or 0) for fobj, size in zip(file_objects, sizes)]
        super().__init__(RawStenciledFile(fileStencils=fileStencils, fileObjectLock=file_lock), buffer_size=buffer_size)


class JoinedFileFromFactory(io.BufferedReader):
    def __init__(
        self,
        file_object_factories: Sequence[Callable[[], IO]],
        file_lock=None,
        buffer_size: int = io.DEFAULT_BUFFER_SIZE,
    ) -> None:
        """
        Similar to JoinedFile but instead of joining a list of file objects, which neccessitates keeping all
        files open, this class opens each file on demand and only keeps one file open. This is useful to avoid
        exceeding the system limit for the number of opened I/O handles.
        """
        super().__init__(RawJoinedFileFromFactory(file_object_factories, file_lock), buffer_size=buffer_size)


class LambdaReaderFile(io.RawIOBase):
    """Creates a file abstraction from a single read(offset, size) function."""

    def __init__(self, rawRead: Callable[[int, int], bytes], size: int) -> None:
        """rawRead: Function which returns bytes for (offset, size) input."""
        io.RawIOBase.__init__(self)

        self.offset = 0
        self.rawRead = rawRead
        self.size = size

    def __enter__(self):
        return self

    def __exit__(self, exception_type, exception_value, exception_traceback):
        pass

    @overrides(io.RawIOBase)
    def seekable(self) -> bool:
        return True

    @overrides(io.RawIOBase)
    def readable(self) -> bool:
        return True

    @overrides(io.RawIOBase)
    def readinto(self, buffer):
        with memoryview(buffer) as view, view.cast("B") as byteView:  # type: ignore
            readBytes = self.read(len(byteView))
            byteView[: len(readBytes)] = readBytes
        return len(readBytes)

    @overrides(io.RawIOBase)
    def read(self, size: int = -1) -> bytes:
        result = self.rawRead(self.offset, min(self.size if size < 0 else size, max(0, self.size - self.offset)))
        self.offset += len(result)
        return result

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


class ZeroFile(LambdaReaderFile):
    """A file abstraction layer for a simple file containing only zero bytes."""

    def __init__(self, size: int) -> None:
        super().__init__((lambda offset, size: b"\x00" * size), size)

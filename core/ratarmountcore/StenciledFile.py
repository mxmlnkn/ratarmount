#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import bisect
import io

from typing import IO, List, Tuple

from .utils import overrides


class StenciledFile(io.BufferedIOBase):
    """A file abstraction layer giving a stenciled view to an underlying file."""

    def __init__(self, fileobj: IO, stencils: List[Tuple[int, int]]) -> None:
        """
        stencils: A list tuples specifying the offset and length of the underlying file to use.
                  The order of these tuples will be kept.
                  The offset must be non-negative and the size must be positive.

        Examples:
            stencil = [(5,7)]
                Makes a new 7B sized virtual file starting at offset 5 of fileobj.
            stencil = [(0,3),(5,3)]
                Make a new 6B sized virtual file containing bytes [0,1,2,5,6,7] of fileobj.
            stencil = [(0,3),(0,3)]
                Make a 6B size file containing the first 3B of fileobj twice concatenated together.
        """

        # fmt: off
        self.fileobj = fileobj
        self.offsets = [x[0] for x in stencils]
        self.sizes   = [x[1] for x in stencils]
        self.offset  = 0
        # fmt: on

        # Calculate cumulative sizes
        self.cumsizes = [0]
        for offset, size in stencils:
            assert offset >= 0
            assert size >= 0
            self.cumsizes.append(self.cumsizes[-1] + size)

        # Seek to the first stencil offset in the underlying file so that "read" will work out-of-the-box
        self.seek(0)

    def _findStencil(self, offset: int) -> int:
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

    @overrides(io.BufferedIOBase)
    def close(self) -> None:
        # Don't close the object given to us
        # self.fileobj.close()
        pass

    @overrides(io.BufferedIOBase)
    def fileno(self) -> int:
        # This is a virtual Python level file object and therefore does not have a valid OS file descriptor!
        raise io.UnsupportedOperation()

    @overrides(io.BufferedIOBase)
    def seekable(self) -> bool:
        return self.fileobj.seekable()

    @overrides(io.BufferedIOBase)
    def readable(self) -> bool:
        return self.fileobj.readable()

    @overrides(io.BufferedIOBase)
    def writable(self) -> bool:
        return False

    @overrides(io.BufferedIOBase)
    def read(self, size: int = -1) -> bytes:
        if size == -1:
            size = self.cumsizes[-1] - self.offset

        # This loop works in a kind of leapfrog fashion. On each even loop iteration it seeks to the next stencil
        # and on each odd iteration it reads the data and increments the offset inside the stencil!
        result = b''
        i = self._findStencil(self.offset)
        if i >= len(self.sizes):
            return result

        offsetInsideStencil = self.offset - self.cumsizes[i]
        assert offsetInsideStencil >= 0
        assert offsetInsideStencil < self.sizes[i]
        self.fileobj.seek(self.offsets[i] + offsetInsideStencil, io.SEEK_SET)

        while size > 0 and i < len(self.sizes):
            # Read as much as requested or as much as the current contiguous region / stencil still contains
            readableSize = min(size, self.sizes[i] - (self.offset - self.cumsizes[i]))
            if readableSize == 0:
                # Go to next stencil
                i += 1
                if i >= len(self.offsets):
                    break
                self.fileobj.seek(self.offsets[i])
            else:
                # Actually read data
                tmp = self.fileobj.read(readableSize)
                self.offset += len(tmp)
                result += tmp
                size -= readableSize
                # Now, either size is 0 or readableSize will be 0 in the next iteration

        return result

    @overrides(io.BufferedIOBase)
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

    @overrides(io.BufferedIOBase)
    def tell(self) -> int:
        return self.offset

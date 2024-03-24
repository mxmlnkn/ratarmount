#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import math
import os
import pathlib
import platform
import sys
import types

from typing import Dict, Generic, Iterable, List, Optional, TypeVar, Union

import importlib

try:
    import importlib.metadata
except ImportError:
    import importlib_metadata

    imeta: types.ModuleType = importlib_metadata
else:
    imeta = importlib.metadata


class RatarmountError(Exception):
    """Base exception for ratarmount module."""


class IndexNotOpenError(RatarmountError):
    """Exception for operations executed on a closed index database."""


class InvalidIndexError(RatarmountError):
    """Exception for indexes being invalid, outdated, or created with different arguments."""


class MismatchingIndexError(RatarmountError):
    """Exception for indexes being created by a different backend."""


class CompressionError(RatarmountError):
    """Exception for trying to open files with unsupported compression or unavailable decompression module."""


def overrides(parentClass):
    """Simple decorator that checks that a method with the same name exists in the parent class"""

    def overrider(method):
        if platform.python_implementation() != 'PyPy':
            assert method.__name__ in dir(parentClass)
            assert callable(getattr(parentClass, method.__name__))
        return method

    return overrider


class _DummyContext:
    def __enter__(self):
        pass

    def __exit__(self, *args):
        pass


def ceilDiv(dividend, divisor):
    return -(dividend // -divisor)


KeyType = TypeVar('KeyType')
ValueType = TypeVar('ValueType')


class LRUCache(Generic[KeyType, ValueType]):
    """
    Provides a cache behaving like dictionary with a limited size, which automatically deletes
    least-recently-used keys-value pairs when the size limit has been reached.
    """

    # Does not inherit from dict to ensure that there aren't any methods not overwritten and causing inconsistencies.
    # E.g. copy would return a dicft instead of LRUCache and so on.
    def __init__(self, size: int = 10):
        self.size = size
        self.lastUsed: List[KeyType] = []
        self.data: Dict[KeyType, ValueType] = {}

    def _refresh(self, key: KeyType):
        if key in self.lastUsed:
            self.lastUsed.remove(key)
        self.lastUsed.append(key)

    def __getitem__(self, key: KeyType):
        self._refresh(key)
        return self.data.__getitem__(key)

    def get(self, key: KeyType, default: Optional[ValueType] = None):
        if key in self.lastUsed:
            self.lastUsed.remove(key)
            self.lastUsed.append(key)
        return self.data.get(key, default)

    def __setitem__(self, key: KeyType, value: ValueType):
        self.data.__setitem__(key, value)

        self._refresh(key)
        while self.data.__len__() > self.size:
            self.data.__delitem__(self.lastUsed.pop(0))

    def __delitem__(self, key: KeyType):
        self.data.__delitem__(key)
        if key in self.lastUsed:
            self.lastUsed.remove(key)

    def __contains__(self, key: KeyType):
        return self.data.__contains__(key)

    def __len__(self):
        return self.data.__len__()

    def __repr__(self):
        return self.data.__repr__()

    def __str__(self):
        return self.data.__str__()

    def clear(self):
        self.data.clear()
        self.lastUsed.clear()

    def keys(self):
        return self.data.keys()

    def values(self):
        return self.data.values()

    def setdefault(self, key: KeyType, default: ValueType):
        self._refresh(key)
        return self.data.setdefault(key, default)

    def __bool__(self):
        return bool(self.data)


class Prefetcher:
    def __init__(self, memorySize):
        self.lastFetched = []
        self.memorySize = memorySize

    def fetch(self, value):
        if value in self.lastFetched:
            self.lastFetched.remove(value)
        self.lastFetched.append(value)
        while len(self.lastFetched) > self.memorySize:
            self.lastFetched.pop(0)

    def prefetch(self, maximumToPrefetch) -> Iterable:
        if not self.lastFetched or maximumToPrefetch <= 0:
            return []

        consecutiveCount = 0
        values = self.lastFetched[::-1]
        for i, j in zip(values[0:-1], values[1:]):
            if i == j + 1:
                consecutiveCount += 1
            else:
                break

        # I want an exponential progression like: logStep**consecutiveCount with the boundary conditions:
        # logStep**0 = 1 (mathematically true for any logStep because consecutiveCount was chosen to fit)
        # logStep**maxConsecutiveCount = maximumToPrefetch
        #   => logStep = exp(ln(maximumToPrefetch)/maxConsecutiveCount)
        #   => logStep**consecutiveCount = exp(ln(maximumToPrefetch) * consecutiveCount/maxConsecutiveCount)
        prefetchCount = int(round(math.exp(math.log(maximumToPrefetch) * consecutiveCount / (self.memorySize - 1))))
        return range(self.lastFetched[-1] + 1, self.lastFetched[-1] + 1 + prefetchCount)


ALPHA = ''.join(chr(ord('a') + i) for i in range(ord('z') - ord('a') + 1))
DIGITS = ''.join(chr(ord('0') + i) for i in range(ord('9') - ord('0') + 1))
HEX = DIGITS + ALPHA[:6]


def isLatinAlpha(text: str):
    return text and all(ord('a') <= ord(c) <= ord('z') for c in text)


def isLatinDigit(text: str):
    return text and all(ord('0') <= ord(c) <= ord('9') for c in text)


def isLatinHexAlpha(text: str):
    return text and all(ord('0') <= ord(c) <= ord('9') or ord('a') <= ord(c) <= ord('f') for c in text)


def formatNumber(i: int, base: str, length: int = 0):
    if len(base) <= 1:
        raise ValueError("Base alphabet must contain more than one letter!")

    result = ''
    while i > 0 or length > 0 or not result:
        result += base[i % len(base)]
        i = i // len(base)
        length = length - 1
    return result[::-1]


def distributionContainsFile(distribution, path: str) -> bool:
    if not distribution.files:
        return False

    for file in distribution.files:
        if not path.endswith(str(file)):
            continue

        try:
            pathlib.Path(path).relative_to(file.locate())
            return True
        except ValueError:
            return False
    return False


def getModule(module: Union[str, types.ModuleType]) -> Optional[types.ModuleType]:
    if isinstance(module, types.ModuleType):
        return module

    if module not in sys.modules:
        try:
            importlib.import_module(module)
        except ImportError:
            pass
    return sys.modules[module] if module in sys.modules else None


def findModuleVersion(moduleOrName: Union[str, types.ModuleType]) -> Optional[str]:
    module = getModule(moduleOrName)
    if not module:
        return None

    # zipfile has no __version__ attribute and PEP 396 ensuring that was rejected 2021-04-14
    # in favor of 'version' from importlib.metadata which does not even work with zipfile.
    # Probably, because zipfile is a built-in module whose version would be the Python version.
    # https://www.python.org/dev/peps/pep-0396/
    # The "python-xz" project is imported as an "xz" module, which complicates things because
    # there is no generic way to get the "python-xz" name from the "xz" runtime module object
    # and importlib.metadata.version will require "python-xz" as argument.
    # Note that even when querying the version with importlib.metadata.version, it can return
    # a different version than the actually imported module if some import tricks were done
    # like manipulating sys.path to import a non-installed module.
    # All in all, this really gets on my nerves and I wished that PEP 396 would have been accepted.
    # Currently, it feels like work has been shifted from the maintainer side to the user side.
    # See below the kinds of handstands we have to do to even just get the unreliable package
    # name from the module name in order to query the version. It's mental.
    # And note that importlib.metadata has only been introduced in Python 3.8 and has been
    # provisional until including Python 3.9, meaning it can still take years to be available
    # and stable on all systems or I have to add yet another dependency just to get the damn version.
    if hasattr(module, '__version__'):
        return str(getattr(module, '__version__'))

    if hasattr(module, '__file__'):
        moduleFilePath = getattr(module, '__file__')
        for distribution in imeta.distributions():
            try:
                if distributionContainsFile(distribution, moduleFilePath) and 'Version' in distribution.metadata:
                    return distribution.metadata['Version']
            except Exception:
                pass

    return None


def isOnSlowDrive(filePath: str):
    # TODO determine whether the whole file or most of it has been cached:
    #      https://serverfault.com/questions/278454/is-it-possible-to-list-the-files-that-are-cached
    #      https://github.com/mxmlnkn/rapidgzip/issues/13#issuecomment-1592856413
    # TODO make it work on Windows: https://devblogs.microsoft.com/oldnewthing/20201023-00/?p=104395
    try:
        device = os.stat(filePath).st_dev
        with open(f"/sys/dev/block/{os.major(device)}:{os.minor(device)}/queue/rotational", 'rb') as file:
            if file.read().strip() == b"1":
                return True
    except Exception:
        pass
    return False

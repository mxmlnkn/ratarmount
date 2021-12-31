#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import math
from typing import Dict, Generic, Iterable, List, Optional, TypeVar


class RatarmountError(Exception):
    """Base exception for ratarmount module."""


class IndexNotOpenError(RatarmountError):
    """Exception for operations executed on a closed index database."""


class InvalidIndexError(RatarmountError):
    """Exception for indexes being invalid, outdated, or created with different arguments."""


class CompressionError(RatarmountError):
    """Exception for trying to open files with unsupported compression or unavailable decompression module."""


def overrides(parentClass):
    """Simple decorator that checks that a method with the same name exists in the parent class"""

    def overrider(method):
        assert method.__name__ in dir(parentClass)
        assert callable(getattr(parentClass, method.__name__))
        return method

    return overrider


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

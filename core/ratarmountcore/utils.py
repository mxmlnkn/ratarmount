#!/usr/bin/env python3
# -*- coding: utf-8 -*-


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

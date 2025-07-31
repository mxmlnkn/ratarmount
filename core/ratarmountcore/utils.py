import base64
import collections
import contextlib
import importlib
import io
import math
import os
import pathlib
import platform
import sys
import types
from collections.abc import Iterable
from pathlib import Path
from typing import Generic, Optional, TypeVar, Union, get_type_hints


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
    # I tried typing.override (Python 3.12+), but support for it does not seem to be ideal (yet)
    # and portability also is an issue. https://github.com/google/pytype/issues/1915 Maybe in 3 years.

    def overrider(method):
        if platform.python_implementation() == 'PyPy':
            return method

        assert method.__name__ in dir(parentClass)
        parentMethod = getattr(parentClass, method.__name__)
        assert callable(parentMethod)

        if os.getenv('RATARMOUNT_CHECK_OVERRIDES', '').lower() not in ('1', 'yes', 'on', 'enable', 'enabled'):
            return method

        # Example return of get_type_hints:
        # {'path': <class 'str'>,
        #  'return': typing.Union[typing.Iterable[str],
        #                         typing.Dict[str, ratarmountcore.MountSource.FileInfo], NoneType]}
        parentTypes = get_type_hints(parentMethod)
        # If the parent is not typed, e.g., fusepy, then do not show errors for the typed derived class.
        for argument, argumentType in get_type_hints(method).items():
            if argument in parentTypes:
                parentType = parentTypes[argument]
                assert argumentType == parentType, f"{method.__name__}: {argument}: {argumentType} != {parentType}"

        return method

    return overrider


def ceil_div(dividend, divisor):
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
        self.lastUsed: list[KeyType] = []
        self.data: dict[KeyType, ValueType] = {}

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
        prefetchCount = round(math.exp(math.log(maximumToPrefetch) * consecutiveCount / (self.memorySize - 1)))
        return range(self.lastFetched[-1] + 1, self.lastFetched[-1] + 1 + prefetchCount)


ALPHA = ''.join(chr(ord('a') + i) for i in range(ord('z') - ord('a') + 1))
DIGITS = ''.join(chr(ord('0') + i) for i in range(ord('9') - ord('0') + 1))
HEX = DIGITS + ALPHA[:6]


def is_latin_alpha(text: str):
    return text and all(ord('a') <= ord(c) <= ord('z') for c in text)


def is_latin_digit(text: str):
    return text and all(ord('0') <= ord(c) <= ord('9') for c in text)


def is_latin_hex_alpha(text: str):
    return text and all(ord('0') <= ord(c) <= ord('9') or ord('a') <= ord(c) <= ord('f') for c in text)


def format_number(i: int, base: str, length: int = 0):
    if len(base) <= 1:
        raise ValueError("Base alphabet must contain more than one letter!")

    result = ''
    while i > 0 or length > 0 or not result:
        result += base[i % len(base)]
        i = i // len(base)
        length = length - 1
    return result[::-1]


def distribution_contains_file(distribution, path: str) -> bool:
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


def get_module(module: Union[str, types.ModuleType]) -> Optional[types.ModuleType]:
    if isinstance(module, types.ModuleType):
        return module

    if module not in sys.modules:
        with contextlib.suppress(ImportError):
            importlib.import_module(module)
    return sys.modules.get(module, None)


def find_module_version(moduleOrName: Union[str, types.ModuleType]) -> Optional[str]:
    module = get_module(moduleOrName)
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
    version = getattr(module, '__version__', None)
    if version:
        return str(version)

    moduleFilePath = getattr(module, '__file__', None)
    if moduleFilePath:
        import importlib.metadata as imeta  # noqa: E402  # Based on -X importtime seems to be pretty slow.

        for distribution in imeta.distributions():
            try:
                if distribution_contains_file(distribution, moduleFilePath) and 'Version' in distribution.metadata:
                    return distribution.metadata['Version']
            except Exception:
                pass

    return None


def is_on_slow_drive(filePath: str):
    # TODO determine whether the whole file or most of it has been cached:
    #      https://serverfault.com/questions/278454/is-it-possible-to-list-the-files-that-are-cached
    #      https://github.com/mxmlnkn/rapidgzip/issues/13#issuecomment-1592856413
    # TODO make it work on Windows: https://devblogs.microsoft.com/oldnewthing/20201023-00/?p=104395
    try:
        device = os.stat(filePath).st_dev
        if Path(f"/sys/dev/block/{os.major(device)}:{os.minor(device)}/queue/rotational").read_bytes().strip() == b"1":
            return True
    except Exception:
        pass
    return False


class FixedRawIOBase(io.RawIOBase):
    @overrides(io.RawIOBase)
    def readall(self) -> bytes:
        # It is necessary to implement this, or else the io.RawIOBase.readall implementation would use
        # io.DEFAULT_BUFFER_SIZE (8 KiB). Notably, this would ignore the block size configured in BufferedReader,
        # when calling read(-1) on it because it thinks that raw.readall is a fast-path, but in this case is ~100x
        # slower than 4 MiB reads equal to the Lustre-advertised block size.
        # https://github.com/python/cpython/issues/85624
        chunks = []
        while result := self.read():
            chunks.append(result)
        return b"".join(chunks)


def get_xdg_cache_home():
    # https://specifications.freedesktop.org/basedir-spec/basedir-spec-latest.html
    # > $XDG_CACHE_HOME defines the base directory relative to which user-specific non-essential data files should
    # > be stored. If $XDG_CACHE_HOME is either not set or empty, a default equal to $HOME/.cache should be used.
    path = os.environ.get('XDG_CACHE_HOME', '')
    home = os.path.expanduser("~/")
    if path.startswith(home):
        path = "~/" + path[len(home) :]
    return path or os.path.join('~', '.cache')


def decode_unpadded_base64(data: str) -> bytes:
    return base64.b64decode(data + '=' * ((4 - len(data) % 4) % 4))


def remove_duplicates_stable(iterable: Iterable):
    seen = set()
    deduplicated = []
    for x in iterable:
        if x not in seen:
            deduplicated.append(x)
            seen.add(x)
    return deduplicated


def determine_recursion_depth(recursive: bool = False, recursionDepth: Optional[int] = None, **_) -> int:
    """
    'recursionDepth' has higher precedence than 'recursive' if specified.
    Can be called directly i.e. f(a,b) or with kwargs f(**options) for which unknown keys will be ignored.
    Returns a real recursion depth. -1 or any negative has no special meaning, but it should effectively
    result in no recursion.
    """
    return (sys.maxsize if recursive else 0) if recursionDepth is None else recursionDepth


def compute_entropy(data: bytes) -> float:
    if not data:
        return 0.0

    # https://en.wikipedia.org/wiki/Cross-entropy
    # https://docs.scipy.org/doc/scipy/reference/generated/scipy.stats.entropy.html
    # > The cross entropy can be calculated as the sum of the entropy and relative entropy.
    # The entropy we compute is basically the number of bits required to encode the message optimally,
    # e.g., with Huffman Coding or, better, with asymmetric numeral systems.
    # I.e., for random data we except the result to be close to 8, e.g. cut off > 7.5, which would correspond
    # to a compression ratio of 0.9375.
    # Note that we would need at least 256 values, or else 256 - N values will be 0 and will therefore
    # reduce entropy! More generically, t should be a multiple of 256 to get more accurate entropy estimations!
    # Tests with SQLAR encrypted data:
    #   N = 10   -> 3.32
    #   N = 100  -> 6.38
    #   N = 384  -> 7.49
    #   N = 1000 -> 7.81
    #
    #   N = 256  -> 7.24
    #   N = 512  -> 7.61
    #   N = 1024 -> 7.81
    #   N = 2048 -> 7.916
    #   N = 4096 -> 7.950
    #   N = 8192 -> 7.977
    # Note that the information entropy is completely ordering-independent!
    # I.e., a sequence like \x00\x01\x02...\xFF will have entropy like random data even though it clearly is not!
    # The first idea coming to mind to encode position, would to take the difference between neighboring bytes.
    # This would still result in the same value range but we could test for sequences, and all linearly related
    # sequences such as \x00\x02\x04..\xFC\xFE.
    # It would even find repeated words because any repeated word would also repeat the same differences between
    # consecutive letters.
    # I really have trouble constructing data that this method wouldn't catch. I guess quadratic sequences like
    # \x01\x02\x04\x08\... could work with overflow but at that point we would go into the direction of pseudorandom
    # generators like (quadratic) linear congruential generators.
    probabilities = [byteFrequency / len(data) for byteFrequency in collections.Counter(data).values()]
    return -sum(p * math.log2(p) for p in probabilities)


def is_random(data: bytes) -> bool:
    if len(data) <= 1:
        return False

    # See benchmarks/scripts/testEntropy to heuristically find a lower bound for entropy of random-distributed data.
    # The longer data is the closer it will get to 8 bits entropy. The difference goes to 0 proportional to 1/length.
    # Larger m make randomness detection more lenient. m=16 follows approximately the median when repeating with
    # different random data, so m should at least be >16.
    # We could do any number of tests for randomness: https://en.wikipedia.org/wiki/Statistical_randomness
    # Some of them might even fail for encrypted data if the encryption is assumedly not strong enough or adds magic
    # bytes at periodic locations.
    # Does not work very well for small number of bytes! Should at least give 1024. A multiple of 256 is preferable.
    m = 40

    def convergence_rate(n: int):
        return 8 * (1 + m) / (n + m)

    def is_in_threshold(dataToTest: bytes):
        return 8 - compute_entropy(dataToTest) < convergence_rate(len(dataToTest))

    diffData = bytes((data[i + 1] - data[i] + 256) % 256 for i in range(len(data) - 1))
    return is_in_threshold(data) and is_in_threshold(diffData)

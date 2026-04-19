import hashlib
import io
import logging
import zlib
from dataclasses import dataclass
from typing import IO, Any, Callable, Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class HashAlgorithm:
    sparse: bool


class _CRC32Hasher:
    def __init__(self) -> None:
        self._value = 0

    def update(self, data: bytes) -> None:
        self._value = zlib.crc32(data, self._value)

    def hexdigest(self) -> str:
        return f"{self._value & 0xFFFFFFFF:08x}"


def _build_hash_registry() -> dict[str, HashAlgorithm]:
    # shake_* require an explicit output length for digest() / hexdigest() and therefore do not fit this interface.
    hashlib_names = {name for name in hashlib.algorithms_available if name and not name.startswith('shake')}
    registry: dict[str, HashAlgorithm] = {
        # Replace sha3_512 to sha3-512 for readability in the --hashes CLI option and the user.hash.sha3-512 key.
        name.replace('_', '-'): HashAlgorithm(sparse=False)
        for name in sorted(hashlib_names)
    }
    registry['crc32'] = HashAlgorithm(sparse=False)
    registry['smplayer'] = HashAlgorithm(sparse=True)
    return registry


HASH_REGISTRY = _build_hash_registry()


def _zero_pad(data: bytes, size: int) -> bytes:
    return data[:size] + b'\0' * max(0, size - len(data))


def _smplayer_hash_accumulate(data: bytes) -> int:
    result = 0
    for i in range(0, len(data), 8):
        result += int.from_bytes(_zero_pad(data[i : i + 8], 8), 'little', signed=False)
    return result


def smplayer_hash(fileObject: IO[bytes]) -> str:
    """
    https://github.com/smplayer-dev/smplayer/blob/master/src/filehash.cpp
    smplayer adds 64-bit numbers but that is the same as adding Python arbitrary
    precision numbers and then masking it to 64-bit at the end.
    All higher bits are simply ignored.
    """
    chunk_size = 64 * 1024

    fileObject.seek(0)
    result = _smplayer_hash_accumulate(fileObject.read(chunk_size))

    fileObject.seek(0, io.SEEK_END)
    file_size = fileObject.tell()
    result += file_size

    # For files < chunk size, this will effectively add 0.
    # For files >= chunk_size and < 2 * chunk_size, this will effectively sum up all QWORDS in the file
    # without summing up the middle QWORDS twice. These edge cases are not really handled in smplayer.
    fileObject.seek(min(max(chunk_size, file_size - chunk_size), file_size))
    result += _smplayer_hash_accumulate(fileObject.read(chunk_size))

    return f"{result & 0xFFFF_FFFF_FFFF_FFFF:016x}"


def compute_hashes(
    fileObject: IO[bytes],
    fileSize: int,
    algorithms: list[str],
    progress_callback: Optional[Callable[[int], None]] = None,
) -> dict[str, str]:
    if not algorithms:
        return {}

    hashers: dict[str, Any] = {}
    sparseAlgorithms: list[str] = []
    for algorithm in algorithms:
        if not algorithm:
            continue
        specification = HASH_REGISTRY.get(algorithm, None)
        if specification is None:
            logger.warning("Unsupported hash algorithm: %s", algorithm)
            continue

        if specification.sparse:
            sparseAlgorithms.append(algorithm)
        elif algorithm == 'crc32':
            hashers[algorithm] = _CRC32Hasher()
        else:
            try:
                # hashlib.new seems to be stable against _ and - substitutions and probably also case.
                hashers[algorithm] = hashlib.new(algorithm)
            except ValueError:
                logger.warning("Unsupported hash: %s", algorithm, exc_info=logger.isEnabledFor(logging.DEBUG))

    # Read and hash in chunks of 1 MiB.
    if hashers:
        readBytes = 0
        fileObject.seek(0)
        while chunk := fileObject.read(1024 * 1024):
            if progress_callback is not None:
                readBytes += len(chunk)
                progress_callback(readBytes)
            for hasher in hashers.values():
                hasher.update(chunk)

    result: dict[str, str] = {}
    for algorithm, hasher in hashers.items():
        result[algorithm] = hasher.hexdigest()

    for algorithm in sparseAlgorithms:
        if algorithm == 'smplayer':
            result[algorithm] = smplayer_hash(fileObject)

    return result

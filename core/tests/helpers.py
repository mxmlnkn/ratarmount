import contextlib
import os
import shutil
import tempfile

from ratarmountcore.compressions import COMPRESSION_FORMATS, find_available_backend


def find_test_file(relativePathOrName):
    for i in range(3):
        path = os.path.sep.join([".."] * i + ["tests", relativePathOrName])
        if os.path.exists(path):
            return path
    return relativePathOrName


@contextlib.contextmanager
def copy_test_file(relativePathOrName):
    with tempfile.TemporaryDirectory() as folder:
        path = os.path.join(folder, os.path.basename(relativePathOrName))
        if os.path.isfile(find_test_file(relativePathOrName)):
            shutil.copy(find_test_file(relativePathOrName), path)
        else:
            for format_id, format_info in COMPRESSION_FORMATS.items():
                backend = find_available_backend(format_id)
                if not backend:
                    continue

                for extension in format_info.extensions:
                    found_path = find_test_file(relativePathOrName + '.' + extension)
                    if not os.path.isfile(found_path):
                        continue

                    with (
                        open(found_path, 'rb') as file,
                        contextlib.suppress(Exception),
                        backend.open(file) as decompressed,
                        open(path, 'wb') as target,
                    ):
                        shutil.copyfileobj(decompressed, target)
                        break

        yield path


@contextlib.contextmanager
def change_working_directory(path):
    old_path = os.getcwd()
    os.chdir(path)
    try:
        yield
    finally:
        os.chdir(old_path)

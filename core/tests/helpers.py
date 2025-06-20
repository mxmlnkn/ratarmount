import contextlib
import os
import shutil
import tempfile


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
        shutil.copy(find_test_file(relativePathOrName), path)
        yield path

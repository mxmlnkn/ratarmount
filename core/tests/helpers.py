import contextlib
import os
import tempfile


def findTestFile(relativePathOrName):
    for i in range(3):
        path = os.path.sep.join([".."] * i + ["tests", relativePathOrName])
        if os.path.exists(path):
            return path
    return relativePathOrName


@contextlib.contextmanager
def copyTestFile(relativePathOrName):
    with tempfile.TemporaryDirectory() as folder:
        path = os.path.join(folder, os.path.basename(relativePathOrName))
        with open(findTestFile(relativePathOrName), 'rb') as file, open(path, 'wb') as target:
            target.write(file.read())
        yield path

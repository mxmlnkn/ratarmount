#!/usr/bin/env python3
"""
Test script to verify the in-memory overlay functionality works correctly.
"""

import os
import shutil

# Add the ratarmount package to the path
import sys
import tarfile
import tempfile
from pathlib import Path

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from ratarmountcore.mountsource.factory import create_mount_source
from ratarmountcore.mountsource.formats.tar import SQLiteIndexedTar

from ratarmount.WriteOverlay import WritableFolderMountSource


def test_memory_overlay_basic():
    """Test basic functionality of in-memory overlay"""
    print("Testing basic in-memory overlay functionality...")

    # Create a simple test tar file
    test_tar_content = {'file1.txt': 'Hello World', 'file2.txt': 'Another file', 'subdir/file3.txt': 'Nested file'}

    # Create a temporary tar file
    with tempfile.NamedTemporaryFile(suffix='.tar', delete=False) as tmp_tar:
        tar_path = tmp_tar.name

    try:
        # Create test tar
        with tarfile.open(tar_path, 'w') as tar:
            for filepath, content in test_tar_content.items():
                info = tarfile.TarInfo(name=filepath)
                info.size = len(content)
                tar.addfile(info, fileobj=open('/dev/null', 'rb'))
                tar.writestr(filepath, content)

        # Create mount source
        mount_source = create_mount_source(tar_path)

        # Test creating in-memory overlay
        overlay = WritableFolderMountSource(path=":memory:", mountSource=mount_source, memory_overlay=True)

        print("✓ In-memory overlay created successfully")

        # Test basic file operations
        # Create a new file in memory
        fd = overlay.create('/newfile.txt', 0o644)
        overlay.write('/newfile.txt', b'Hello from memory!', 0, fd)
        overlay.flush('/newfile.txt', fd)
        overlay.close('/newfile.txt', fd)

        print("✓ File creation and writing to memory overlay works")

        # Test that we can read it back
        fd = overlay.open('/newfile.txt', os.O_RDONLY)
        data = overlay.read('/newfile.txt', 1024, 0, fd)
        overlay.close('/newfile.txt', fd)

        assert data == b'Hello from memory!'
        print("✓ File reading from memory overlay works")

        # Test that existing files can be accessed
        if overlay.mountSource.exists('/file1.txt'):
            fd = overlay.open('/file1.txt', os.O_RDONLY)
            data = overlay.read('/file1.txt', 1024, 0, fd)
            overlay.close('/file1.txt', fd)
            print("✓ Existing file access works")

        print("All basic tests passed!")
        return True

    except Exception as e:
        print(f"✗ Test failed with error: {e}")
        import traceback

        traceback.print_exc()
        return False
    finally:
        # Cleanup
        if os.path.exists(tar_path):
            os.unlink(tar_path)


def test_memory_overlay_with_real_tar():
    """Test in-memory overlay with a real tar file"""
    print("\nTesting in-memory overlay with real tar file...")

    # Create a simple test tar file
    test_tar_content = {
        'hello.txt': 'Hello World!',
        'test.txt': 'Some test content',
    }

    # Create a temporary tar file
    with tempfile.NamedTemporaryFile(suffix='.tar', delete=False) as tmp_tar:
        tar_path = tmp_tar.name

    try:
        # Create test tar
        with tarfile.open(tar_path, 'w') as tar:
            for filepath, content in test_tar_content.items():
                info = tarfile.TarInfo(name=filepath)
                info.size = len(content)
                tar.addfile(info, fileobj=open('/dev/null', 'rb'))
                tar.writestr(filepath, content)

        # Create mount source
        mount_source = create_mount_source(tar_path)

        # Test creating in-memory overlay
        overlay = WritableFolderMountSource(path=":memory:", mountSource=mount_source, memory_overlay=True)

        print("✓ Real tar in-memory overlay created successfully")

        # Test file operations
        # Create a new file
        fd = overlay.create('/created.txt', 0o644)
        overlay.write('/created.txt', b'This is a test file', 0, fd)
        overlay.flush('/created.txt', fd)
        overlay.close('/created.txt', fd)

        # Read it back
        fd = overlay.open('/created.txt', os.O_RDONLY)
        data = overlay.read('/created.txt', 1024, 0, fd)
        overlay.close('/created.txt', fd)

        assert data == b'This is a test file'
        print("✓ File creation and reading works with real tar")

        # Test modifying existing file
        fd = overlay.open('/hello.txt', os.O_RDWR)
        overlay.write('/hello.txt', b'Modified content', 0, fd)
        overlay.flush('/hello.txt', fd)
        overlay.close('/hello.txt', fd)

        print("✓ File modification works")

        print("Real tar test passed!")
        return True

    except Exception as e:
        print(f"✗ Real tar test failed with error: {e}")
        import traceback

        traceback.print_exc()
        return False
    finally:
        # Cleanup
        if os.path.exists(tar_path):
            os.unlink(tar_path)


if __name__ == "__main__":
    print("Running in-memory overlay tests...")

    success1 = test_memory_overlay_basic()
    success2 = test_memory_overlay_with_real_tar()

    if success1 and success2:
        print("\n🎉 All tests passed!")
        exit(0)
    else:
        print("\n❌ Some tests failed!")
        exit(1)

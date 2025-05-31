#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# This file is supposed to provide the 'fuse' symbol.
# pylint: disable=unused-import

import sys
import traceback

try:
    import mfusepy as fuse  # type: ignore
except AttributeError as importException:
    traceback.print_exc()
    print("[Error] Some internal exception occurred while trying to load mfusepy:", importException)
    sys.exit(1)
except (ImportError, OSError) as importException:
    print("[Warning] Failed to load mfusepy. Will try to load system fusepy. Exception was:", importException)
    try:
        import fuse  # type: ignore
    except (ImportError, OSError) as fuseException:
        try:
            import fusepy as fuse  # type: ignore
        except ImportError as fusepyException:
            print("[Error] Did not find any FUSE installation. Please install it, e.g., with:")
            print("[Error]  - apt install libfuse2")
            print("[Error]  - yum install fuse fuse-libs")
            print("[Error] Exception for fuse:", fuseException)
            print("[Error] Exception for fusepy:", fusepyException)
            sys.exit(1)

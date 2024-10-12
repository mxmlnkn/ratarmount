#!/usr/bin/env python3

import math
import socket
import sys
import time
from contextlib import closing

timeout = int(sys.argv[2]) if len(sys.argv) > 2 else 60

t0 = time.time()
with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as sock:
    n = max(1, int(math.ceil(timeout / 5)))
    for i in range(n):
        try:
            if sock.connect_ex(("127.0.0.1", int(sys.argv[1]))) == 0:
                print(f"Weed port opened after {time.time() - t0:.1f} s.")
                sys.exit(0)
        except Exception:
            pass
        # Do not wait after the last test.
        if i < n - 1:
            time.sleep(5)
sys.exit(1)

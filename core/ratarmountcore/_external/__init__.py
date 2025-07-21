import os
import sys

_current_path = os.path.abspath(os.path.dirname(__file__))
if _current_path not in sys.path:
    sys.path.insert(0, _current_path)

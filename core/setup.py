#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import site
import sys
from setuptools import setup

# workaround https://github.com/pypa/pip/issues/7953
site.ENABLE_USER_SITE = "--user" in sys.argv[1:]
# Workaround for https://github.com/pypa/setuptools/issues/2938
#   Did not work in earlier pip versions: python3 -m pip install --user --editable .
#   Use: python3 setup.py develop --user .
# Workaround for https://github.com/pypa/setuptools/issues/3582#issuecomment-1242384337
#   python3 -m pip install --user --no-build-isolation --editable .
# https://github.com/pypa/pip/issues/6264#issuecomment-1184327415
# https://github.com/pypa/pip/pull/11466
# Upgrade to pip 22.3.1 or higher to avoid having to specify --no-build-isolation
setup()

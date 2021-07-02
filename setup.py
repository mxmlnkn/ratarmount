#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
from setuptools import setup

scriptPath = os.path.abspath( os.path.dirname( __file__ ) )
with open( os.path.join( scriptPath, 'README.md' ), encoding = 'utf-8' ) as file:
    readmeContents = file.read()

with open( 'requirements.txt', 'rt' ) as file:
    requirements = file.read().splitlines()

setup(
    name             = 'ratarmount',
    version          = '0.8.1',

    description      = 'Random Access Read-Only Tar Mount',
    url              = 'https://github.com/mxmlnkn/ratarmount',
    author           = 'Maximilian Knespel',
    author_email     = 'mxmlnkn@github.de',
    license          = 'MIT',
    classifiers      = [ 'License :: OSI Approved :: MIT License',
                         'Development Status :: 3 - Alpha',
                         'Operating System :: POSIX',
                         'Operating System :: Unix',
                         'Programming Language :: Python :: 3',
                         'Topic :: System :: Archiving' ],

    long_description = readmeContents,
    long_description_content_type = 'text/markdown',

    py_modules       = [ 'ratarmount' ],
    install_requires = requirements + [ 'dataclasses; python_version < "3.7.0"' ],
    # Make these optional requirements because they have no binaries on PyPI meaning they are built from source
    # and will fail if system dependencies are not installed.
    extras_require   = {
                            'full' : [ 'cffi', 'lzmaffi' ],
                            # cffi dependency seems to be configured wrong in lzmaffi,
                            # therefore also list it here before lzmaffi:
                            # https://github.com/r3m0t/backports.lzma/issues/3
                            'xz' : [ 'cffi', 'lzmaffi' ],
                       },
    entry_points = { 'console_scripts': [ 'ratarmount=ratarmount:cli' ] }
)

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import sys
from setuptools import setup

scriptPath = os.path.abspath( os.path.dirname( __file__ ) )
with open( os.path.join( scriptPath, 'README.md' ), encoding = 'utf-8' ) as file:
    readmeContents = file.read()

setup(
    name             = 'ratarmount',
    version          = '0.6.1',

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
    install_requires = [ 'fusepy', 'indexed_gzip', 'indexed_bzip2>=1.1.2' ],
    extras_require   = {
                            'legacy-serializers' : [
                                'lz4',
                                'msgpack',
                                'simplejson',
                                'pyyaml',
                                'ujson',
                                'cbor',
                                'python-rapidjson'
                            ]
                       },
    entry_points = { 'console_scripts': [ 'ratarmount=ratarmount:cli' ] }
)

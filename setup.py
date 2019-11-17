#!/usr/bin/env python3
# -*- coding: utf-8 -*-

from setuptools import setup

setup(
    name             = 'ratarmount',
    version          = '0.2.0',

    description      = 'Random Access Read-Only Tar Mount',
    url              = 'https://github.com/mxmlnkn',
    author           = 'Maximilian Knespel',
    author_email     = 'mxmlnkn@github.de',
    license          = 'MIT',
    classifiers      = [ 'License :: OSI Approved :: MIT License',
                         'Development Status :: 3 - Alpha',
                         'Operating System :: POSIX',
                         'Operating System :: Unix',
                         'Programming Language :: Python :: 3',
                         'Topic :: System :: Archiving' ],

    py_modules       = [ 'ratarmount' ],
    install_requires = [ 'fusepy',
                         'lz4',
                         'msgpack',
                         'simplejson',
                         'pyyaml',
                         'ujson',
                         'cbor',
                         'python-rapidjson' ],
    entry_points = { 'console_scripts': [ 'ratarmount=ratarmount:cli' ] }
)

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import sys
from setuptools import setup
from setuptools.extension import Extension

buildCython = '--cython' in sys.argv

extensions = [
    Extension(
        'bzip2',
        [ 'bzip2.pyx' if buildCython else 'bzip2.cpp' ],
        include_dirs       = [ '.' ],
        language           = 'c++',
        extra_compile_args = [ '-std=c++11', '-O3', '-DNDEBUG' ],
    ),
]

if buildCython:
    from Cython.Build import cythonize
    extensions = cythonize( extensions, compiler_directives = { 'language_level' : '3' } )
    del sys.argv[sys.argv.index( '--cython' )]

setup(
    name             = 'ratarmount',
    version          = '0.3.0',

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
    ext_modules      = extensions,
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

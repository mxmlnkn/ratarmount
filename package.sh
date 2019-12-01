#!/usr/bin/env bash

rm -rf build dist *.egg-info __pycache__

# generate bzip2.cpp from bzip2.pyx
python3 setup.py build_ext --inplace --cython
python3 setup.py sdist bdist_wheel

#twine upload dist/*

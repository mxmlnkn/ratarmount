#!/usr/bin/env bash

cd -- "$( dirname -- "${BASH_SOURCE[0]}" )/.." || { echo 'Failed to cd to git root!'; exit 1; }

source tests/common.sh


shellcheck -x tests/run*.sh AppImage/ratarmount-metadata/*.sh || returnError "$LINENO" 'shellcheck failed!'

yamlFiles=()
while read -r file; do
    yamlFiles+=( "$file" )
done < <( git ls-tree -r --name-only HEAD | 'grep' '[.]yml$' | 'grep' -v '/_external/' )
yamllint -c tests/.yamllint.yml "${yamlFiles[@]}" || returnError "$LINENO" 'yamllint failed!'

files=()
while read -r file; do
    files+=( "$file" )
done < <(
    git ls-tree -r --name-only HEAD |
        'grep' '[.]py$' |
        'grep' -v -F '__init__.py' |
        'grep' -v 'benchmarks/' |
        'grep' -v -F 'setup.py' |
        'grep' -v 'test.*.py' |
        'grep' -v '/_external/'
)

allTextFiles=()
while read -r file; do
    allTextFiles+=( "$file" )
done < <( git ls-tree -r --name-only HEAD | 'grep' -E '[.](py|md|txt|sh|yml)' | 'grep' -v '/_external/' )
codespell "${allTextFiles[@]}"

allPythonFiles=()
while read -r file; do
    allPythonFiles+=( "$file" )
done < <( git ls-tree -r --name-only HEAD | 'grep' '[.]py$' | 'grep' -v '/_external/' )

ruff check --fix --config tests/.ruff.toml -- "${allPythonFiles[@]}"
#ruff check --fix --unsafe-fixes --config tests/.ruff.toml -- "${allPythonFiles[@]}"
ruff check --config tests/.ruff.toml -- "${allPythonFiles[@]}" || returnError "$LINENO" 'Ruff check failed!'

testFiles=()
while read -r file; do
    testFiles+=( "$file" )
done < <(
    git ls-tree -r --name-only HEAD | 'grep' 'test.*[.]py$' | 'grep' -v 'conftest[.]py$' | 'grep' -v '/_external/'
)

# Parallelism with -j 3 does not improve much anymore and anything larger even worsens the runtime!
pylint --rcfile tests/.pylintrc ratarmount core/ratarmountcore "${testFiles[@]}" | tee pylint.log
if 'grep' -E -q ': E[0-9]{4}: ' pylint.log; then
    echoerr 'There were warnings during the pylint run!'
    exit 1
fi
rm pylint.log

# No parallelism yet: https://github.com/python/mypy/issues/933
mypy --config-file tests/.mypy.ini ratarmount core/ratarmountcore core/tests || returnError "$LINENO" 'Mypy failed!'

pytype -j auto -d import-error -P"$( cd core && pwd ):$( pwd )" --exclude=core/ratarmountcore/_external \
    ratarmount core/ratarmountcore core/tests || returnError "$LINENO" 'Pytype failed!'

black -q --line-length 120 --skip-string-normalization "${allPythonFiles[@]}"

filesToSpellCheck=()
while read -r file; do
    filesToSpellCheck+=( "$file" )
done < <( git ls-tree -r --name-only HEAD | 'grep' -E '[.](py|md|txt|sh|yml)' | 'grep' -v '/_external/' )
# fsspec uses cachable instead of cacheable ...
codespell "${filesToSpellCheck[@]}"

flake8 --config tests/.flake8 "${files[@]}" "${testFiles[@]}" || returnError "$LINENO" 'Flake8 failed!'

# Test runtimes 2024-04-04 on Ryzen 3900X. On the CI with nproc=4, the speedup is roughly 2x.
# Note that pytest-xdist doesn't scale arbitrarily because it seems to start up threads sequentially,
# which can take ~2s for 48 threads!
# core/tests/test_AutoMountLayer.py         in 19.05s   parallelize -> 5.64s
# core/tests/test_BlockParallelReaders.py   in 57.95s   parallelize -> 12.22s
# core/tests/test_LibarchiveMountSource.py  in 246.99s  parallelize -> 74.43s
# core/tests/test_RarMountSource.py         in 0.08s
# core/tests/test_SQLiteBlobFile.py         in 0.24s
# core/tests/test_SQLiteIndex.py            in 0.10s
# core/tests/test_SQLiteIndexedTar.py       in 154.08s  parallelize -> 63.95s
# core/tests/test_StenciledFile.py          in 1.91s
# core/tests/test_SubvolumesMountSource.py  in 0.12s
# core/tests/test_UnionMountSource.py       in 0.12s
# core/tests/test_ZipMountSource.py         in 0.09s
# core/tests/test_compressions.py           in 0.13s
# core/tests/test_factory.py                in 0.36s
# core/tests/test_utils.py                  in 0.22s
# tests/test_cli.py                         in 67.09s  parallelize -> n=8: 8.91s, n=24: 4.54s, n=48: 4.33s,
#                                                                     n=96: 6.52s

# Pytest has serious performance issues. It does collect all tests beforehand and does not free memory
# after tests have finished it seems. Or maybe that memory is a bug with indexed_gzip. But the problem is
# that all tests after that one outlier also run slower! Maybe because of a Python garbage collector bug?
# For that reason, run each test file separately.
for testFile in "${testFiles[@]}"; do
    case "$testFile" in
        "tests/test_cli.py")
            # First off, n=auto seems to use the physical cores and ignores virtual ones.
            # Secondly, these tests scale much better than the others because most time is spent waiting for
            # the FUSE mount point to appear or disappear, which doesn't seem to be bottlenecked by CPU usage.
            python3 -X dev -W ignore::DeprecationWarning -u \
                -c "import pytest, re, sys; sys.exit(pytest.console_main())" \
                -n 24 --disable-warnings "$testFile" || returnError "$LINENO" 'pytest failed!'
            ;;
        "core/tests/test_AutoMountLayer.py"\
        |"core/tests/test_BlockParallelReaders.py"\
        |"core/tests/test_LibarchiveMountSource.py"\
        |"core/tests/test_SQLiteIndexedTar.py")
            echo "$testFile"  # pytest-xdist seems to omit the test file name
            pytest -n auto --disable-warnings "$testFile" || returnError "$LINENO" 'pytest failed!'
            ;;
        *)
            if [[ "${testFile//test_//}" != "$testFile" ]]; then
                # Fusepy warns about usage of use_ns because the implicit behavior is deprecated.
                # But there has been no development to fusepy for 4 years, so I think it should be fine to ignore.
                pytest --disable-warnings "$testFile" || returnError "$LINENO" 'pytest failed!'
            fi
            ;;
    esac
done

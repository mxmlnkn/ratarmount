#!/usr/bin/env bash

# Will print warnings for each folder because find tries to descend into it after finding those.
# It may be confusing but it is a nice verbose output to list deleted folders.
find . -type d '(' \
    -name '*.egg-info' -or -name '*.mypy_cache' -or -name '__pycache__' -or -name '.ruff_cache' -or \
    -name '*.pytest_cache' -or -name '*.pytype' -or -name 'dist' -or -name 'build' -or \
    -name 'ratarmount.stdout' -or -name 'ratarmount.stderr' \
')' -exec rm -rf {} ';'
'rm' -f httpd-ruby-webrick.log ratarmount.stderr.log.tmp ratarmount.stdout.log.tmp

for service in httpd ipfs pyftpdlib wsgidav; do pkill -f "$service"; done

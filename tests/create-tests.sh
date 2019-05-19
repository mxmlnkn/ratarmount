#!/bin/bash

recreateArchive()
(
    archive="$( pwd )/$( basename -- $1 )"
    newArchive="${archive%.tar}.new.tar"
    shift

    cd -- "$( mktemp -d )"
    tar -x -f "$archive"

    # run TAR command
    set -x
    tar -c -f "$newArchive" --owner=user --group=group "$@"
)

recreateArchive 'single-file.tar' bar
recreateArchive 'single-file-with-leading-dot-slash.tar' ./bar
recreateArchive 'folder-with-leading-dot-slash.tar' ./
recreateArchive 'single-nested-file.tar' foo/fighter/ufo
recreateArchive 'single-nested-folder.tar' foo/fighter/

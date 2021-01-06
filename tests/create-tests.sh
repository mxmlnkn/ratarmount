#!/bin/bash

recreateArchive()
(
    archive="$( pwd )/$( basename -- "$1" )"
    newArchive="${archive%.tar}.new.tar"
    shift

    cd -- "$( mktemp -d )" || { echo 'Failed to create temporary directory!'; return 1; }
    tar -x -f "$archive"

    # run TAR command
    set -x
    tar -c -f "$newArchive" --owner=user --group=group "$@"
)

# These archives are mostly only different in what kind of TAR they produce but don't differ with their actual contents.
# So, for these test TARs the creation command is more important than their contents.
# That's why their creation commands are indirectly documented in this shell script to recreate the archives from
# their extracted contents.
# Archives simply created by, e.g., "tar cf contents{.tar,}" don't have to be listed here
recreateArchive 'single-file.tar' bar
recreateArchive 'single-file-with-leading-dot-slash.tar' ./bar
recreateArchive 'folder-with-leading-dot-slash.tar' ./
recreateArchive 'single-nested-file.tar' foo/fighter/ufo
recreateArchive 'single-nested-folder.tar' foo/fighter/
recreateArchive 'file-existing-as-non-link-and-link.tar' foo/fighter/ foo/fighter/ufo

echo foo > bar
tar -c --owner=user --group=group --numeric -f 'single-self-link.tar' bar bar
tar --delete --occurrence=1 --file 'single-self-link.tar' bar

tar -c --owner=user --group=group --numeric -f 'two-self-links.tar' bar bar bar
tar --delete --occurrence=1 --file 'two-self-links.tar' bar

cp 'single-file.tar' 'empty.tar'
tar --delete --file 'empty.tar' bar

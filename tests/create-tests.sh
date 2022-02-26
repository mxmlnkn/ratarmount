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

# Create super nested archive
mkdir super-nested-archive
(
    cd -- "$_" || exit 1
    tarFileName=updated-file-with-folder.tar
    cp "../$tarFileName" .

    bzip2 -k -- "$tarFileName"
    gzip  -k -- "$tarFileName"
    zstd  -k -- "$tarFileName"
    xz    -k -- "$tarFileName"

    tar -xf ../single-nested-folder.tar
    tar -xf ../single-file.tar

    7z a seven-elves.7z foo bar
    rar a foos-rar-dah.rar foo bar
    zip -r bag.zip foo bar
    mkisofs -lJR -o miso.iso foo bar

    mkdir files
    mv -- * files

    # Create archives of archives
    tar -cf  tar-with-archives.tar     -- files
    tar -cjf tar-with-archives.tar.bz2 -- files
    tar -czf tar-with-archives.tar.gz  -- files
    tar -cJf tar-with-archives.tar.xz  -- files

    7z a seven-elves.7z files
    rar a foos-rar-dah.rar files
    zip -r bag.zip files
    mkisofs -lJR -o miso.iso files

    cd ..
    tar -cjf super-nested-archive{.tar.bz2,}
)


tar -xf nested-tar.tar
rar a -hpfoo encrypted-headers-nested-tar.rar foo
rar a -pfoo encrypted-nested-tar.rar foo
zip -r --encrypt --password foo encrypted-nested-tar.zip foo


mkdir foo2
echo iriya > foo2/ufo
rar a file-in-non-existing-folder.rar foo2/ufo
zip file-in-non-existing-folder.zip foo2/ufo


echo fighter > foo
echo iriya > ufo
rar a natsu.rar foo ufo
zip rar.zip natsu.rar

zip natsu.zip foo ufo
rar a zip.rar natsu.rar


rm foo
tar -xf nested-tar.tar
( cd foo/fighter && ln -s ufo saucer; )
zip -r --symlinks nested-with-symlink.zip foo
# RAR simply copies the link target when adding the file by default, need -ol to save the link itself
rar a -ol nested-with-symlink.rar foo


rm foo
tar -xf single-nested-folder.tar
( cd foo && ln -s fighter jet; )
zip -r --symlinks folder-symlink.zip foo
# RAR simply copies the link target when adding the file by default, need -ol to save the link itself
rar a -ol folder-symlink.rar foo


cat <<EOF > CHANGELOG.md
What is Lorem Ipsum?

Lorem Ipsum is simply dummy text of the printing and typesetting industry. Lorem Ipsum has been the industry's standard dummy text ever since the 1500s, when an unknown printer took a galley of type and scrambled it to make a type specimen book. It has survived not only five centuries, but also the leap into electronic typesetting, remaining essentially unchanged. It was popularised in the 1960s with the release of Letraset sheets containing Lorem Ipsum passages, and more recently with desktop publishing software like Aldus PageMaker including versions of Lorem Ipsum.
Why do we use it?

It is a long established fact that a reader will be distracted by the readable content of a page when looking at its layout. The point of using Lorem Ipsum is that it has a more-or-less normal distribution of letters, as opposed to using 'Content here, content here', making it look like readable English. Many desktop publishing packages and web page editors now use Lorem Ipsum as their default model text, and a search for 'lorem ipsum' will uncover many web sites still in their infancy. Various versions have evolved over the years, sometimes by accident, sometimes on purpose (injected humour and the like).
EOF

cp CHANGELOG.md README.md
zip bag.zip README.md CHANGELOG.md &&
zip bag1.zip CHANGELOG.md &&
rar a rar-misrecognized-as-zip.rar bag.zip bag1.zip


tarFile='updated-file-with-file-under-that-path.tar'
echo bar > foo
tar -c --owner=user --group=group --numeric -f "$tarFile" foo
rm foo
mkdir foo
echo iriya > foo/fighter
tar -u --owner=user --group=group --numeric -f "$tarFile" foo/fighter
mkdir -p bar/par
echo ufo > bar/par/sora
tar -u --owner=user --group=group --numeric -f "$tarFile" bar
rm bar/par/sora
mkdir bar/par/sora
echo iriya > bar/par/sora/natsu
tar -u --owner=user --group=group --numeric -f "$tarFile" bar/par/sora/natsu


rm foo ./*'-incremental-backup.level.'* root-file.txt
mkdir foo
echo 'Hello World!' > root-file.txt
echo 'one' > foo/1
echo 'three' > foo/3
sleep 2s
echo 'two' > foo/2
tar -c --owner=user --group=group --numeric -f 'incremental-backup.level.0.tar' --listed-incremental="new-incremental-backup.level.0.snar" root-file.txt foo

# Create an incremental backup
cp new-incremental-backup.level.{0,1}.snar
mv foo/{1,moved}
printf '\nmodified\n' >> foo/3
rm foo/2
tar -c --owner=user --group=group --numeric -f 'incremental-backup.level.1.tar' --listed-incremental="new-incremental-backup.level.1.snar" root-file.txt foo

rm foo root-file.txt


rm foo
echo bar > foo
tar -c --owner=user --group=group --numeric -f 'single-file-incremental.tar' --incremental foo
octalMTime=$( printf %o "$( stat -c %Y foo )" )
mkdir "$octalMTime"
mv foo "$_"
tar -c --owner=user --group=group --numeric -f 'single-file-incremental-mockup.tar' "$octalMTime/foo"
rm "$octalMTime"

longName=$( printf 000000000%s 1 2 3 4 5 6 7 8 9 A B C )
rm "$longName" 'single-file-incremental-long-name'*
echo bar > "$longName"
tar -c --owner=user --group=group --numeric -f 'single-file-incremental-long-name.tar' --incremental "$longName"
octalMTime=$( printf %o "$( stat -c %Y "$longName" )" )
mkdir "$octalMTime"
mv "$longName" "$_"
tar -c --owner=user --group=group --numeric -f 'single-file-incremental-long-name-mockup.tar' "$octalMTime/$longName"
rm "$octalMTime"


echo bar > /tmp/foo
tar -c --absolute-names --owner=user --group=group --numeric -f 'absolute-file-incremental.tar' --incremental /tmp/foo


# special-char.tar
mkdir mimi momo
echo iriya > 'mimi/Datei-mit-dämlicher-Kodierung.txt'
ratarmount -o modules=iconv,to_code=ISO-8859-1 mimi momo
tar -c --owner=0 --group=0 --numeric-owner -f special-char.tar momo/*
fusermount -u momo
rm -r mimi momo


# nested special-char.tar
mkdir -p 'mimi/Ördner-mìt-dämlicher-Ködierúng'
file='Ördner-mìt-dämlicher-Ködierúng/Datei-mit-dämlicher-Kodierung.txt'
echo iriya > "mimi/$file"
ratarmount -o modules=iconv,to_code=ISO-8859-1 mimi momo
( cd momo && tar -c --owner=0 --group=0 --numeric-owner -f ../nested-special-char.tar "$file"; )
fusermount -u momo
rm -r mimi momo

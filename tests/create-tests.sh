#!/usr/bin/env bash

if ! [ -e single-file.tar ]; then
    echo "error: missing test file single-file.tar"
    echo "you have to run this script in ratarmount/tests/"
    exit 1
fi

tarc()
{
    tar -c --owner=user --group=group --numeric "$@"
}

recreateArchive()
(
    archive="$( pwd )/$( basename -- "$1" )"
    newArchive="${archive%.tar}.new.tar"
    shift

    cd -- "$( mktemp -d )" || { echo 'Failed to create temporary directory!'; return 1; }
    tar -x -f "$archive"

    # run TAR command
    set -x
    tarc -f "$newArchive" "$@"
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
tarc -f 'single-self-link.tar' bar bar
tar --delete --occurrence=1 --file 'single-self-link.tar' bar

tarc -f 'two-self-links.tar' bar bar bar
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
    ls | grep -v -x files | xargs mv -t files

    # Create archives of archives
    tarc -f  tar-with-archives.tar     -- files
    tarc -jf tar-with-archives.tar.bz2 -- files
    tarc -zf tar-with-archives.tar.gz  -- files
    tarc -Jf tar-with-archives.tar.xz  -- files

    7z a seven-elves.7z files
    rar a foos-rar-dah.rar files
    zip -r bag.zip files
    mkisofs -lJR -o miso.iso files

    cd ..
    tarc -jf super-nested-archive{.tar.bz2,}
)


tar -xf nested-tar.tar
bzip2 foo/lighter.tar
tarc -f nested-compressed-tar.tar foo
rm -r foo


tar -xf nested-tar.tar
rar a -hpfoo encrypted-headers-nested-tar.rar foo
rar a -pfoo encrypted-nested-tar.rar foo
zip -r --encrypt --password foo encrypted-nested-tar.zip foo
7z a -pfoo encrypted-nested-tar.7z foo


mkdir foo2
echo iriya > foo2/ufo
rar a file-in-non-existing-folder.rar foo2/ufo
zip file-in-non-existing-folder.zip foo2/ufo
7z a file-in-non-existing-folder.7z foo2/ufo


rm -rf foo ufo
echo fighter > foo
echo iriya > ufo
rar a natsu.rar foo ufo
zip rar.zip natsu.rar
7z a natsu.7z foo ufo

zip natsu.zip foo ufo
rar a zip.rar natsu.zip
7z a zip.7z natsu.zip


rm foo
tar -xf nested-tar.tar
( cd foo/fighter && ln -s ufo saucer; )
zip -r --symlinks nested-with-symlink.zip foo
# RAR simply copies the link target when adding the file by default, need -ol to save the link itself
rar a -ol nested-with-symlink.rar foo
7z a -snl nested-with-symlink.7z foo


rm -rf foo
tar -xf single-nested-folder.tar
( cd foo && ln -s fighter jet; )
zip -r --symlinks folder-symlink.zip foo
# RAR simply copies the link target when adding the file by default, need -ol to save the link itself
rar a -ol folder-symlink.rar foo
7z a -ol folder-symlink.7z foo

tar -cf- foo | sqfstar -noI -noId -noD -noF -noX folder-symlink.no-compression.squashfs
for compression in gzip lzma lzo lz4 xz zstd; do
    tar -cf- foo | sqfstar -comp "$compression" "folder-symlink.$compression.squashfs"
done


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
tarc -f "$tarFile" foo
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
tarc -f 'incremental-backup.level.0.tar' --listed-incremental="new-incremental-backup.level.0.snar" root-file.txt foo

# Create an incremental backup
cp new-incremental-backup.level.{0,1}.snar
mv foo/{1,moved}
printf '\nmodified\n' >> foo/3
rm foo/2
tarc -f 'incremental-backup.level.1.tar' --listed-incremental="new-incremental-backup.level.1.snar" root-file.txt foo

rm foo root-file.txt


rm foo
echo bar > foo
tarc -f 'single-file-incremental.tar' --incremental foo
octalMTime=$( printf %o "$( stat -c %Y foo )" )
mkdir "$octalMTime"
mv foo "$_"
tarc -f 'single-file-incremental-mockup.tar' "$octalMTime/foo"
rm "$octalMTime"

longName=$( printf 000000000%s 1 2 3 4 5 6 7 8 9 A B C )
rm "$longName" 'single-file-incremental-long-name'*
echo bar > "$longName"
tarc -f 'single-file-incremental-long-name.tar' --incremental "$longName"
octalMTime=$( printf %o "$( stat -c %Y "$longName" )" )
mkdir "$octalMTime"
mv "$longName" "$_"
tarc -f 'single-file-incremental-long-name-mockup.tar' "$octalMTime/$longName"
rm "$octalMTime"


echo bar > /tmp/foo
tarc --absolute-names -f 'absolute-file-incremental.tar' --incremental /tmp/foo


# special-char.tar
mkdir mimi momo
echo iriya > 'mimi/Datei-mit-dämlicher-Kodierung.txt'
ratarmount -o modules=iconv,to_code=ISO-8859-1 mimi momo
tarc -f special-char.tar momo/*
fusermount -u momo
rm -r mimi momo


# nested special-char.tar
mkdir -p 'mimi/Ördner-mìt-dämlicher-Ködierúng'
file='Ördner-mìt-dämlicher-Ködierúng/Datei-mit-dämlicher-Kodierung.txt'
echo iriya > "mimi/$file"
ratarmount -o modules=iconv,to_code=ISO-8859-1 mimi momo
( cd momo && tarc -f ../nested-special-char.tar "$file"; )
fusermount -u momo
rm -r mimi momo


# 1000 times packed
echo iriya > ufo
tar -O -c -f ufo_00.tar ufo
for (( i=1; i < 100; ++i )); do
    tar -O -c -f "ufo_$( printf %02i "$i" ).tar" "ufo_$( printf %02i "$(( i-1 ))" ).tar"
    'rm' "ufo_$( printf %02i "$(( i-1 ))" ).tar"
done
mv ufo_99.tar packed-100-times.tar


echo iriya > ufo
tar -O --gzip -c -f ufo_00.tar.gz ufo
for (( i=1; i < 100; ++i )); do
    tar -O --gzip -c -f "ufo_$( printf %02i "$i" ).tar.gz" "ufo_$( printf %02i "$(( i-1 ))" ).tar.gz"
    'rm' "ufo_$( printf %02i "$(( i-1 ))" ).tar.gz"
done
mv ufo_99.tar.gz compressed-100-times.tar.gz


echo iriya > ufo
tar -O --gzip -c -f ufo_000.tar.gz ufo
for (( i=1; i < 1000; ++i )); do
    tar -O --gzip -c -f "ufo_$( printf %03i "$i" ).tar.gz" "ufo_$( printf %03i "$(( i-1 ))" ).tar.gz"
    'rm' "ufo_$( printf %03i "$(( i-1 ))" ).tar.gz"
done
mv ufo_999.tar.gz compressed-1000-times.tar.gz


echo iriya > ufo
gzip -c ufo > ufo_000.gz
for (( i=1; i < 1000; ++i )); do
    gzip -c "ufo_$( printf %03i "$(( i-1 ))" ).gz" > "ufo_$( printf %03i "$i" ).gz"
    'rm' "ufo_$( printf %03i "$(( i-1 ))" ).gz"
done
mv ufo_999.gz compressed-1000-times.gz


echo iriya > ufo
gzip -c ufo > ufo_00.gz
for (( i=1; i < 100; ++i )); do
    gzip -c "ufo_$( printf %02i "$(( i-1 ))" ).gz" > "ufo_$( printf %02i "$i" ).gz"
    'rm' "ufo_$( printf %02i "$(( i-1 ))" ).gz"
done
mv ufo_99.gz compressed-100-times.gz


# parent path
mkdir -p root
(
    cd root || exit 1
    mkdir -p folder
    echo iriya > ufo
    echo foo > bar
    echo bar > foo

    # 7z cleans those paths but Zip 3.0 (July 5th 2008), by Info-ZIP only cleans the leading ./././ but not any other ones
    # zipinfo denormal-paths.zip
    #     -rwx------  3.0 unx  6 tx stor 22-Nov-06 17:57 folder/../ufo
    #     -rwx------  3.0 unx  4 tx stor 22-Nov-06 17:57 ../root/bar
    #     -rwx------  3.0 unx  4 tx stor 22-Nov-06 17:57 folder/../././foo
    zip ../denormal-paths.zip folder/../ufo ../root/bar ./././folder/../././foo

    # tar fails to clean up some ./ but cleans up most .. in the path. However, with transform, we can re-add '..'!
    tarc --transform 's,ufo,root/../ufo,' --transform 's,root/bar,../root/./bar,' -f ../denormal-paths.tar \
        ./folder/.././folder/./../ufo ../root/./bar ./././folder/../././foo
    # tar tvlf denormal-paths.tar
    #     -rwx------ 1000/1000         6 2022-11-06 18:00 root/../ufo
    #     -rwx------ 1000/1000         4 2022-11-06 18:00 root/./bar
    #     -rwx------ 1000/1000         4 2022-11-06 18:00 ././foo
    python3 -c '
import os
import tarfile
with tarfile.open("../denormal-paths-tarfile.tar", "w") as tar:
    for path in ["folder/../ufo", "../root/bar", "./././folder/../././foo"]:
        tarInfo = tar.gettarinfo(path)
        tarInfo.uname = ""
        tarInfo.gname = ""
        tar.addfile(tarInfo)
'
    # Not only is tar refusing to add leading ../ to contained files it even errors out on such TARs when created
    # with Python's tarfile module!
    # tar tvlf ../denormal-paths.tar
    #     tar: Removing leading `folder/../' from member names
    #     -rwx------ 1000/1000         6 2022-11-06 20:21 folder/../ufo
    #     tar: Skipping to next header
    #     tar: Removing leading `../' from member names
    #     -rwx------ 1000/1000         4 2022-11-06 20:21 ../root/bar
    #     tar: Skipping to next header
    #     tar: Removing leading `./././folder/../' from member names
    #     -rwx------ 1000/1000         4 2022-11-06 20:21 ./././folder/../././foo
    #     tar: Exiting with failure status due to previous errors
    # Actually, even tarfile fails to read all but the 'ufo' file!

    # rar automatically normalizes paths and removes leading ../ so we have to re-add with -ap
    rar a -apfolder/../ ../denormal-paths.rar folder/../ufo
    rar a -ap../ ../denormal-paths.rar ../root/bar
    rar a -ap./././folder/../././ ../denormal-paths.rar ./././folder/../././foo
    # rar l ../denormal-paths.rar
    #     -rwx------  6  2022-11-06 20:21  folder/../ufo
    #     -rwx------  4  2022-11-06 20:21  ../root/bar
    #     -rwx------  4  2022-11-06 20:21  ./././folder/../././foo
)
rm -rf root


# Split files
echo foo >> simple-file-split.001
echo bar >> simple-file-split.002

split --numeric-suffixes=1 --number=2 --suffix-length=3 single-file.tar single-file-split.tar.


# Self-extracting archives
echo iriya > ufo
zip single-file.zip
echo 0000 > mockup-self-extracting.zip
cat single-file.zip >> mockup-self-extracting.zip

# Chimera file
cp single-file.tar.bz2 chimera-tbz2-zip
cat folder-symlink.zip >> chimera-tbz2-zip

# Double-compressed
7z a nested-tar.tar{.7z,}
7z a double-compressed-nested-tar.tar.7z.7z nested-tar.tar.7z

# Simple stream compressions
# sudo apt install gzip bzip2 lzip lzip ncompress lrzip
echo "foo fighter" > simple
bzip2 -k simple
gzip -k simple
xz -k simple
lzma -k simple
lz4 -k simple
lrzip simple
lzip -k -o simple{.lzip,}  # Default extension: .lz
lzop -k simple  # extension .lzo
compress < simple > simple.Z

if ! [ -e hello-world.warc ]; then
    wget 'https://github.com/iipc/warc-specifications/raw/be2ac9e0af22eb0ac50fef691ece7417932fcdb3/primers/web-archive-formats/hello-world.warc'
fi

# Libarchive-supported archive formats
# sudo apt install binutils lcab genisoimage
rm -rf foo
tar tvlf single-file.tar
ar rcs single-file.ar bar
lcab bar single-file.cab
xar -c -f single-file.xar bar
for format in bin odc newc crc hpbin hpodc; do
    echo bar | cpio --format="$format" --create > "single-file.$format.cpio"
done
genisoimage -o single-file.iso -V volume-foo -R -J bar

# Large archive with two files to test seekability and independence of opened files.
true > spaces-32-MiB.txt; for i in $( seq $(( 32 * 1024 )) ); do printf '%1024s' $'\n' >> spaces-32-MiB.txt; done
true > zeros-32-MiB.txt; for i in $( seq $(( 32 * 1024 )) ); do printf '%01023d\n' 0 >> zeros-32-MiB.txt; done
7z a two-large-files-32Ki-lines-each-1024B.7z spaces-32-MiB.txt zeros-32-MiB.txt

# Large archive with two files to test seekability and independence of opened files.
true > spaces-32-MiB.txt; for i in $( seq $(( 32 * 1024 )) ); do printf '%1023s' $'\n' >> spaces-32-MiB.txt; done
true > zeros-32-MiB.txt; for i in $( seq $(( 32 * 1024 )) ); do printf '%01022d\n' 0 >> zeros-32-MiB.txt; done
7z a two-large-files-32Ki-lines-each-1023B.7z spaces-32-MiB.txt zeros-32-MiB.txt

# Would be nice to have this without sudo, but I don't want to create test cases with the same program being tested.
head -c $(( 1024 * 1024 )) /dev/zero > 'folder-symlink.fat'
mkfs.fat 'folder-symlink.fat'
mkdir mounted
sudo mount 'folder-symlink.fat' mounted
( cd mounted && sudo unzip ../tests/folder-symlink.zip )
sudo umount mounted

echo bar > foo
setfattr --name user.tags --value mytag foo
#getfattr --dump foo
bsdtar --numeric-owner --xattrs -cf file-with-attribute.bsd.tar foo
tar --numeric-owner --xattrs -cf file-with-attribute.gnu.tar foo

# sqlar
if ! command -v sqlar 2>/dev/null; then
    # download and build sqlar
    name='sqlar-src-4824e73896'
    wget "https://www.sqlite.org/sqlar/tarball/4824e73896/${name}.tar.gz"
    tar -xf "${name}.tar.gz"
    (
        cd -- "$name" &&
        sed -i 's|-Werror||g' Makefile &&
        make
    )
    export PATH="$PATH:$PWD/$name"
fi
if command -v sqlar 2>/dev/null; then
    # run sqlar
    (
        cd -- "$name" &&
        tar -xf ../nested-tar.tar &&
        sqlar ../nested-tar-compressed.sqlar foo/ &&
        sqlar -n ../nested-tar.sqlar foo/
    )
fi

if ! python3 -c "import sqlcipher3" 2>/dev/null; then
    python3 -m pip install sqlcipher3-binary
fi
# This unfortunately does not work :(
#cp nested-tar{,-encrypted}.sqlar
#python3 -c '
#from sqlcipher3 import dbapi2 as sqlcipher3;
#c = sqlcipher3.connect("encrypted-nested-tar.sqlar");
#c.execute("PRAGMA rekey=\"foo\";");'
#
# > sqlcipher3.dbapi2.OperationalError: An error occurred with PRAGMA key or rekey. PRAGMA key requires a key of one
# > or more characters. PRAGMA rekey can only be run on an existing encrypted database. Use sqlcipher_export() and
# > ATTACH to convert encrypted/plaintext databases.
# Note that this seems to be an error from the C library because sqlcipher_export only exists there.
sqlite3 nested-tar.sqlar .schema
python3 -c '
from sqlcipher3 import dbapi2 as sqlcipher3
c1 = sqlcipher3.connect("nested-tar.sqlar")

c2 = sqlcipher3.connect("encrypted-nested-tar.sqlar")
c2.execute("PRAGMA key=\"foo\";")
c2.executescript("""DROP TABLE IF EXISTS sqlar;
CREATE TABLE sqlar(
  name TEXT PRIMARY KEY,
  mode INT,
  mtime INT,
  sz INT,
  data BLOB
);
""")
rows = c1.execute("SELECT * FROM sqlar;").fetchall()
c2.executemany("INSERT INTO sqlar VALUES (?,?,?,?,?);", rows)
c2.commit()
c2.close()
'
sqlite3 encrypted-nested-tar.sqlar 'SELECT * FROM sqlite_master'
# Error: in prepare, file is not a database (26)
python3 -c 'import sqlite3; print(sqlite3.connect("encrypted-nested-tar.sqlar").execute("SELECT * FROM sqlite_master").fetchall())'
# Traceback (most recent call last):
#   File "<string>", line 1, in <module>
# sqlite3.DatabaseError: file is not a database
python3 -c 'from sqlcipher3 import dbapi2 as sqlcipher3;
print(sqlcipher3.connect("encrypted-nested-tar.sqlar").execute("SELECT * FROM sqlite_master").fetchall())'
# Traceback (most recent call last):
#   File "<string>", line 2, in <module>
# sqlcipher3.dbapi2.DatabaseError: file is not a database
python3 -c '
from sqlcipher3 import dbapi2 as sqlcipher3;
c = sqlcipher3.connect("encrypted-nested-tar.sqlar")
c.execute("PRAGMA key=\"foo\"")
print(c.execute("SELECT * FROM sqlite_master").fetchall())
print(c.execute("SELECT name FROM sqlar").fetchall())'
# [('table', 'sqlar', 'sqlar', 2, 'CREATE TABLE sqlar(\n  name TEXT PRIMARY KEY,\n  mode INT,\n  mtime INT,\n  sz INT,\n  data BLOB\n)'), ('index', 'sqlite_autoindex_sqlar_1', 'sqlar', 3, None)]

python3 <<EOF
import urllib.parse
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.backends import default_backend
import sqlcipher3.dbapi2 as sqlcipher3

fname = "encrypted-nested-tar.sqlar"
password = b"foo"

key = PBKDF2HMAC(
    algorithm=hashes.SHA512(),
    # 256-bit key for 256-bit AES in CBC mode
    length=32,
    # The salt is stored in the first 16 bytes.
    salt=open(fname, 'rb').read(16),
    # This is the current default. Older versions may have used fewer iterations.
    # It can also be specified with 'PRAGMA kdf_iter'.
    iterations=256_000,
    backend=default_backend(),
).derive(password)

c = sqlcipher3.connect(f"file:{urllib.parse.quote(fname)}?mode=ro", uri=True)
c.execute(f"PRAGMA key = \"x'{key.hex()}'\";")
print(c.execute("SELECT * FROM sqlite_master;").fetchall())

print(c.execute("SELECT name FROM sqlar").fetchall())
EOF
# [('table', 'sqlar', 'sqlar', 2,
#   'CREATE TABLE sqlar(\n  name TEXT PRIMARY KEY,\n  mode INT,\n  mtime INT,\n  sz INT,\n  data BLOB\n)'),
#   ('index', 'sqlite_autoindex_sqlar_1', 'sqlar', 3, None)]
# [('foo',), ('foo/fighter',), ('foo/fighter/ufo',), ('foo/lighter.tar',)]

# EXT4
mountPoint='ext4mount'
mkdir "$mountPoint"
for size in 1M 10M; do
    name=nested-tar-$size.ext4
    dd if=/dev/zero of="$name" bs="$size" count=1
    mkfs.ext4 "$name"
    # For 1M, I get:
    #   Filesystem too small for a journal
    #   Creating filesystem with 256 4k blocks and 128 inodes
    # I don'T get this for 10M, so I guess I should test with both.
    sudo mount -o loop "$name" "$mountPoint"  # Still not possible without sudo :(
    (
        cd "$mountPoint" &&
        sudo rmdir --ignore-fail-on-non-empty 'lost+found' &&
        sudo chmod a+rwx . &&
        tar -xf "$OLDPWD/tests/nested-tar.tar"
    )
    sudo umount "$mountPoint"
done
rmdir "$mountPoint"

# SAR
if ! command -v asar 2>/dev/null; then
    if ! command -v npm 2>/dev/null; then
        sudo apt install npm --no-install-recommends
    fi
    if ! command -v node 2>/dev/null; then
        # TODO? use "sudo apt install nodejs"
        # install node to /usr/local/bin/
        sudo npx n latest  # 24.1.0
    fi
    # avoid the "Ok to proceed? (y)" dialog of npx
    npm install @electron/asar
    function asar() { npx @electron/asar "$@"; }
fi
asar --help
asar pack non-existing empty.asar
# For some reason asar removes the top-level folder, so we need to nest it -.-
mkdir foodir
mv foo foodir
asar pack foodir nested-tar.asar

# Skippable frame in LZ4
printf '\x5a\x2a\x4d\x18\x03\x00\x00\x00\x00\x00\x00' > nested-tar.skippable-frame.lz4
lz4 -c nested-tar.tar >> nested-tar.skippable-frame.lz4

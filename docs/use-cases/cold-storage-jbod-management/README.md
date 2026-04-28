Assuming you simply using just a bunch of disks, no RAID.
Some of the older disks are simply stored on shelves as cold storage backups.
The data on them changes only very rarely if at all.

We want several things:

 1. A metadata database, similar to updatedb/plocate, for locating files or browsing, even on non-connected drives.
   - Index each drive like this:
     `ratarmount --hashes crc32,sha1,sha256,smplayer --force-folder-index --index-file ~/drive.sqlite --no-mount /media/drive/`
   - `ratarmount old.sqlite old`. Now the mount point `old` shows the whole folder hierarchy of the drive and
     with `getfattr --dump`, you can dump the checksums stored as extended attributes.
     In the future, the file contents should show thumbnails for images and maybe an animated thumbnail for videos.

 2. Check which of the older drives can be removed because all of their data exists on 1+ other disks.
   - Check which files on the old drive ceased to exist on newer drives:
     `python3 find-unbacked-files.py --main-db old.sqlite --other-dbs new-drive-1.sqlite new-drive-2.sqlite -t 1`
   - In case, some of the file names contains a CRC32 checksum in parentheses, list all broken files:
     `sqlite3 old.sqlite '.read find-mismatching-crc32.sql'`
   - Using `find-unbacked-files.py --exclude-crc-mismatch ...` shows unbacked files without those with broken CRC32.
     Broken files, whether replaced or not, probably are fine to get lost.

# mbox-db

Simple solution for email backups.
Write-only DB with easy encryption integration and sync-friendly

## Why I need this?
This tool is not IMAP/email backup,
this tools only helps you to organise your existing backups.

Also, since all backup files are written only once,
you can be sure that nothing will be lost in the future.
Data is not duplicated, so the backup takes up little space.


## How to use?

0. You need Python 3.6+ and pip.
1. Download `.mbox` files from your mail provider.
I use (this](https://github.com/ralbear/IMAPbackup) simple tool.
2. Install dependencies `pip3 install --user -r requirements.txt` or `pip3 install --user -r requirements.txt` (may require root on Linux or admin rights on Windows).
3. Run `python3 do_backup.py -m meta.db -s storage -i PATH_TO_MBOX -p`
4. Your _write-only_ objects located at `storage`,
sqlite database with metadata located at `meta.db`.
You can view it with, for example, [SQLiteStudio](https://sqlitestudio.pl/)

## Pro use

* You can encrypt results with any FUSE encryption module,
for example, encfs or gocryptfs.
* It is possible to recover metadata db form storage objects.
But **not implemented** now.
* You can process many files at once with included script:
`python3 recursive_backup.py FOLDER_WITH_MBOX -m meta.db -s storage`,
`FOLDER_WITH_MBOX` will be traversed recursively to find all `*.mbox` files.

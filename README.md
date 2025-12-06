# adaptive-backup

Utility to back up a [remoteStorage](https://remotestorage.io/) account to a local drive, and restore to that or another account.

Limits the number of simultaneous requests and honors HTTP statuses `429 Too Many Requests` and `503 Service Unavailable`, for fast and reliable transfers.
Avoids unnecessary transfers.

Before backing up, moves the old contents of the backup directory to your temporary folder.
To keep multiple local backups, use multiple backup directories.

Restore will overwrite existing documents on the server.
Files added to the backup before restoration will be created on the server.
Files deleted from the backup will *not* be deleted from the server if they exist there.
Files can be edited before restoration, unless you set the `--etag-algorithm` argument to blank (to trust the ETags in the `000_folder-description.json` files).

Based on [rs-backup](https://github.com/raucao/rs-backup.git), but re-architected.
Backups are compatible with rs-backup.

[//]: # (## Installation)

[//]: # ()
[//]: # (1. Install [node.js]&#40;https://nodejs.org/en/download&#41;)

[//]: # (2. `npm install -g adaptive-backup`)

[//]: # ()
[//]: # ()
[//]: # (## Usage &#40;installed&#41;)

[//]: # ()
[//]: # (    adaptive-backup -o path-to-backup-dir)

[//]: # ()
[//]: # (    adaptive-restore -i path-to-backup-dir)

[//]: # ()
[//]: # (Use option `--help` to see all options.)

## Usage (repository downloaded)

    node ./backup.js -o path-to-backup-dir

    node ./restore.js -o path-to-backup-dir

Use option `--help` to see all options, including options to set user address and authorization token, as is needed by automated backup jobs.


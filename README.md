# throttled-backup

remoteStorage backup and restore utility which limits the number of simultaneous requests and honors status 429 and 503

Based on [rs-backup](https://github.com/raucao/rs-backup.git), but re-architected.

## Installation



## Usage

    throttled-backup -o path_to_backup_dir
    throttled-restore -i path_to_backup_dir

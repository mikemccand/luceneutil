#!/bin/bash -i

set -x

/bin/bash /home/mike/nightly_strict.cmd

echo "Copy all.log up..."

cd /l/util.nightly

/usr/bin/python3 -u src/python/syncAllLog.py

echo "Backup bench logs to /ssd/logs.nightly.bak"
rsync -a /l/logs.nightly/ /ssd/logs.nightly.bak/

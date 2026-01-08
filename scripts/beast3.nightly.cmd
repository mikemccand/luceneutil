#!/bin/bash -i

set -x

/bin/bash /home/mike/nightly_strict.cmd

#echo "Copy all.log up..."

#cd /l/util.nightly

#echo "Backup bench logs to /ssd/logs.nightly.bak"
#rsync -a /l/logs.nightly/ /ssd/logs.nightly.bak/

# echo "Copy all.log up..."
cp /l/logs.nightly/all.log /l/reports.nightly

echo "clean /l/tmp"
rm -rf /l/tmp/*

echo
echo "Now copy all.log to nightly timestamp'd log dir"
python3 -u /l/util.nightly/src/python/save_full_log.py

echo
echo "Now git add/commit nightly log file (all.log)"
cd /l/lucenenightly
git add -u .
git commit -m "$(date): auto commit nightly benchy log file"
git push

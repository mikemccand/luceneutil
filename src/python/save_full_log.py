import os
import shutil

import constants

# silly little log helper tool

all_log_path = os.path.join(constants.LOGS_DIR, "all.log")
with open(all_log_path) as f:
  for line in f.readlines():
    i = line.find("] log dir ")
    if i != -1:
      log_dir = line[i + 10 :].strip()
      print(f"now shutil.copy2 {all_log_path} to {log_dir}")
      shutil.copy2(all_log_path, os.path.join(log_dir, "all.log"))
      break
  else:
    raise RuntimeError(f"did not find timestamp'd log dir for this run in {all_log_path}")

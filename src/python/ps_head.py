# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import os
import shutil
import subprocess
import threading
import time

PS_EXE_PATH = shutil.which("ps")

# NOTE: 'watch' seemed to demand interactivity (it won't just log nicely to a file), and
#       'top' requires interactivity to set top N?


class PSTopN:
  """Simple class to launch a background thread that
  periodically records top N processes (by CPU descending) to
  a destination log file.
  """

  def __init__(self, top_n, log_file_name, poll_interval_sec=5):
    if PS_EXE_PATH is None:
      raise RuntimeError("could not find ps executable in this environment")
    if os.path.exists(log_file_name):
      raise RuntimeError(f"please remove log file {log_file_name} first")
    self.cmd = f"{PS_EXE_PATH} -eo pid,%cpu,%mem,bsdtime,etime,start,args --cols=120 --sort=-%cpu | head -{top_n} >> {log_file_name} 2>&1"
    self.stop_now = False
    self.poll_interval_sec = poll_interval_sec
    self.wakey_wakey = threading.Condition()
    self.log_file_name = log_file_name

    self.thread = threading.Thread(target=self.__run_thread)
    self.thread.start()

  def stop(self):
    self.stop_now = True
    with self.wakey_wakey:
      self.wakey_wakey.notify()
    self.thread.join()

  def __run_thread(self):
    start_time = None
    target_time = None
    while not self.stop_now:
      with open(self.log_file_name, "a") as f:
        if start_time is None:
          start_time = time.monotonic()
          target_time = start_time
        f.write(f"\n{datetime.datetime.now()}\n")
      try:
        subprocess.check_call(self.cmd, shell=True)
      except subprocess.CalledProcessError as e:
        print(f'command "{self.cmd}" failed with errorcode {e.returncode}')
        print(f"stdout: {e.stdout}")
        print(f"stderr: {e.stderr}")
        raise

      target_time += self.poll_interval_sec
      with self.wakey_wakey:
        wait_time = target_time - time.monotonic()
        if wait_time > 0:
          self.wakey_wakey.wait(wait_time)


if __name__ == "__main__":
  ps_topn = PSTopN(10, "ps_test.log", 1)
  time.sleep(7)
  ps_topn.stop()

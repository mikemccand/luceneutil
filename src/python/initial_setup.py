#!/usr/bin/env python

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

import argparse
import os
import shutil
import sys
import time
from urllib.request import urlretrieve

PYTHON_MAJOR_VER = sys.version_info.major

BASE_URL = "https://home.apache.org/~mikemccand"
BASE_URL2 = "https://home.apache.org/~sokolov"

DATA_FILES = [
  # remote url, local name
  ("https://luceneutil-corpus-files.s3.ca-central-1.amazonaws.com/enwiki-20120502-lines-1k-fixed-utf8-with-random-label.txt.lzma",
   "enwiki-20120502-lines-1k-fixed-utf8-with-random-label.txt.lzma"),
  # ("https://luceneutil-corpus-files.s3.ca-central-1.amazonaws.com/cohere-wikipedia-docs-768d.vec", "cohere-wikipedia-docs-768d.vec"),
  ("https://luceneutil-corpus-files.s3.ca-central-1.amazonaws.com/cohere-wikipedia-docs-5M-768d.vec", "cohere-wikipedia-docs-5M-768d.vec"),
  ("https://luceneutil-corpus-files.s3.ca-central-1.amazonaws.com/cohere-wikipedia-queries-768d.vec", "cohere-wikipedia-queries-768d.vec"),
  ("https://downloads.cs.stanford.edu/nlp/data/glove.6B.zip", "glove.6B.zip"),
]

USAGE = """
Usage: python initial_setup.py [-download]

Options:
  -download downloads a 5GB linedoc file 

"""
DEFAULT_LOCAL_CONST = """
BASE_DIR = '%(base_dir)s'
BENCH_BASE_DIR = '%(base_dir)s/%(cwd)s'
"""


def runSetup(download):
  cwd = os.getcwd()
  parent, base = os.path.split(cwd)
  data_dir = os.path.join(parent, "data")
  idx_dir = os.path.join(parent, "indices")

  if not os.path.exists(data_dir):
    print("create data directory at %s" % (data_dir))
    os.mkdir(data_dir)
  else:
    print("data directory already exists %s" % (data_dir))

  if not os.path.exists(idx_dir):
    os.mkdir(idx_dir)
    print("create indices directory at %s" % (idx_dir))
  else:
    print("indices directory already exists %s" % (idx_dir))

  pySrcDir = os.path.join(cwd, "src", "python")
  local_const = os.path.join(pySrcDir, "localconstants.py")
  if not os.path.exists(local_const):
    f = open(local_const, "w")
    try:
      f.write(DEFAULT_LOCAL_CONST % ({"base_dir": parent, "cwd": base}))
    finally:
      f.close()
  else:
    print("localconstants.py already exists - skipping")

  local_run = os.path.join(pySrcDir, "localrun.py")
  example = os.path.join(pySrcDir, "example.py")
  if not os.path.exists(local_run):
    shutil.copyfile(example, local_run)
  else:
    print("localrun.py already exists - skipping")

  if download:
    for url_source, local_filename in DATA_FILES:
      target_file = os.path.join(data_dir, local_filename)
      if os.path.exists(target_file):
        print("file %s already exists - skipping" % target_file)
      else:
        print("download %s to %s - might take a long time!" % (url_source, target_file))
        Downloader(url_source, target_file).download()
        print()
        print("downloading %s to %s done " % (url_source, target_file))

      for suffix in (".bz2", ".lzma", ".zip", ".xz"):
        if target_file.endswith(suffix):
          print("NOTE: make sure you decompress %s" % target_file)
          break

  print("setup successful")


class Downloader:
  HISTORY_SIZE = 100

  def __init__(self, url, target_path):
    self.__url = url
    self.__target_path = target_path
    Downloader.times = [time.time()] * Downloader.HISTORY_SIZE
    Downloader.sizes = [0] * Downloader.HISTORY_SIZE
    Downloader.index = 0

  def download(self):
    urlretrieve(self.__url, self.__target_path, Downloader.reporthook)

  @staticmethod
  def reporthook(count, block_size, total_size):
    current_time = time.time()
    current_size = int(count * block_size)
    last_time = Downloader.times[Downloader.index]
    last_size = Downloader.sizes[Downloader.index]
    delta_size = current_size - last_size
    delta_time = current_time - last_time
    Downloader.times[Downloader.index] = current_time
    Downloader.sizes[Downloader.index] = current_size
    Downloader.index = (Downloader.index + 1) % Downloader.HISTORY_SIZE

    speed = float(delta_size) / (1024 * delta_time)
    percent = int(current_size * 100 / total_size)
    sys.stdout.write("\r ")
    #    sys.stdout.write('(%d, %d), (%d, %d), (%d, %d) ' % (current_size, current_time, last_size, last_time, delta_size, delta_time))
    sys.stdout.write("downloading ... %d%%, %.2f MB/%.2fMB, speed %.2f KB/s" % (percent, float(current_size) / (1024 * 1024), float(total_size) / (1024 * 1024), speed))
    sys.stdout.flush()


if __name__ == "__main__":
  parser = argparse.ArgumentParser(prog="luceneutil setup", description="Benchmarking setup for lucene")
  parser.add_argument(
    "-d",
    "-download",
    "--download",
    action="store_true",
    help="Download datasets to run benchmarks. A 6 GB compressed Wikipedia line doc file, and a 13 GB vectors file is downloaded from Apache mirrors",
  )
  args = parser.parse_args()
  runSetup(args.download)

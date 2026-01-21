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
import ssl
import sys
import time
from urllib import request

PYTHON_MAJOR_VER = sys.version_info.major

BASE_URL = "https://home.apache.org/~mikemccand"
BASE_URL2 = "https://home.apache.org/~sokolov"

DATA_FILES = [
  # remote url, local name
  "https://pub-6de3254d7180436684278e0ec33ada22.r2.dev/enwiki-20120502-lines-1k-fixed-utf8-with-random-label.txt.lzma",
  "https://pub-a0911d22bca84510bc906f76af183a65.r2.dev/cohere-v3-wikipedia-en-scattered-1024d.docs.first1M.vec",
  "https://pub-a0911d22bca84510bc906f76af183a65.r2.dev/cohere-v3-wikipedia-en-scattered-1024d.docs.first1M.csv",
  "https://pub-a0911d22bca84510bc906f76af183a65.r2.dev/cohere-v3-wikipedia-en-scattered-1024d.queries.first200K.vec",
  "https://pub-a0911d22bca84510bc906f76af183a65.r2.dev/cohere-v3-wikipedia-en-scattered-1024d.queries.first200K.csv",
  "https://downloads.cs.stanford.edu/nlp/data/glove.6B.zip",
  "https://download.geonames.org/export/dump/allCountries.zip",
  # NAD (National Address Database) for facets benchmark (~7.8 GB)
  ("https://data.transportation.gov/download/fc2s-wawr/", "NAD_r21_TXT.zip"),
]

USAGE = """
Usage: python initial_setup.py [-download]

Options:
  -download downloads a 5GB linedoc file and untold GB of vector files

"""
DEFAULT_LOCAL_CONST = """
BASE_DIR = '%(base_dir)s'
BENCH_BASE_DIR = '%(base_dir)s/%(cwd)s'
"""


def runSetup(download, insecure_ssl=False):
  print("=" * 60)
  print("Lucene Benchmarking Utility - Initial Setup")
  print("=" * 60)

  cwd = os.getcwd()
  parent, base = os.path.split(cwd)
  data_dir = os.path.join(parent, "data")
  idx_dir = os.path.join(parent, "indices")

  print("\n[Step 1/5] Setting up directory structure...")
  print(f"  Working directory: {cwd}")
  print(f"  Parent directory: {parent}")

  if not os.path.exists(data_dir):
    print(f"  Creating data directory at {data_dir}")
    os.mkdir(data_dir)
  else:
    print(f"  Data directory already exists: {data_dir}")

  if not os.path.exists(idx_dir):
    os.mkdir(idx_dir)
    print(f"  Creating indices directory at {idx_dir}")
  else:
    print(f"  Indices directory already exists: {idx_dir}")

  print("\n[Step 2/5] Creating localconstants.py configuration file...")
  pySrcDir = os.path.join(cwd, "src", "python")
  local_const = os.path.join(pySrcDir, "localconstants.py")
  if not os.path.exists(local_const):
    print(f"  Writing configuration to {local_const}")
    f = open(local_const, "w")
    try:
      f.write(DEFAULT_LOCAL_CONST % ({"base_dir": parent, "cwd": base}))
    finally:
      f.close()
    print("  Configuration file created successfully")
  else:
    print(f"  localconstants.py already exists at {local_const} - skipping")

  print("\n[Step 3/5] Setting up localrun.py from example template...")
  local_run = os.path.join(pySrcDir, "localrun.py")
  example = os.path.join(pySrcDir, "example.py")
  if not os.path.exists(local_run):
    print(f"  Copying {example} to {local_run}")
    shutil.copyfile(example, local_run)
    print("  localrun.py created successfully")
  else:
    print(f"  localrun.py already exists at {local_run} - skipping")

  if download:
    print("\n[Step 4/5] Downloading benchmark data files...")
    print(f"  Target directory: {data_dir}")
    print(f"  Number of files to process: {len(DATA_FILES)}")

    for i, tup in enumerate(DATA_FILES, 1):
      if type(tup) is str:
        url_source = tup
        local_filename = os.path.basename(url_source)
      elif type(tup) is tuple and len(tup) == 2:
        url_source, local_filename = tup
      else:
        raise RuntimeError(f"DATA_FILES elements should be single string or length 2 tuple; got: {tup}")
      target_file = os.path.join(data_dir, local_filename)

      print(f"\n  [{i}/{len(DATA_FILES)}] Processing: {local_filename}")
      if os.path.exists(target_file):
        print(f"    File already exists - skipping")
      else:
        print(f"    Source URL: {url_source}")
        print(f"    Destination: {target_file}")
        print(f"    Starting download (this might take a while)...")
        Downloader(url_source, target_file, insecure_ssl).download()
        print()
        print(f"    Download complete: {local_filename}")

      for suffix in (".bz2", ".lzma", ".zip", ".xz"):
        if target_file.endswith(suffix):
          print(f"    NOTE: Remember to decompress {target_file}")
          break

    # Process NAD data to generate taxonomy file
    print("\n[Step 5/5] Processing NAD data...")
    nad_zip = os.path.join(data_dir, "NAD_r21_TXT.zip")
    nad_taxonomy = os.path.join(data_dir, "NAD_taxonomy.txt.gz")
    if os.path.exists(nad_taxonomy):
      print(f"  NAD taxonomy file already exists at {nad_taxonomy} - skipping")
    elif not os.path.exists(nad_zip):
      print(f"  NAD zip file not found at {nad_zip} - skipping taxonomy generation")
    else:
      print(f"  Generating NAD taxonomy from {nad_zip}")
      print(f"  This processes ~92M address records and may take several minutes...")
      from generateNADTaxonomies import generate_nad_taxonomy
      generate_nad_taxonomy(parent)
      print(f"  NAD taxonomy generation complete: {nad_taxonomy}")
  else:
    print("\n[Step 4/5] Skipping data file downloads (use -download flag to enable)")
    print("\n[Step 5/5] Skipping NAD processing (requires -download flag)")

  print("\n" + "=" * 60)
  print("Setup completed successfully!")
  print("=" * 60)


class Downloader:
  HISTORY_SIZE = 100

  def __init__(self, url, target_path, insecure_ssl=False):
    self.__url = url
    self.__target_path = target_path
    self.__insecure_ssl = insecure_ssl
    Downloader.times = [time.time()] * Downloader.HISTORY_SIZE
    Downloader.sizes = [0] * Downloader.HISTORY_SIZE
    Downloader.index = 0

  def download(self):
    print(f"    Configuring SSL context...")
    # Handle SSL certificate configuration
    if self.__insecure_ssl:
      print("    Warning: Using insecure SSL context (certificate verification disabled)")
      ssl_context = ssl._create_unverified_context()
    else:
      try:
        # Try to use certifi bundle if available
        import certifi
        print(f"    Using certifi SSL certificates")
        ssl_context = ssl.create_default_context(cafile=certifi.where())
      except ImportError:
        # Fall back to system certificates
        print(f"    Using system SSL certificates")
        ssl_context = ssl.create_default_context()

    # Install SSL context
    print(f"    Setting up HTTP handler...")
    https_handler = request.HTTPSHandler(context=ssl_context)
    opener = request.build_opener(https_handler)
    opener.addheaders = [("User-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/123.0.0.0 Safari/537.36")]
    request.install_opener(opener)

    print(f"    Initiating download...")
    try:
      request.urlretrieve(self.__url, self.__target_path, Downloader.reporthook)
    except ssl.SSLError as e:
      print(f"\nSSL Certificate Error: {e}")
      print("\nThis error occurs when SSL certificates cannot be verified.")
      print("Solutions:")
      print("1. Set SSL_CERT_FILE environment variable:")
      print("   export SSL_CERT_FILE=$(python -c \"import certifi; print(certifi.where())\")")
      print("2. Use --insecure-ssl flag (not recommended for production):")
      print("   python src/python/initial_setup.py -download --insecure-ssl")
      print("3. Update your system's CA certificates")
      raise

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
  parser.add_argument(
    "--insecure-ssl",
    action="store_true",
    help="Skip SSL certificate verification (use only if you encounter SSL certificate errors)",
  )
  args = parser.parse_args()
  runSetup(args.download, args.insecure_ssl)

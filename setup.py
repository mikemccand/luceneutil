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

import os
import sys
import urllib
import shutil

BASE_URL = 'http://people.apache.org/~mikemccand'
DATA_FILES = [
  'enwiki-20120502-lines-1k.txt.lzma',
  'wikimedium500.tasks'
  ]
USAGE= """
Usage: python setup.py [-download]

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
  data_dir = os.path.join(parent, 'data')
  idx_dir = os.path.join(parent, 'indices')

  if not os.path.exists(data_dir):
    print 'create data directory at %s' % (data_dir)
    os.mkdir(data_dir)
  else:
    print 'data directory already exists %s' % (data_dir)

  if not os.path.exists(idx_dir):
    os.mkdir(idx_dir)
    print 'create indices directory at %s' % (idx_dir)
  else:
    print 'indices directory already exists %s' % (idx_dir)

  local_const = os.path.join(cwd, 'localconstants.py')
  if not os.path.exists(local_const):
    f = open(local_const, 'w')
    try:
      f.write(DEFAULT_LOCAL_CONST % ({'base_dir' : parent, 'cwd' : base}))
    finally:
      f.close()
  else:
    print 'localconstants.py already exists - skipping'

  local_run = os.path.join(cwd, 'localrun.py')
  example = os.path.join(cwd, 'example.py')
  if not os.path.exists(local_run):
    shutil.copyfile(example, local_run)
  else:
    print 'localrun.py already exists - skipping'
    
  if download:
    for filename in DATA_FILES:
      url = '%s/%s' % (BASE_URL, filename)
      target_file = os.path.join(data_dir, filename)
      if os.path.exists(target_file):
        print 'file %s already exists - skipping' % (target_file)
      else:
        print 'download ', url, ' - time might take a long time!'
        urllib.urlretrieve(url, filename=target_file)
        print 'downloading %s to  %s done ' % (url, target_file)
      if target_file.endswith('.bz2') or target.endswith('.lzma'):
        print 'NOTE: make sure you decompress %s' % (target_file)

  print 'setup successful'
    
if __name__ == '__main__':
  if '-help' in sys.argv or '--help' in sys.argv:
    print USAGE
  else:
    download = '-download' in sys.argv
    runSetup(download)


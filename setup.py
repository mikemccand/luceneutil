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

BASE_URL = 'http://people.apache.org/~mikemccand'
DATA_FILES = ['enwiki-20100302-pages-articles-lines-1k.txt.bz2', 'wikimedium500.tasks']
USAGE= """
Usage: python setup.py [-download, [-prepareTrunk]]

Options:
  -download downloads a 5GB linedoc file 
  -prepareTrunk checks out a lucene trunk into ../trunk

"""
def runSetup(download, prepare_trunk):    
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
      f.write("BASE_DIR = %s" % parent)
    finally:
      f.close()
  else:
    print 'localconstants.py already exists'
    
  if download:
    for filename in DATA_FILES:
      url = '%s/%s' % (BASE_URL, filename)
      target_file = os.path.join(data_dir, filename)
      if os.path.exists(target_file):
        print 'file %s already exists - skipping' % (target_file)
      else:
        print 'download ', url, ' - time might take a long time!'
        download = urllib.urlretrieve(url, filename=target_file)
        print 'downloading %s to  %s done ' % (url, target_file)
      if target_file.endswith('bz2'):
        print 'NOTE: make sure you decompress %s' % (target_file)
  if prepare_trunk:
    trunk_dir = os.path.join(parent, 'trunk')
    if os.path.exists(trunk_dir):
      print 'trunk dir already exists %s' % (trunk_dir)
    else:
      print 'check out lucene trunk at %s' % (trunk_dir)
      supported = False
      try:
        import pysvn
        supported = True 
      except ImportError:
        print 'module pysvn not available can not prepare trunk - see http://pysvn.tigris.org/ for details'
        print 'run svn command instead: svn checkout https://svn.apache.org/repos/asf/lucene/dev/trunk %s' % (trunk_dir)
      if supported:
        client = pysvn.Client()
        client.checkout('https://svn.apache.org/repos/asf/lucene/dev/trunk', trunk_dir)

  print 'setup successful'
    
if __name__ == '__main__':
  if '-help' in sys.argv or '--help' in sys.argv:
    print USAGE
  else:
    prepare_trunk = '-prepareTrunk' in sys.argv
    download = '-download' in sys.argv
    runSetup(download, prepare_trunk)


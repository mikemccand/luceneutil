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

import traceback
import sys
import os
import smtplib
import datetime
import time
import re

import benchUtil
import competition
try:
  import localpass
except ImportError:
  localpass = None
import constants

reSVNRev = re.compile(r'revision (.*?)\.')
REAL = True

if '-debug' in sys.argv:
  DEBUG = True
  sys.argv.remove('-debug')
else:
  DEBUG = False

if len(sys.argv) > 1:
  NIGHTLY_DIR = sys.argv[1]
elif DEBUG:
  NIGHTLY_DIR = 'clean2.svn'
else:
  NIGHTLY_DIR = 'trunk.nightly'

def now():
  return datetime.datetime.now()

def toSeconds(td):
  return td.days * 86400 + td.seconds + td.microseconds/1000000.

def message(s):
 print '[%s] %s' % (now(), s)

def runCommand(command):
  if REAL:
    message('RUN: %s' % command)
    t0 = time.time()
    if os.system(command):
      message('  FAILED')
      raise RuntimeError('command failed: %s' % command)
    message('  took %.1f sec' % (time.time()-t0))
  else:
    message('WOULD RUN: %s' % command)

def main():
  
  if True:
    os.chdir(constants.BENCH_BASE_DIR)
    for i in range(30):
      try:
        runCommand('hg pull -u > hgupdate.log');
      except RuntimeError:
        message('  retry...')
        time.sleep(60.0)
      else:
        s = open('hgupdate.log', 'r').read()
        if s.find('not updating') != -1:
          raise RuntimeError('hg did not update: %s' % s)
        else:
          break
    else:
      raise RuntimeError('failed to run hg pull -u')

    os.chdir('%s/%s' % (constants.BASE_DIR, NIGHTLY_DIR))

    runCommand('svn cleanup')
    open('update.log', 'ab').write('\n\n[%s]: update' % datetime.datetime.now())
    for i in range(30):
      try:
        runCommand('svn update > update.log 2>&1')
      except RuntimeError:
        message('  retry...')
        time.sleep(60.0)
      else:
        svnRev = int(reSVNRev.search(open('update.log', 'rb').read()).group(1))
        print 'SVN rev is %s' % svnRev
        break
    else:
      raise RuntimeError('svn update failed')

  runCommand('%s clean > clean.log 2>&1' % constants.ANT_EXE)
  runCommand('%s compile > compile.log 2>&1' % constants.ANT_EXE)

  MEDIUM_LINE_FILE = constants.NIGHTLY_MEDIUM_LINE_FILE
  MEDIUM_INDEX_NUM_DOCS = constants.NIGHTLY_MEDIUM_INDEX_NUM_DOCS

  mediumSource = competition.Data('wikimedium',
                                  MEDIUM_LINE_FILE,
                                  MEDIUM_INDEX_NUM_DOCS,
                                  constants.WIKI_MEDIUM_TASKS_FILE)

  comp = competition.Competition()
  index = comp.newIndex(NIGHTLY_DIR, mediumSource)
  c = comp.competitor(id, NIGHTLY_DIR, index=index)
  r = benchUtil.RunAlgs(constants.JAVA_COMMAND, True, True)
  r.compile(c)

def sendEmail(emailAddress, message):
  if localpass is None:
    print 'WARNING: no smtp password; skipping email'
    return
  SMTP_SERVER = localpass.SMTP_SERVER
  SMTP_PORT = localpass.SMTP_PORT
  FROM_EMAIL = 'admin@mikemccandless.com'
  smtp = smtplib.SMTP(SMTP_SERVER, port=SMTP_PORT)
  smtp.ehlo(FROM_EMAIL)
  smtp.starttls()
  smtp.ehlo(FROM_EMAIL)
  localpass.smtplogin(smtp)
  smtp.sendmail(FROM_EMAIL, emailAddress.split(','), message)
  smtp.quit()

try:
  main()
except:
  traceback.print_exc()
  if localpass is not None and not DEBUG:
    emailAddr = 'mail@mikemccandless.com'
    message = 'From: %s\r\n' % localpass.FROM_EMAIL
    message += 'To: %s\r\n' % emailAddr
    message += 'Subject: Compile trunk.nightly FAILED!!\r\n'
    sendEmail(emailAddr, message)


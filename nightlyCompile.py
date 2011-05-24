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
import localpass
import constants

reSVNRev = re.compile(r'revision (.*?)\.')
REAL = True

DEBUG = '-debug' in sys.argv

if DEBUG:
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
  
  os.chdir('/lucene/%s' % NIGHTLY_DIR)

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

  runCommand('ant clean > clean.log 2>&1')
  runCommand('ant compile > compile.log 2>&1')

  comp = competition.Competition()
  index = comp.newIndex(NIGHTLY_DIR)
  c = comp.competitor(id, NIGHTLY_DIR)
  c.withIndex(index)
  r = benchUtil.RunAlgs(constants.JAVA_COMMAND)
  r.compile(c.build())

def sendEmail(emailAddress, message):
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
  if not DEBUG:
    emailAddr = 'mail@mikemccandless.com'
    message = 'From: %s\r\n' % localpass.FROM_EMAIL
    message += 'To: %s\r\n' % emailAddr
    message += 'Subject: Compile trunk.nightly FAILED!!\r\n'
    sendEmail(emailAddr, message)


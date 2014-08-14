import sys
import constants
import time
import os
import common
import subprocess

# Just runs "ant test" from Lucene dir on a remote machine after rsync:
USERNAME = 'mike'

hostName = sys.argv[1]
rootDir = common.findRootDir(os.getcwd())
t = time.time()
cmd = '/usr/bin/rsync --delete -rtS %s -e "ssh -x -c arcfour -o Compression=no" --exclude="build/core/test" --exclude=".#*" --exclude="C*.events" --exclude=.svn/ --exclude="*.log" %s@%s:%s' % \
      (rootDir, USERNAME, hostName, constants.BASE_DIR)
print(cmd)
if os.system(cmd):
  raise RuntimeError('failed')
print('  done [%.1f sec]' % (time.time()-t))
print('killall java...')
os.system('ssh %s@%s killall java' % (USERNAME, hostName))
cmd = 'ssh %s@%s "cd %s; ant test"' % (USERNAME, hostName, rootDir)
print('cmd: %s' % cmd)
checkout = rootDir[len(constants.BASE_DIR)+1:]
print('checkout: %s' % checkout)
p = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
while True:
  line = p.stdout.readline()
  if line == '':
    break
  print(line.rstrip().replace('   [junit4]', '[%s:%s] ' % (hostName, checkout)))
  sys.stdout.flush()

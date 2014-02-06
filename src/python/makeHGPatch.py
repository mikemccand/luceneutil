import shutil
import os
import sys
import check

if '-r' in sys.argv:
  idx = sys.argv.index('-r')
  rev = int(sys.argv[idx+1])
  del sys.argv[idx:idx+2]
else:
  rev = None

checkout = os.getcwd().split('/')[2]
issueNumber = sys.argv[1]

def run(command):
  if os.system('%s' % command):
    raise RuntimeError('%s failed' % command)

os.chdir('/lucene/%s' % checkout)
isHG = os.path.exists('.hg')
if isHG:

  if rev is None:
    run('hg diff > diffsHG.x')
  else:
    run('hg diff -r %s > diffsHG.x' % rev)

  f = open('diffsHG.x', 'rb')
  fOut = open('diffs.x', 'wb')
  while True:
    line = f.readline()
    if line == '':
      break
    if line.startswith('diff -r'):
      fOut.write('Index: %s' % line[21:])
    else:
      line = line.replace('--- a/', '--- ')
      line = line.replace('+++ b/', '+++ ')
      fOut.write(line)
  f.close()
  fOut.close()
else:
  run('svn diff > diffs.x')

if '-check' in sys.argv:
  check.main('diffs.x')

patchOut = '/x/tmp/patch/LUCENE-%s.patch' % issueNumber
shutil.copy2('diffs.x', patchOut)
print 'Saved patch to %s' % patchOut

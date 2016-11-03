import datetime
import os
import mailbox
import re

'''
Silly tool that parses the email archive to lucene-commits, to find
which commit hashes the master branch has pointed to over time.
It is annoying that git does not seem to preserve this information
itself.
'''

# Get the lucene-commits mbox archives, e.g.:
#
#   wget http://mail-archives.apache.org/mod_mbox/lucene-commits/201611.mbox

reMaster = re.compile('^  refs/heads/master ([0-9a-f]+) -> ([0-9a-f]+)$', re.MULTILINE)

# master commit hashes pushed
masterCommits = set()

for month in 8, 9, 10, 11:

  m = mailbox.mbox('2016%02d.mbox' % month)

  for message in m:
    p = message.get_payload()
    if type(p) is str:
      p = [p]
    for x in p:
      matches = reMaster.findall(str(x))
      for fromHash, toHash in matches:
        #print("fromHash %s" % fromHash)
        masterCommits.add(toHash)

masterCommitsAndTimes = []

with os.popen('git log --format=fuller --parents', 'r') as f:

  line = f.readline().strip()

  commits = {}
  childCount = {}
  firstCommit = None

  while True:
    #print("got: %s" % line)
    tup = line.split()
    if tup[0] != 'commit':
      raise RuntimeError('expected commit but saw %s' % line)

    hash = tup[1]
    if hash in commits:
      raise RuntimeError('duplicate commit hash %s' % hash)
    parentHashes = tup[2:]
    if len(parentHashes) not in (0, 1, 2):
      raise RuntimeError('expected 0 or 1 or 2 parents but saw %s' % line)
    author = f.readline().strip()
    if author.startswith('Merge: '):
      author = f.readline().strip()
    authorDate = f.readline().strip()
    commitUser = f.readline().strip()
    commitDate = f.readline()[11:].strip()
    comments = []

    commits[hash] = (commitUser, commitDate, parentHashes, comments)

    f.readline()
    while True:
      line = f.readline()
      if line == '':
        break
      if line.rstrip().startswith('commit '):
        break
      comments.append(line)

    if firstCommit is None:
      firstCommit = hash

    if hash[:9] in masterCommits:
      # parse commit time, and convert to UTC
      t = datetime.datetime.strptime(commitDate, '%a %b %d %H:%M:%S %Y %z')
      t = datetime.datetime(*t.utctimetuple()[:6])
      masterCommitsAndTimes.append((t, hash))

    if line == '':
      break

print('found %d commit + times vs %d commits only' % (len(masterCommitsAndTimes), len(masterCommits)))

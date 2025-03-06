import os

with os.popen("git log --format=fuller --parents", "r") as f:
  line = f.readline().strip()

  commits = {}
  childCount = {}
  firstCommit = None

  while True:
    # print("got: %s" % line)
    tup = line.split()
    if tup[0] != "commit":
      raise RuntimeError("expected commit but saw %s" % line)

    hash = tup[1]
    if hash in commits:
      raise RuntimeError("duplicate commit hash %s" % hash)
    parentHashes = tup[2:]
    if len(parentHashes) not in (0, 1, 2):
      raise RuntimeError("expected 0 or 1 or 2 parents but saw %s" % line)
    author = f.readline().strip()
    if author.startswith("Merge: "):
      author = f.readline().strip()
    authorDate = f.readline().strip()
    commitUser = f.readline().strip()
    commitDate = f.readline()[11:].strip()
    comments = []

    commits[hash] = (commitUser, commitDate, parentHashes, comments)
    for parentHash in parentHashes:
      childCount[parentHash] = childCount.get(parentHash, 0) + 1

    f.readline()
    while True:
      line = f.readline()
      if line == "":
        break
      if line.rstrip().startswith("commit "):
        break
      comments.append(line)

    if firstCommit is None:
      firstCommit = hash

    if line == "":
      break

print("%d commits" % len(commits))

if False:
  for hash, count in childCount.items():
    if count > 1:
      print("hash %s has childCount=%s" % (hash, count))

hash = firstCommit
while True:
  commitUser, commitDate, parentHashes, comments = commits[hash]
  print("%s %s: %s" % (commitDate, commitUser, hash))
  if childCount.get(hash, 0) > 1:
    print("  ****")
  if "Merge branch 'master' of" in "".join(comments):
    print("  xxxx")
  if len(parentHashes) > 1:
    print("  %d parents; pick first" % len(parentHashes))
    hash = parentHashes[1]
  else:
    hash = parentHashes[0]

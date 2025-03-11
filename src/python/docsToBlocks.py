import sys

pending = []
pendingDocCount = 0
pendingByteCount = 0


def writePending(fOut):
  global pendingDocCount
  global pendingByteCount
  fOut.write(("%d %d\n" % (pendingDocCount, pendingByteCount)).encode("utf-8"))
  fOut.write(b"".join(pending))
  pending.clear()
  pendingDocCount = 0
  pendingByteCount = 0


# python3 -u /l/util/src/python/docsToBlocks.py /lucenedata/geonames/documents.json /l/data/geonames.luceneserver.blocks

with open(sys.argv[1]) as f, open(sys.argv[2], "wb") as fOut:
  first = True
  while True:
    l = f.readline()
    if len(l) == 0:
      break

    bytes = ('{"fields": %s}' % l.rstrip()).encode("utf-8")
    if not first:
      pending.append(b",")
      pendingByteCount += 1

    first = False

    pending.append(bytes)
    pendingDocCount += 1
    pendingByteCount += len(bytes)

    if pendingByteCount > 128 * 1024:
      writePending(fOut)

  if pendingByteCount > 0:
    writePending(fOut)

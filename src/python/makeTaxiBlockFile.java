pending = []
pendingByteCount = 0
with open('/lucenedata/nyc-taxi-data/alltaxis.csv', 'rb') as f, open('/lucenedata/nyc-taxi-data/alltaxis.csv.blocks', 'wb') as fOut:
  while True:
    line = f.readline()
    if line == b'':
      fOut.write(('%d %d\n' % (pendingByteCount, len(pending))).encode('utf-8'))
      fOut.write(b''.join(pending))
      pending.clear()
      break
    pending.append(line)
    pendingByteCount += len(line)

    if pendingByteCount > 256*1024:
      fOut.write(('%d %d\n' % (pendingByteCount, len(pending))).encode('utf-8'))
      fOut.write(b''.join(pending))
      pending.clear()
      pendingByteCount = 0

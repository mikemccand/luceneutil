import datetime
import io
import struct
import sys

# python3 -u src/python/buildBinaryLineDocs.py /lucenedata/enwiki/enwiki-20110115-lines-1k-fixed.txt /l/data/enwiki-20110115-lines-1k-fixed.bin

epoch = datetime.datetime.utcfromtimestamp(0)


def flush(pending, pendingDocCount, fOut):
  fOut.write(struct.pack("i", pendingDocCount))
  fOut.write(struct.pack("i", pending.tell()))
  fOut.write(pending.getbuffer())


print(f"build binary file from {sys.argv[1]} to {sys.argv[2]}")

with open(sys.argv[1], encoding="utf-8") as f, open(sys.argv[2], "wb") as fOut:
  first = True
  pending = io.BytesIO()
  pendingDocCount = 0
  while True:
    line = f.readline()
    if len(line) == 0:
      break
    if first:
      # skip header
      first = False
      if line.startswith("FIELDS_HEADER_INDICATOR"):
        print("skip header")
        if len(line.strip().split("\t")) != 5:
          raise RuntimeError("can only convert line doc files that have header title, timestamp, body text, random label: saw header %s" % line.rstrip())
        continue
      else:
        print("no header")
    tup = line.split("\t")
    if len(tup) != 4:
      raise RuntimeError("got %s" % str(tup))
    if False:
      for s in tup:
        if not s.strip():
          raise RuntimeError("contained empty category %s" % str(tup))
    title, date, body, randomLabel = tup

    dt = datetime.datetime.strptime(date.replace(".000", ""), "%d-%b-%Y %H:%M:%S")
    msecSinceEpoch = int((dt - epoch).total_seconds() * 1000)

    timeSec = dt.hour * 3600 + dt.minute * 60 + dt.second
    titleBytes = title.encode("utf-8")
    bodyBytes = body.encode("utf-8")
    randomLabelBytes = randomLabel.strip().encode("utf-8")
    totalLength = len(titleBytes) + len(bodyBytes) + len(randomLabelBytes) + 20
    # print('len=%s' % totalLength)
    # print('HERE: %s, offset=%s' % (struct.pack('i', totalLength), fOut.tell()))
    pending.write(struct.pack("iiiil", len(titleBytes), len(bodyBytes), len(randomLabelBytes), timeSec, msecSinceEpoch))
    pending.write(titleBytes)
    pending.write(bodyBytes)
    pending.write(randomLabelBytes)
    pendingDocCount += 1

    if pending.tell() > 64 * 1024:
      # print('%d docs in %.1f KB chunk' % (pendingDocCount, pending.tell()/1024.))
      flush(pending, pendingDocCount, fOut)
      pending = io.BytesIO()
      pendingDocCount = 0

  if pending.tell() > 0:
    flush(pending, pendingDocCount, fOut)

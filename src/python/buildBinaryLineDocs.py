import struct
import sys
import datetime
import io

# python3 -u src/python/buildBinaryLineDocs.py /lucenedata/enwiki/enwiki-20110115-lines-1k-fixed.txt /l/data/enwiki-20110115-lines-1k-fixed.bin 

epoch = datetime.datetime.utcfromtimestamp(0)

def flush(pending, pendingDocCount, fOut):
  fOut.write(struct.pack('i', pendingDocCount))
  fOut.write(struct.pack('i', pending.tell()))
  fOut.write(pending.getbuffer())

with open(sys.argv[1], 'r', errors='replace') as f, open(sys.argv[2], 'wb') as fOut:
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
      if line.startswith('FIELDS_HEADER_INDICATOR'):
        print('skip header')
        if len(line.strip().split('\t')) != 4:
          raise RuntimeError('cannot convert line doc files that have more than title, timestamp, text fields: saw header %s' % line.rstrip())
        continue
      else:
        print('no header')
    tup = line.split('\t')
    if len(tup) != 3:
      raise RuntimeError('got %s' % str(tup))
    title, date, body = tup

    dt = datetime.datetime.strptime(date.replace('.000', ''), '%d-%b-%Y %H:%M:%S')
    msecSinceEpoch = int((dt - epoch).total_seconds() * 1000)

    timeSec = dt.hour*3600 + dt.minute * 60 + dt.second
    titleBytes = title.encode('utf-8')
    bodyBytes = body.encode('utf-8')
    totalLength = len(titleBytes)+len(bodyBytes)+16
    #print('len=%s' % totalLength)
    #print('HERE: %s, offset=%s' % (struct.pack('i', totalLength), fOut.tell()))

    pending.write(struct.pack('iili', len(titleBytes), len(bodyBytes), msecSinceEpoch, timeSec))
    pending.write(titleBytes)
    pending.write(bodyBytes)
    pendingDocCount += 1
    
    if pending.tell() > 64*1024:
      #print('%d docs in %.1f KB chunk' % (pendingDocCount, pending.tell()/1024.))
      flush(pending, pendingDocCount, fOut)
      pending = io.BytesIO()
      pendingDocCount = 0

  if pending.tell() > 0:
    flush(pending, pendingDocCount, fOut)

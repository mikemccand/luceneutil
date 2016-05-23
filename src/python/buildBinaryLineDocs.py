import struct
import sys
import datetime

epoch = datetime.datetime.utcfromtimestamp(0)

with open(sys.argv[1], 'r', errors='replace') as f, open(sys.argv[2], 'wb') as fOut:
  # skip header
  first = True
  while True:
    line = f.readline()
    if len(line) == 0:
      break
    if first:
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
    
    fOut.write(struct.pack('iili', totalLength, len(titleBytes), msecSinceEpoch, timeSec))
    fOut.write(titleBytes)
    fOut.write(bodyBytes)

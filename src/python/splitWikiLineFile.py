import sys

f = open(sys.argv[1], 'rb')
fOut = open(sys.argv[2], 'wb')
numCharLimit = int(sys.argv[3])

leftover = ''
count = 0

while True:
  count += 1
  if leftover == '':
    l = f.readline()
    if l == '':
      break
    l = l.strip().split('\t', 2)
    if len(l) != 3:
      print 'failed to get 3 elems: %d, count %s, line %s; skipping' % (len(l), count, l)
      continue
    else:
      title = l[0]
      date = l[1]
      leftover = l[2]

  if len(leftover) <= numCharLimit:
    fOut.write('%s\t%s\t%s\n' % (title, date, leftover))
    leftover = ''
  else:
    spot = numCharLimit
    while spot >= 0 and leftover[spot] != ' ':
      spot -= 1

    if spot == 0:
      spot = numCharLimit

    chunk = leftover[:spot]
    fOut.write('%s\t%s\t%s\n' % (title, date, chunk))
    leftover = leftover[spot:]

print '%d lines' % count

f.close()
fOut.close()
      

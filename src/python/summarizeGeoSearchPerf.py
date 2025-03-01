import os

results = {}
fields = set()
print()
print('indexing:')
print()
print('PrecStep        Size        IndexTime')
for precStep in 4, 8, 16:

  with open('geo.index%d.log' % precStep) as f:

    lastSec = None
    for line in f.readlines():
      tup = line.split(':')
      if len(tup) == 2:
        lastSec = float(tup[1].split()[0])
    mb = float(os.popen('du -s /l/scratch/indices/geonames%s' % precStep).readline().split()[0])/1024.
    print('   %5s   %6.1f MB   %10.1f sec' % (precStep, mb, lastSec))

  print()
  totTerms = 0
  with open('search.geo.%dprecstep.txt' % precStep) as f:
    field = None
    for line in f.readlines():
      line = line.strip()

      if line.startswith('field='):
        field = line[6:].strip()
        fields.add(field)
      if line.startswith('msec='):
        msec = float(line[5:])
        results[(field,precStep)] = msec, totTerms
      if line.startswith('tot term rewrites='):
        totTerms = int(line[18:])

print()
print()
print('searching:')
print()
print('     Field  PrecStep   QueryTime   TermCount')
for field in fields:
  for precStep in 4, 8, 16:
    msec, totTerms = results[(field, precStep)]
    print('%10s  %8d  %7.1f ms  %10d' % (field, precStep, msec, totTerms))

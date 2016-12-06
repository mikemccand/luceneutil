import sys
import re

r = re.compile(r': (\d+) msec to (?:merge|write) points')
tot = 0
with open(sys.argv[1]) as f:
  for line in f.readlines():
    m = r.search(line)
    if m is not None:
      #print(line)
      tot += int(m.group(1))
print('total: %.3f sec' % (tot/1000.))

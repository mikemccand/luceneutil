import os
import datetime
import re

reDateTime = re.compile('^(\d\d\d\d).(\d\d).(\d\d).\d\d.\d\d.\d\d$')

mostRecent = None

for f in os.listdir('/lucene/logs.nightly'):
  if os.path.exists('/lucene/logs.nightly/%s/results.pk' % f):
    m = reDateTime.match(f)
    if m is not None:
      d = datetime.date(year=int(m.group(1)),
                        month=int(m.group(2)),
                        day=int(m.group(3)))
      if mostRecent is None or d > mostRecent:
        mostRecent = d

print('most recent %s' % mostRecent)

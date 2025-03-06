import datetime
import os
import re

reDateTime = re.compile("^(\d\d\d\d).(\d\d).(\d\d).(\d\d).(\d\d).(\d\d)$")

mostRecent = None

for f in os.listdir("/lucene/logs.nightly"):
  if os.path.exists("/lucene/logs.nightly/%s/results.pk" % f):
    m = reDateTime.match(f)
    if m is not None:
      d = datetime.datetime(year=int(m.group(1)), month=int(m.group(2)), day=int(m.group(3)), hour=int(m.group(4)), minute=int(m.group(5)), second=int(m.group(6)))
      if mostRecent is None or d > mostRecent:
        mostRecent = d

print(repr(mostRecent))

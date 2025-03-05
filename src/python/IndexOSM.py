import time

import rtree


def build():
  with open("/lucenedata/open-street-maps/latlon.subsetPlusAllLondon.txt", "rb") as f:
    count = 0
    while True:
      line = f.readline()
      if len(line) == 0:
        break
      id, lat, lon = line.strip().split(",")
      lat = float(lat)
      lon = float(lon)
      count += 1
      if count % 1000000 == 0:
        print("%d..." % count)
      yield int(id), (lat, lon, lat, lon), None


t0 = time.time()
p = rtree.index.Property()
p.filename = "theindex"
p.overwrite = True
p.storage = rtree.index.RT_Disk
p.dimension = 2

# 17.5 sec
# p.variant = rtree.index.RT_Star
# p.fill_factor = 0.4

# 17.4 sec:
# p.variant = rtree.index.RT_Quadratic
# p.fill_factor = 0.4

# 17.5 sec:
# p.variant = rtree.index.RT_Linear
# p.fill_factor = 0.4

# 34 sec:
# p.leaf_capacity = 1000

# 18.2 sec:
# p.fill_factor = 0.9

rtree.index.Index(p.filename, build(), properties=p, interleaved=True, overwrite=True)
print("%.3f sec to index" % (time.time() - t0))

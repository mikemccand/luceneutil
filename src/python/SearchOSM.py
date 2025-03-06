import time

import rtree

p = rtree.index.Property()
p.filename = "theindex"
p.overwrite = False
p.storage = rtree.index.RT_Disk
p.variant = rtree.index.RT_Linear
p.dimension = 2

index = rtree.index.Rtree(p.filename, properties=p)

# London, UK:
STEPS = 5
MIN_LAT = 51.0919106
MAX_LAT = 51.6542719
MIN_LON = -0.3867282
MAX_LON = 0.8492337

# for lon in range(-180, 175, 10):
#  for lat in range(-90, 85, 10):
#    for width in range(10, 355, 10):
#      for height in range(10, 175, 10):

for iter in range(100):
  tStart = time.time()
  totHits = 0
  queryCount = 0
  for latStep in range(STEPS):
    lat = MIN_LAT + latStep * (MAX_LAT - MIN_LAT) / STEPS
    for lonStep in range(STEPS):
      lon = MIN_LON + lonStep * (MAX_LON - MIN_LON) / STEPS
      for latStepEnd in range(latStep + 1, STEPS + 1):
        latEnd = MIN_LAT + latStepEnd * (MAX_LAT - MIN_LAT) / STEPS
        for lonStepEnd in range(lonStep + 1, STEPS + 1):
          lonEnd = MIN_LON + lonStepEnd * (MAX_LON - MIN_LON) / STEPS
          # print("LON: %.1f to %.1f, LAT: %.1f to %.1f" % (lon, lonEnd, lat, latEnd))
          # t0 = time.time()
          count = index.count((lat, lon, latEnd, lonEnd))
          totHits += count
          queryCount += 1
          # print('  %d total hits' % count)
          # print('  %.1f msec' % (1000*(time.time()-t0)))
  tEnd = time.time()
  print("ITER: %s, %.2f sec; totHits=%d; %d queries" % (iter, tEnd - tStart, totHits, queryCount))

import json

geoIDs = {}

with open('/x/tmp/downloads/shapes_simplified_low.txt') as f:
  f.readline()
  for line in f.readlines():
    tup = line.strip().split('\t')
    assert len(tup) == 2

    geoID, s = tup
    polys = []
    geoIDs[geoID] = [None, polys]

    shape = json.loads(s)
    coords = shape['coordinates']
    
    if shape['type'] == 'Polygon':
      #print('%s poly: %s' % (geoID, len(coords[0])))
      l = []
      polys.append(l)
      for lon, lat in coords[0]:
        l.append((lat, lon))
    elif shape['type'] == 'MultiPolygon':
      #print('%s multi: %s' % (geoID, len(coords)))
      for poly in coords:
        l = []
        polys.append(l)
        for lon, lat in poly[0]:
          l.append((lat, lon))
    else:
      assert false, 'got type %s' % shape['type']

# Give names to the geo ids:
with open('/lucenedata/geonames/allCountries.txt') as f:
  
  for line in f.readlines():
    tup = line.split('\t')
    if tup[0] in geoIDs:
      # print('%s -> %s' % (geoID, tup[1]))
      geoIDs[tup[0]][0] = tup[1]

count = 0

for geoID, (name, polys) in geoIDs.items():
  print
  print('// %s: %s: %d polys' % (geoID, name, len(polys)))
  print('BooleanQuery.Builder b%d = new BooleanQuery.Builder();' % count)
  for poly in polys:
    print('b%d.add(LatLonPoint.newPolygonQuery("point", new double[] {%s}, new double[] {%s}), BooleanClause.Occur.SHOULD);' % \
          (count, ', '.join(str(x[0]) for x in poly), ', '.join(str(x[1]) for x in poly)))
  print('BooleanQuery q%d = b%d.build();' % (count, count))
  count += 1

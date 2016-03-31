import json

# get shapes_simplified_low.zip from: http://download.geonames.org/export/dump/

# geojson: http://geojson.org/geojson-spec.html

# nice poly explanation: http://esri.github.io/geometry-api-java/doc/Polygon.html

# rm LatLonPointPoly*.java; python src/python/geoJSONToJava.py ; javac -cp /l/trunk/lucene/build/core/lucene-core-7.0.0-SNAPSHOT.jar:/l/trunk/lucene/build/sandbox/lucene-sandbox-7.0.0-SNAPSHOT.jar LatLonPointPoly*.java

geoIDs = {}

with open('/l/util/src/python/shapes_simplified_low.txt') as f:
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
      for subPoly in coords:
        l2 = []
        l.append(l2)
        for lon, lat in subPoly:
          l2.append((lat, lon))
    elif shape['type'] == 'MultiPolygon':
      #print('%s multi: %s' % (geoID, len(coords)))
      for poly in coords:
        l = []
        polys.append(l)
        for subPoly in poly:
          l2 = []
          l.append(l2)
          for lon, lat in subPoly:
            l2.append((lat, lon))
    else:
      assert false, 'got type %s' % shape['type']

# Give names to the geo ids:
with open('/lucenedata/geonames/allCountries.txt') as f:
  
  for line in f.readlines():
    tup = line.split('\t')
    if tup[0] in geoIDs:
      # print('%s -> %s' % (geoID, tup[1]))
      geoIDs[tup[0]][0] = tup[1]

f = open('/l/util/src/python/shapes_simplified_low.out.txt', 'w')
for geoID, (name, polys) in geoIDs.items():
  f.write('count=%d %s %s\n' % (len(polys), name, geoID))
  for poly in polys:
    f.write('  poly count=%d\n' % len(poly))
    for subPoly in poly:
      f.write('    vertex count=%d\n' % len(subPoly))
      f.write('      lats %s\n' % ' '.join(str(x[0]) for x in subPoly))
      f.write('      lons %s\n' % ' '.join(str(x[1]) for x in subPoly))
f.close()

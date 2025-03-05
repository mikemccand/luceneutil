import json
import sys

s = open(sys.argv[1]).read()
print("len %s" % len(s))

d = json.loads(s)

features = d["features"]
if len(features) != 1:
  raise RuntimeError("has %d features" % len(features))

if features[0]["type"] != "Feature":
  raise RuntimeError("type should be Feature")

t = features[0]["geometry"]["type"]

if t not in ("Polygon", "MultiPolygon"):
  raise RuntimeError("Feature type should be Polygon or MultiPolygon but got %s" % t)

coords = features[0]["geometry"]["coordinates"]

if t == "MultiPolygon":
  print("  multi polygon has %d polygons" % len(coords))
  if len(coords) > 1:
    raise RuntimeError("cannot handle more than one MultiPolygon")
  coords = coords[0]

count = len(coords[0])
holes = len(coords) - 1
tot = 0
for p in coords:
  tot += len(p)

print("%s vertices, %s total, %s holes" % (count, tot, holes))

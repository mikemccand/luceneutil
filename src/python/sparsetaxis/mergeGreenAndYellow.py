import csv
import datetime

headerMap = {
  "vendorid": "vendor_id",
  "ratecodeid": "rate_code_id",
  "store_and_fwd_flag": "store_and_fwd_flag",
  "payment_type": "payment_type",
  "trip_type": "trip_type",
  "tpep_pickup_datetime": "pickup_datetime",
  "tpep_dropoff_datetime": "dropoff_datetime",
  "lpep_pickup_datetime": "pickup_datetime",
  "lpep_dropoff_datetime": "dropoff_datetime",
  "pickup_longitude": "pickup_longitude",
  "pickup_latitude": "pickup_latitude",
  "dropoff_longitude": "dropoff_longitude",
  "dropoff_latitude": "dropoff_latitude",
  "passenger_count": "passenger_count",
  "trip_distance": "trip_distance",
  "fare_amount": "fare_amount",
  "extra": "extra",
  "mta_tax": "mta_tax",
  "tip_amount": "tip_amount",
  "tolls_amount": "tolls_amount",
  "improvement_surcharge": "improvement_surcharge",
  "total_amount": "total_amount",
  "ehail_fee": "ehail_fee",
}


def toDateTime(s):
  return datetime.datetime.strptime(s, "%Y-%m-%d %H:%M:%S")


def pickupTime(color, ride):
  if color == "green":
    idx = greenPickupHeader
  else:
    idx = yellowPickupHeader
  return toDateTime(ride[idx])


def dropoffTime(color, ride):
  if color == "green":
    idx = greenDropoffHeader
  else:
    idx = yellowDropoffHeader
  return toDateTime(ride[idx])


def compareByTime(color1, ride1, color2, ride2):
  pickup1 = pickupTime(color1, ride1)
  pickup2 = pickupTime(color2, ride2)
  if pickup1 < pickup2:
    return -1
  elif pickup1 > pickup2:
    return 1
  else:
    # tie break by dropoff time
    dropoff1 = dropoffTime(color1, ride1)
    dropoff2 = dropoffTime(color2, ride2)
    if dropoff1 < dropoff2:
      return -1
    elif dropoff1 > dropoff2:
      return 1
    else:
      return 0


class TaxiReader:
  def __init__(self, color, globalHeaders):
    self.color = color
    self.year = 2015
    self.month = 4
    self.f = None
    self.globalHeaders = globalHeaders
    self.openNext()

  def openNext(self):
    if self.f is not None:
      self.f.close()

    fileName = "/lucenedata/nyc-taxi-data/%s_tripdata_%04d-%02d.sorted.csv" % (self.color, self.year, self.month)
    self.month += 1
    if self.month == 13:
      self.year += 1
      self.month = 1

    self.f = open(fileName, "r", encoding="utf-8")
    self.reader = csv.reader(self.f)
    headers = next(self.reader)
    self.headersToIndex = []
    for i, header in enumerate(headers):
      header = header.strip().lower()
      header = headerMap[header]
      if header in self.globalHeaders:
        self.headersToIndex.append((i, self.globalHeaders.index(header)))

  def next(self):
    try:
      raw = next(self.reader)
    except StopIteration:
      self.openNext()
      raw = next(self.reader)

    row = [""] * len(self.globalHeaders)
    for src, dest in self.headersToIndex:
      row[dest] = raw[src]
      globalCounts[dest] += 1

    if False:
      print("next for %s:" % self.color)
      for i, s in enumerate(row):
        if s != "":
          print("  %2d: %s -> %s" % (i, globalHeaders[i], s))

    return row


globalHeaders = list(set(headerMap.values()))

# These fields only show up in green, so even in the non-sparse case, they are sparse, so we don't include them:
globalHeaders.remove("trip_type")
globalHeaders.remove("ehail_fee")

globalHeaders.sort()
print("headers:")
for i, header in enumerate(globalHeaders):
  print("%2d: %s" % (i, header))

globalCounts = [0] * len(globalHeaders)

greenPickupHeader = yellowPickupHeader = globalHeaders.index("pickup_datetime")
greenDropoffHeader = yellowDropoffHeader = globalHeaders.index("dropoff_datetime")

print("greenPickupHeader=%s" % greenPickupHeader)
print("greenDropoffHeader=%s" % greenDropoffHeader)
print("yellowPickupHeader=%s" % yellowPickupHeader)
print("yellowDropoffHeader=%s" % yellowDropoffHeader)

green = TaxiReader("green", globalHeaders)
yellow = TaxiReader("yellow", globalHeaders)

fileNameOut = "20M.mergedSubset.csv"

with open("/lucenedata/nyc-taxi-data/%s" % fileNameOut, "w", encoding="utf-8") as fOut:
  fOut.write(",".join(globalHeaders) + "\n")

  nextGreenRide = green.next()
  nextYellowRide = yellow.next()

  docCount = 0
  yellowCount = 0
  greenCount = 0

  while docCount < 20000000:
    if compareByTime("green", nextGreenRide, "yellow", nextYellowRide) <= 0:
      fOut.write("g:" + (",".join(nextGreenRide) + "\n"))
      x = green.next()
      if compareByTime("green", x, "green", nextGreenRide) < 0:
        raise RuntimeError("green rides are out of order:\n  %s\n  %s" % (nextGreenRide, x))
      nextGreenRide = x
      greenCount += 1
    else:
      fOut.write("y:" + (",".join(nextYellowRide) + "\n"))
      x = yellow.next()
      if compareByTime("yellow", x, "yellow", nextYellowRide) < 0:
        raise RuntimeError("yellow rides are out of order")
      nextYellowRide = x
      yellowCount += 1
    docCount += 1
    if docCount % 100000 == 0:
      print("  %d..." % docCount)

print("%d yellow, %d green" % (yellowCount, greenCount))

sparseness = []
for i in range(len(globalHeaders)):
  count = globalCounts[i]
  sparseness.append((count, "%6.1f%% %s" % (100.0 * count / docCount, globalHeaders[i])))

sparseness.sort(reverse=True)

for ign, s in sparseness:
  print("  %s" % s)

"""
17699826 yellow, 2300174 green
   100.0% vendor_id
   100.0% trip_distance
   100.0% total_amount
   100.0% tolls_amount
   100.0% tip_amount
   100.0% store_and_fwd_flag
   100.0% rate_code_id
   100.0% pickup_longitude
   100.0% pickup_latitude
   100.0% pickup_datetime
   100.0% payment_type
   100.0% passenger_count
   100.0% mta_tax
   100.0% improvement_surcharge
   100.0% fare_amount
   100.0% extra
   100.0% dropoff_longitude
   100.0% dropoff_latitude
   100.0% dropoff_datetime
"""

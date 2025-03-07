import csv
import datetime
import os

"""
Sorts all rides in this csv first by pickup time then by dropoff time.
"""


def sortOneFile(fileName):
  print("sort %s..." % fileName)
  fileNameOut = fileName.replace(".csv", ".sorted.csv")
  if os.path.exists(fileNameOut):
    print("  already done!")
    return
  reader = csv.reader(open(fileName, "r", encoding="utf-8"))
  headers = next(reader)
  pickupTimeIndex = None
  dropoffTimeIndex = None
  for i, header in enumerate(headers):
    header = header.lower()
    if header in ("lpep_pickup_datetime", "tpep_pickup_datetime"):
      if pickupTimeIndex is not None:
        raise RuntimeError("saw two pickup datetime headers=%s" % headers)
      pickupTimeIndex = i
    elif header in ("lpep_dropoff_datetime", "tpep_dropoff_datetime"):
      if dropoffTimeIndex is not None:
        raise RuntimeError("saw two dropoff datetime headers=%s" % headers)
      dropoffTimeIndex = i

  if dropoffTimeIndex is None:
    raise RuntimeError("could not find dropoff time header in headers=%s" % headers)
  if pickupTimeIndex is None:
    raise RuntimeError("could not find pickup time header in headers=%s" % headers)

  l = []
  while True:
    try:
      row = next(reader)
    except StopIteration:
      break
    pickup = datetime.datetime.strptime(row[pickupTimeIndex], "%Y-%m-%d %H:%M:%S")
    dropoff = datetime.datetime.strptime(row[dropoffTimeIndex], "%Y-%m-%d %H:%M:%S")
    l.append((pickup, dropoff, ",".join(row)))
    if len(l) % 100000 == 0:
      print("  %d..." % len(l))

  print("  %d rows, now sort..." % len(l))
  l.sort()
  with open(fileNameOut, "w", encoding="utf-8") as fOut:
    fOut.write(",".join(headers) + "\n")
    for ign, ign, row in l:
      fOut.write(row + "\n")


if __name__ == "__main__":
  for month in range(12):
    sortOneFile("/lucenedata/nyc-taxi-data/yellow_tripdata_2015-%02d.csv" % (month + 1))
    sortOneFile("/lucenedata/nyc-taxi-data/green_tripdata_2015-%02d.csv" % (month + 1))

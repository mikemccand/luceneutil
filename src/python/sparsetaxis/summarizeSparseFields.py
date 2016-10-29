import csv
import struct
import sys
import io

counts = {}
docCount = 0
with open(sys.argv[1], 'rb') as f:
  headers = f.readline().strip().decode('utf-8').split(',')
  while True:
    b = f.read(4)
    if len(b) == 0:
      break
    numBytes = struct.unpack('i', b)[0]
    for row in csv.reader(io.StringIO(f.read(numBytes).decode('utf-8'))):
      docCount += 1
      if docCount % 100000 == 0:
        print('%d...' % docCount)
      for i in range(len(headers)):
        if len(row[i]) > 0:
          counts[headers[i]] = 1+counts.get(headers[i], 0)

l = []
for k, v in counts.items():
  l.append((v, k))

l.sort(reverse=True)
print('Fields across %d docs in %s:' % (docCount, sys.argv[1]))
for count, fieldName in l:
  print('%6.1f%% %s' % (100.*count/docCount, fieldName))


'''
Fields across 50000000 docs in /lucenedata/nyc-taxi-data/mergedSubset.sparse.csv.blocks:
  88.3% yellow_vendor_id
  88.3% yellow_trip_distance
  88.3% yellow_total_amount
  88.3% yellow_tolls_amount
  88.3% yellow_tip_amount
  88.3% yellow_store_and_fwd_flag
  88.3% yellow_rate_code_id
  88.3% yellow_pickup_longitude
  88.3% yellow_pickup_latitude
  88.3% yellow_pickup_datetime
  88.3% yellow_payment_type
  88.3% yellow_passenger_count
  88.3% yellow_mta_tax
  88.3% yellow_improvement_surcharge
  88.3% yellow_fare_amount
  88.3% yellow_extra
  88.3% yellow_dropoff_longitude
  88.3% yellow_dropoff_latitude
  88.3% yellow_dropoff_datetime
  11.7% green_vendor_id
  11.7% green_trip_distance
  11.7% green_total_amount
  11.7% green_tolls_amount
  11.7% green_tip_amount
  11.7% green_store_and_fwd_flag
  11.7% green_rate_code_id
  11.7% green_pickup_longitude
  11.7% green_pickup_latitude
  11.7% green_pickup_datetime
  11.7% green_payment_type
  11.7% green_passenger_count
  11.7% green_mta_tax
  11.7% green_improvement_surcharge
  11.7% green_fare_amount
  11.7% green_extra
  11.7% green_dropoff_longitude
  11.7% green_dropoff_latitude
  11.7% green_dropoff_datetime
  11.7% green_trip_type
'''

'''
Fields across 50000000 docs in /lucenedata/nyc-taxi-data/mergedSubset.nonsparse.csv.blocks:
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
  11.7% trip_type
'''

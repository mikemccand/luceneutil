import time
import sys
import datetime
import os
import csv
import json

# Directory where the original .csv files were downloaded to, using downloadTaxisCSVs.py:
CSV_DIR = '/lucenedata/nyc-taxi-data'

normalize = {
  'vendorid': 'vendor_id',
  'vendor_id': 'vendor_id',
  'vendor_name': 'vendor_name',
  'pickup_datetime': 'pick_up_date_time',
  'trip_pickup_datetime': 'pick_up_date_time',
  'lpep_pickup_datetime': 'pick_up_date_time',
  'tpep_pickup_datetime': 'pick_up_date_time',
  'dropoff_datetime': 'drop_off_date_time',
  'trip_dropoff_datetime': 'drop_off_date_time',
  'passenger_count': 'passenger_count',
  'trip_distance': 'trip_distance',
  'lpep_dropoff_datetime': 'drop_off_date_time',
  'tpep_dropoff_datetime': 'drop_off_date_time',
  'pickup_latitude': 'pick_up_lat',
  'pickup_longitude': 'pick_up_lon',
  'start_lat': 'pick_up_lat',
  'start_lon': 'pick_up_lon',
  'dropoff_latitude': 'drop_off_lat',
  'dropoff_longitude': 'drop_off_lon',
  'end_lat': 'drop_off_lat',
  'end_lon': 'drop_off_lon',
  'payment_type': 'payment_type',
  'trip_type': 'trip_type',
  'fare_amount': 'fare_amount',
  'fare_amt': 'fare_amount',
  'surcharge': 'surcharge',
  'rate_code': 'rate_code',
  'ratecodeid': 'rate_code',
  'mta_tax': 'mta_tax',
  'extra': 'extra',
  'ehail_fee': 'ehail_fee',
  'improvement_surcharge': 'improvement_surcharge',
  'tip_amount': 'tip_amount',
  'tip_amt': 'tip_amount',
  'tolls_amount': 'tolls_amount',
  'tolls_amt': 'tolls_amount',
  'total_amount': 'total_amount',
  'total_amt': 'total_amount',
  'store_and_fwd_flag': 'store_and_fwd_flag',
  'store_and_forward': 'store_and_fwd_flag',
  'cab_color': 'cab_color',
}

def loadDocs():

  csvHeaders = list(set(normalize.values()))
  cabColorIndex = csvHeaders.index('cab_color')

  yield ','.join(csvHeaders)
  
  for fileName in os.listdir(CSV_DIR):
    if fileName.endswith('.csv') and fileName != 'alltaxis.csv':
      print('file %s' % fileName, file=sys.stderr)
      cabColor = fileName.split('_')[0]
      with open('%s/%s' % (CSV_DIR, fileName), 'r') as f:
        reader = csv.reader(f)
        headers = list(reader.__next__())
        #print('headers in: %s' % headers);
        datetimes = []
        strings = []
        for i in range(len(headers)):
          headers[i] = normalize[headers[i].strip().lower()]
          if headers[i].endswith('_date_time'):
            datetimes.append((i, csvHeaders.index(headers[i])))
          else:
            strings.append((i, csvHeaders.index(headers[i])))
          
        for row in reader:
          #print('  row: %s' % str(row))
          if len(row) == 0:
            continue
          csvRow = [''] * len(csvHeaders)
          csvRow[cabColorIndex] = cabColor
          for src, target in strings:
            if row[src] != '':
              csvRow[target] = row[src]
          for src, target in datetimes:
            if row[src] != '':
              try:
                dt = datetime.datetime.strptime(row[src], '%Y-%m-%d %H:%M:%S')
              except ValueError:
                print('cannot parse %s in field %s as datetime' % (row[src], headers[src]))
              else:
                # TODO: add datetime type to lucene server!
                # seconds since epoch
                csvRow[target] = str(int(dt.strftime('%s')))
          yield ','.join(csvRow)

if __name__ == '__main__':
  startTime = time.time()
  bytes = 0
  totBytes = 0
  buffer = []
  count = 0
  first = True
  for doc in loadDocs():
    if first:
      print(doc)
      first = False
      continue
    buffer.append(doc)
    bytes += len(doc)+1
    count += 1
    if count % 100000 == 0:
      print('%10.1f sec: %d docs, %.3f GB...' % (time.time() - startTime, count, totBytes/1024./1024/1024), file=sys.stderr)
    if bytes > 256*1024:
      print('%d %d' % (bytes, len(buffer)))
      print('\n'.join(buffer))
      del buffer[:]
      totBytes += bytes
      bytes = 0
    if count == 25000000:
      break
  if bytes > 0:
    print('%d %d' % (bytes, len(buffer)))
    print('\n'.join(buffer))

import multiprocessing
import time
import datetime
import os
import csv

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

def loadDocs(jobs, id, csvHeaders):

  cabColorIndex = csvHeaders.index('cab_color')

  while True:
    fileName = jobs.get()
    if fileName is None:
      return
    
    print('%s: file %s' % (id, fileName))
    cabColor = fileName.split('_')[0]
    with open('%s/%s' % (CSV_DIR, fileName), 'r') as f:
      reader = csv.reader(f)
      headers = list(reader.__next__())
      #print('headers in: %s' % headers);
      datetimes = []
      strings = []
      fareAmountIndex = -1
      fareAmountDestIndex = -1      
      for i in range(len(headers)):
        headers[i] = normalize[headers[i].strip().lower()]
        if headers[i].endswith('_date_time'):
          datetimes.append((i, csvHeaders.index(headers[i])))
        elif headers[i] == 'fare_amount':
          fareAmountIndex = i
          fareAmountDestIndex = csvHeaders.index('fare_amount')
        else:
          strings.append((i, csvHeaders.index(headers[i])))

      for row in reader:
        #print('  row: %s' % str(row))
        if len(row) == 0:
          continue
        csvRow = [''] * len(csvHeaders)
        csvRow[cabColorIndex] = cabColor
        if fareAmountIndex != -1:
          try:
            v = float(row[fareAmountIndex])
          except ValueError:
            print('skip bogus fare_amount: %s' % row[fareAmountIndex])
          else:
            # convert to its simpler string, e.g. 10.619999999999999 becomes 10.62
            csvRow[fareAmountDestIndex] = '%s' % v
        for src, target in strings:
          if row[src] != '':
            try:
              v = float(row[src])
            except ValueError:
              v = row[src]
            else:
              # convert double to their simpler string, e.g. 10.619999999999999 becomes 10.62
              v = '%s' % v
            csvRow[target] = v
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

def writeOneCSV(jobs, id):

  with open('/b/alltaxis.%02d.csv' % id, 'w') as f:

    csvHeaders = list(set(normalize.values()))
    
    if id == 0:
      f.write(','.join(csvHeaders) + '\n')

    bytes = 0
    totBytes = 0
    buffer = []
    for doc in loadDocs(jobs, id, csvHeaders):
      buffer.append(doc)
      bytes += len(doc)+1
      if bytes > 256*1024:
        f.write('%d %d\n' % (bytes, len(buffer)))
        f.write('\n'.join(buffer))
        f.write('\n')
        del buffer[:]
        totBytes += bytes
        bytes = 0
    if bytes > 0:
      f.write('%d %d\n' % (bytes, len(buffer)))
      f.write('\n'.join(buffer))
      f.write('\n')

if __name__ == '__main__':

  startTime = time.time()

  jobs = multiprocessing.Queue()

  for fileName in os.listdir(CSV_DIR):
    if fileName.endswith('.csv') and fileName != 'alltaxis.csv':
      jobs.put(fileName)

  cpuCount = multiprocessing.cpu_count()
  
  print('%d files to process, using %d processes' % (jobs.qsize(), cpuCount))

  for i in range(cpuCount):
    jobs.put(None)

  for i in range(cpuCount):
    p = multiprocessing.Process(target=writeOneCSV, args=(jobs, i))
    p.start()

  for i in range(cpuCount):
    p.join()
  

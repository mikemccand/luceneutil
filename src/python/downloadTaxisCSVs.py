import os

# first cd to a place that has lots (at least 200 GB) of space!

for year in range(2009, 2016):
    for month in range(1, 13):
        for color in 'yellow', 'green':
            if color == 'green':
                # green taxis only show up starting from 08/2013
                if year <= 2012:
                    continue
                if year == 2013 and month < 8:
                    continue
            fileName = '%s_tripdata_%s-%02d.csv' % (color, year, month)
            if not os.path.exists(fileName):
                url = 'https://storage.googleapis.com/tlc-trip-data/%s/%s' % (year, fileName)
                print('\nDOWNLOAD: %s' % url)
                if os.system('wget --no-check-certificate %s' % url):
                    raise RuntimeError('failed')

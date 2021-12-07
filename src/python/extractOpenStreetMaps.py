#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import subprocess
import re

r = re.compile('id="(.*?)" lat="(.*?)" lon="(.*?)"')

with subprocess.Popen('cat planet-latest.osm.20160307.bz2 | bunzip2 -c', stdout=subprocess.PIPE, shell=True) as p, \
     open('latlon.txt', 'wb') as fOut:
#with open('planet-latest.osm', 'rb') as f,
#     open('latlon.txt', 'wb') as fOut:
    f = p.stdout
    lineCount = 0
    nodeCount = 0
    while True:
        l = f.readline().decode('utf-8')
        if len(l) == 0:
            break
        l = l.strip()
        lineCount += 1
        if lineCount % 100000 == 0:
            print('%s: %d, %d nodes...' % (datetime.datetime.now(), lineCount, nodeCount))
        #print(l.rstrip().encode('ascii', errors='replace'))
        if l.startswith('<node'):
            m = r.search(l)
            if m is not None:
                fOut.write(('%s,%s,%s\n' % m.groups()).encode('ascii'))
                nodeCount += 1
            else:
                print('match failed: %s' % l)


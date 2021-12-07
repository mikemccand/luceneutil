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

import random

MIN_LAT = 51.0919106
MAX_LAT = 51.6542719
MIN_LON = -0.3867282
MAX_LON = 0.8492337

with open('latlon.txt', 'rb') as f, \
        open('latlon.subsetPlusAllLondon.txt', 'wb') as fOut:
    while True:
        line = f.readline()
        if len(line) == 0:
            break
        tup = line.strip().decode('ascii').split(',')
        lat = float(tup[1])
        lon = float(tup[2])
        if random.randint(0, 49) == 17 or \
                (lat >= MIN_LAT and lat <= MAX_LAT and lon >= MIN_LON and lon <= MAX_LON):
            fOut.write(line)

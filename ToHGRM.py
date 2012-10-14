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

import subprocess
import os
import sys
import struct

HDR_HISTOGRAM_PATH = '/l/HdrHistogram'

file = sys.argv[1]
warmupSec = float(sys.argv[2])

maxLatencyMS = 0
allMS = []

f = open(file, 'rb')
results = []
while True:
  b = f.read(17)
  if len(b) == 0:
    break
  timestamp, latencyMS, queueTimeMS, totalHitCount, taskLen = struct.unpack('fffIB', b)
  taskString = f.read(taskLen)
  if timestamp >= warmupSec:
    allMS.append(latencyMS)
    maxLatencyMS = max(maxLatencyMS, latencyMS)
f.close()

# Compile
if os.system('javac -cp .:%s/src ToHGRM.java' % HDR_HISTOGRAM_PATH):
  raise RuntimeError('compile failed')

p = subprocess.Popen('java -cp .:%s/src ToHGRM %g > out.hgrm 2>&1' % (HDR_HISTOGRAM_PATH, maxLatencyMS), shell=True, stdin=subprocess.PIPE)
print '%d queries' % len(allMS)
for ms in allMS:
  p.stdin.write('%g\n' % ms)
p.stdin.close()

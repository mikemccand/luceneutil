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

import os
import math
from PIL import ImageDraw, Image, ImageFont
import sys
import re

#install using pip: sudo pip install iso8601
import iso8601

from datetime import datetime

"""
Parses infoStream output from IW and draws an movie showing the merges over time.
"""

WIDTH = 1280
HEIGHT = 720
MAX_SEG_COUNT = 60
MAX_SEG_SIZE_MB = 500.0
LIMIT = None
LOG_BASE_MB = 10.0
LOG_BASE = math.log(LOG_BASE_MB)
FPS = 24

#FONT = ImageFont.truetype('/usr/local/src/yjp-9.0.7/jre64/lib/fonts/LucidaSansRegular.ttf', 15)
#FONT = ImageFont.truetype('/usr/share/fonts/liberation/LiberationSans-Bold.ttf', 15)
FONT = ImageFont.truetype('/usr/share/fonts/liberation/LiberationMono-Regular.ttf', 20)

# TODO
#   - handle deletions

mergeColors = ('#ffccff',  # pink
               '#ffff99',  # yellow
               '#ccff99',  # green
               '#ff9900',  # orange
               '#00ccff',  # blue
               '#33ffcc',  # neon green
               '#9999ff',  # purple
               )
TMP_DIR = '/dev/shm/mergeimages'

def parseTime(l):
  #Lucene now uses ISO8601 dates, so parse those.
  m = reTime.search(l)
  dt = iso8601.parse_date(m.group(1)).replace(tzinfo=None)
  diff_seconds = (dt-datetime.fromtimestamp(0)).total_seconds()
  return diff_seconds

def main():
  global MAX_SEG_COUNT
  global MAX_SEG_SIZE_MB
  
  merges, segToFullMB = parse(sys.argv[1])
  outputFile = sys.argv[2]

  MAX_SEG_COUNT = 1
  MAX_SEG_SIZE_MB = 0.0
  for i, ev in enumerate(merges):
    if ev[0] == 'index':
      segs = ev[2]
      MAX_SEG_COUNT = max(MAX_SEG_COUNT, len(segs))
      for seg, mb, delPct in segs:
        MAX_SEG_SIZE_MB = max(MAX_SEG_SIZE_MB, mb)

  MAX_SEG_COUNT += 2
  MAX_SEG_SIZE_MB = 100*math.ceil((MAX_SEG_SIZE_MB*1.1)/100.0) + 50.0

  print('MAX seg MB %s' % MAX_SEG_SIZE_MB)
  print('%d events' % len(merges))
    
  mergeToColor = {}
  segToMBAndDel = {}
  segs = None
  os.mkdir(TMP_DIR)
  upto = 0
  lastT = None
  totMergeMB = 0
  newestSeg = ''
  minT = None
  for i, ev in enumerate(merges):
    t = ev[1]
    if minT is None:
      minT = t
    # silly frame doubling, for the end:
    if lastT is not None:
      gap = t - lastT
      if gap > 2 and len(merges)-i < 6:
        print('FILL gap=%s' % gap)
        delta = (t-lastT)/(gap*5.0)
        t0 = lastT
        for _ in range(gap*5):
          t0 += delta
          print(' fake t %s' % (t0-minT))
          img, mergeToColor = draw(t0, segs, mergeToColor, newestSeg, totMergeMB)
          img.save('%s/%08d.png' % (TMP_DIR, upto))
          upto += 1
    lastT = t
    
    print('%s: %s/%s' % (t-minT, i, len(merges)))
    #print ev
    if ev[0] == 'index':
      segs = ev[2]
      for seg, fullMB, _ in segs:
        if seg not in segToMBAndDel:
          newestSeg = seg
        segToMBAndDel[seg] = (fullMB, delPct)
      if i < len(merges)-1 and merges[1+i][0] == 'merge':
        continue
    elif ev[0] == 'merge':
      seen = set()
      for seg, color in mergeToColor.items():
        seen.add(color)
      for color in mergeColors:
        if color not in seen:
          for seg in ev[2]:
            totMergeMB += segToFullMB[seg] * (2.0 - segToMBAndDel[seg][1])
            mergeToColor[seg] = color
          break
      else:
        raise RuntimeError('ran out of colors')
    else:
      raise RuntimeError('unknown event %s' % ev[0])

    img, mergeToColor = draw(t, segs, mergeToColor, newestSeg, totMergeMB)
    img.save('%s/%08d.png' % (TMP_DIR, upto))
    upto += 1
    if LIMIT is not None and upto >= LIMIT:
      break

  for x in range(FPS*5):
    t += 0.2
    img, mergeToColor = draw(t, segs, mergeToColor, newestSeg, totMergeMB)
    img.save('%s/%08d.png' % (TMP_DIR, upto))
    upto += 1

  os.chdir(TMP_DIR)
  cmd = ['mencoder',
         'mf://*.png',
         '-mf',
         'type=png:w=%s:h=%s:fps=%s' % (WIDTH, HEIGHT, FPS),
         '-ovc',
         'x264',
         '-x264encopts',
         'crf=15:frameref=6:threads=8:bframes=0:me=umh:partitions=all:trellis=1:direct_pred=auto:keyint=100:psnr',
         '-oac',
         'copy',
         '-o',
         '%s' % outputFile]
  os.spawnvp(os.P_WAIT, 'mencoder', cmd)
  print('DONE')

tMin = None
def draw(t, segs, mergeToColor, rightSegment, totMergeMB):
  global tMin
  if tMin is None:
    tMin = t
    
  i = Image.new('RGB', (WIDTH, HEIGHT), 'white')

  segsAlive = set([s[0] for s in segs])
  #print 'alive: %s' % segsAlive

  newMergeToColor = {}
  for seg, color in mergeToColor.items():
    if seg in segsAlive:
      newMergeToColor[seg] = color
      
  maxLog = math.log(LOG_BASE_MB+MAX_SEG_SIZE_MB) - LOG_BASE
  yPerLog = (HEIGHT - 20)/maxLog
  
  xPerSeg = int(WIDTH / MAX_SEG_COUNT)

  d = ImageDraw.Draw(i)

  for sz in (10.0, 50.0, 100.0, 500.0, 1024, 5*1024):
    y = HEIGHT - 10 - yPerLog * (math.log(LOG_BASE_MB + sz) - LOG_BASE)
    d.line(((0, y), (WIDTH, y)), fill='#cccccc')
    if sz >= 1024:
      s = '%d GB' % (sz/1024)
    else:
      s = '%d MB' % sz
    d.text((WIDTH-80, y-20), s, fill='black', font=FONT)

  totMB = 0
  mergingMB = 0
  for idx, (seg, mb, delPct) in enumerate(segs):
    totMB += mb*(1.0-delPct)
    x0 = idx * (xPerSeg) + 1
    x1 = x0 + xPerSeg - 2
    y0 = HEIGHT - 10 - yPerLog * (math.log(LOG_BASE_MB + mb) - LOG_BASE)
    y1 = HEIGHT - 10

    if seg in mergeToColor:
      fill = mergeToColor[seg]
      mergingMB += mb
    else:
      fill = '#dddddd'
      
    d.rectangle(((x0, y0), (x1, y1)), outline='black', fill=fill)

    if delPct > 0.0:
      y2 = y0 + (y1-y0)*delPct
      d.rectangle(((x0, y0), (x1, y2)), outline='black', fill='gray')

  baseY = HEIGHT - 10 - yPerLog * (math.log(LOG_BASE_MB + 500) - LOG_BASE) + 15
  baseX = WIDTH - 220
  
  d.text((baseX, baseY), '%d sec' % (t-tMin), fill='black', font=FONT)

  if totMB < 1024:
    sz = '%4.1f MB' % totMB
  else:
    sz = '%4.2f GB' % (totMB/1024.)
  d.text((baseX, 20+baseY), '%s' % sz, fill='black', font=FONT)

  d.text((baseX, 40+baseY), '%d segs; %s' % (len(segs), rightSegment), fill='black', font=FONT)

  if mergingMB < 1024:
    sz = '%.1f MB' % mergingMB
  else:
    sz = '%.2f GB' % (mergingMB/1024.)
  d.text((baseX, 60+baseY), '%s merging' % sz, fill='black', font=FONT)

  if totMergeMB >= 1024:
    s = '%4.2f GB' % (totMergeMB/1024)
  else:
    s = '%4.1f MB' % totMergeMB

  d.text((baseX, 80+baseY), '%s merged' % s, fill='black', font=FONT)

  return i, newMergeToColor

reSeg1 = re.compile(r'\*?(_.*?)\(.*?\):[cC]v?([0-9]+)(/[0-9]+)?')
reSeg2 = re.compile(r'seg=\*?(_.*?)\(.*?\):[cC]v?([0-9]+)(/[0-9]+)? .*?size=([0-9.]+) MB')
reTime = re.compile(r'\[(.*?)\; ')
def parse(fileName):
  events = []
  f = open(fileName, 'rb')
  segs = None
  segsToFullMB = {}
  t = None
  
  for l in f.readlines():
    if l == '':
      break

    if segs is not None:
      if l.find('allowedSegmentCount=') != -1 or l.find('LMP:   level ') != -1:
        events.append(('index', t, segs))
        segs = None
      else:
        m2 = reSeg2.search(l)
        if m2 is not None:
          seg = m2.group(1)
          # print 'matches %s' % str(m2.groups())
          delCount = m2.group(3)
          if delCount is not None:
            delCount = int(delCount[1:])
          else:
            delCount = 0
          docCount = int(m2.group(2))
            
          undelSize = float(m2.group(4))
          if seg not in segsToFullMB:
            if delCount != 0:
              delRatio = float(delCount) / docCount
              if delRatio < 1.0:
                fullSize = undelSize / (1.0 - delRatio)
              else:
                # total guess!
                print('WARNING: total guess!')
                fullSize = 0.1
            else:
              fullSize = undelSize
            segsToFullMB[seg] = fullSize

          # seg name, fullMB, delPct
          assert delCount <= docCount, 'docCount %s delCount %s line %s'  % (docCount, delCount, l)
          segs.append((seg, segsToFullMB[seg], float(delCount)/docCount))
        
    i = l.find('   add merge=')
    if i != -1:
      l2 = reSeg1.findall(l)
      merged = []
      for tup in reSeg1.findall(l):
        seg = tup[0]
        merged.append(seg)
      events.append(('merge', parseTime(l), merged))
      continue

    if l.find(': findMerges: ') != -1:
      segs = []
      t = parseTime(l)

  f.close()
  return events, segsToFullMB

main()

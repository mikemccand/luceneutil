from PIL import Image, ImageDraw
import sys
import os

# rm -rf /x/tmp/frames; python3 -u /l/util/src/python/geoShapeVideo.py /x/tmp/londonBG2.png /x/tmp/london.poly /x/tmp/frames /x/tmp/bkd.shapes.txt /x/tmp/out.avi

# steps
#   - go to OSM, browse map to background image, click export so you save lat/lon, take full screen shot, try to target 16:9 aspect ratio
#   - follow this to download polygon shape:
#     - http://stackoverflow.com/questions/14499511/how-can-i-get-the-city-borders-for-a-country
#   - gifsicle: http://stackoverflow.com/questions/6079150/how-to-generate-gif-from-avi-using-ffmpeg

FPS = 10

ALPHA = 64

frameCount = 0

def writeFrame(img):
  global frameCount
  img.save('%s/%05d.png' % (framesDir, frameCount))
  if False:
    #img.convert('RGB').convert('P', palette=Image.ADAPTIVE).save('%s/%05d.gif' % (framesDir, frameCount))
    #img.convert('RGB').convert('P', dither=Image.NONE).save('%s/%05d.gif' % (framesDir, frameCount))
    #img.convert('P', dither=Image.NONE).save('%s/%05d.gif' % (framesDir, frameCount))
    img.save('%s/%05d.gif' % (framesDir, frameCount))
    #img.convert('RGB').save('%s/%05d.gif' % (framesDir, frameCount))
    if os.system('convert %s/%05d.png %s/%05d.gif' % (framesDir, frameCount, framesDir, frameCount)):
      raise RuntimeError('convert failed')
  
  frameCount += 1
  if frameCount % 100 == 0:
    print('%d frames...' % frameCount)

def addCell(map, cellLatMin, cellLatMax, cellLonMin, cellLonMax, color):
  rect = lonToX(cellLonMin), latToY(cellLatMax), lonToX(cellLonMax), latToY(cellLatMin)
  rw = rect[2]-rect[0]
  rh = rect[3]-rect[1]
  cell = Image.new('RGBA', (rw+1, rh+1))
  cellDraw = ImageDraw.Draw(cell)
  cellDraw.rectangle((0, 0, rw, rh), fill=color, outline=(0, 0, 0, ALPHA))
  map.paste(cell, (rect[0], rect[1]), mask=cell)

def lonToX(lon):
  return int((lon - lonMin) * w/(lonMax - lonMin))

def latToY(lat):
  return h - int((lat - latMin) * h/(latMax - latMin))

# Map, screen grab from Openstreetmaps:
#bg = Image.open(sys.argv[1]).convert('RGB')
bg = Image.open(sys.argv[1])

# Polygon we used for searching
polyFile = sys.argv[2]

# Where to store video frames
framesDir = sys.argv[3]

# Which cells Lucene visited to gather hits
cellsFile = sys.argv[4]

# Where to write the video file
outputFile = sys.argv[5]

w, h = bg.size

print('orig w=%d, h=%d' % (w, h))

# Remove boundaries from full window screenshot:
w2 = w-56-406
h2 = h-81-182

# Cut to exactly 16 : 9 aspect ratio for Youtube:
w2a = int(h2*(16/9.))
h2a = int(w2/(16/9.))

latMin = 51.1630
latMax = 51.8146
lonMin = -1.0080
lonMax = .8281

if w2 > w2a:
  # Trim from the left to make narrower
  print('w2 %s vs %s' % (w2, w2a))
  latMin = latMax - (float(w2a)/w2)*(latMax - latMin)
  w2 = w2a
else:
  # Trim from the top to make shorter:
  print('h2 %s vs %s' % (h2, h2a))
  lonMin = lonMax - (float(h2a)/h2)*(lonMax - lonMin)
  h2 = h2a

justMap = bg.crop((w-56-w2, h-81-h2, w-56, h-81))

w, h = justMap.size

print('w=%d, h=%d, img=%s' % (w, h, justMap))

draw = ImageDraw.Draw(justMap)

lastLon = None

lons = []
lats = []

if os.path.exists(framesDir):
  raise RuntimeError('you must remove frames dir output "%s" first' % framesDir)

os.makedirs(framesDir)

while frameCount < 5:
  writeFrame(justMap)

with open(polyFile) as f:
  f.readline()
  f.readline()
  count = 0
  for line in f.readlines():
    line = line.strip()
    if line == 'END':
      break
    
    lon, lat = (float(x) for x in line.split())
    if count % 5 == 0:
      if lastLon is not None:
        draw.line((lonToX(lastLon),
                   latToY(lastLat),
                   lonToX(lon),
                   latToY(lat)),
                  fill='red',
                  width=2)
      lastLon = lon
      lastLat = lat
      lats.append(lat)
      lons.append(lon)

      if count % 105 == 0:
        writeFrame(justMap)

    count += 1

print('%d frames after poly' % frameCount)

#print('lats = %s' % ', '.join(str(x) for x in lats))
#print('lons = %s' % ', '.join(str(x) for x in lons))

draw.line((lonToX(lastLon),
           latToY(lastLat),
           lonToX(lon),
           latToY(lat)),
          fill='red',
          width=2)

#latMin=51.2875 latMax=51.69187 lonMin=-0.5103751 lonMax=0.333814

print('%d points in poly' % count)

count = 0

with open(cellsFile) as f:
  for line in f.readlines():
    if line.startswith('#'):
      continue
    if True:
      if line[0] == 'A':
        color = (128, 0, 0, ALPHA)
      else:
        color = (0, 0, 128, ALPHA)
    else:
      color = (0, 0, 128, ALPHA)
        
    cellLatMin, cellLatMax, cellLonMin, cellLonMax = (float(x) for x in line[2:].strip().split())
    addCell(justMap, cellLatMin, cellLatMax, cellLonMin, cellLonMax, color)

    #rect = lonToX(cellLonMin), latToY(cellLatMax), lonToX(cellLonMax), latToY(cellLatMin)
    #print('%s -> %s' % (line, rect))

    #rw = rect[2]-rect[0]
    #rh = rect[3]-rect[1]
    #cell = Image.new('RGBA', (rw+1, rh+1))
    #cellDraw = ImageDraw.Draw(cell)
    #cellDraw.rectangle((0, 0, rw, rh), fill=color, outline='white')
    #justMap.paste(cell, (rect[0], rect[1]), mask=cell)
    if count % 2 == 0:
      writeFrame(justMap)
    count += 1

print('%d frames after cells' % frameCount)

for i in range(20):
  writeFrame(justMap)

os.chdir(framesDir)

# song is 126 seconds long
fps = (frameCount-20)/124.5

print('FPS: %.2f' % fps)

cmd = ['mencoder',
       'mf://*.png',
       '-mf',
       'type=png:w=%s:h=%s:fps=%.2f' % (w, h, fps),
       '-ovc',
       'x264',
       '-x264encopts',
       'crf=20:frameref=6:threads=8:bframes=0:me=umh:partitions=all:trellis=1:direct_pred=auto:keyint=100:psnr',
       '-audiofile',
       '/x/tmp/sugarplum.m4a',
       '-oac',
       'pcm',
       '-o',
       '%s' % outputFile]

os.spawnvp(os.P_WAIT, 'mencoder', cmd)

import math

try:
  from PIL import Image, ImageDraw, ImageFont

  supported = True
except ImportError:
  supported = False

# FONT = ImageFont.truetype('/usr/local/src/yjp-9.0.7/jre64/lib/fonts/LucidaSansRegular.ttf', 15)
# FONT = ImageFont.truetype('/usr/share/fonts/liberation/LiberationSans-Bold.ttf', 15)
# FONT = ImageFont.truetype('/usr/share/fonts/liberation/LiberationMono-Regular.ttf', 12)
# BOLD_FONT = ImageFont.truetype('/usr/share/fonts/liberation/LiberationMono-Bold.ttf', 12)


def findAndLoadFont(fonts, size):
  try:
    return ImageFont.truetype(fonts[0], size)
  except:
    for f in fonts[1:]:
      res = findAndLoadFont([f], size)
      if res:
        return res
  return None


FONTS = ["LiberationMono-Regular.ttf", "arial.ttf"]
BOLD_FONTS = ["LiberationMono-Bold.ttf", "arial.ttf"]

FONT = findAndLoadFont(FONTS, 12)
BOLD_FONT = findAndLoadFont(BOLD_FONTS, 12)


HEIGHT = 600

BAR_WIDTH = 30
X_AXIS_GAP = 30
X_SPACER1 = 20
X_SPACER2 = 4
Y_SPACER = 20


class QPSChart:
  def __init__(self, data, fileOut):
    self.maxQPS = -math.inf
    for tup in data:
      self.maxQPS = max(self.maxQPS, tup[2], tup[4])

    if self.maxQPS > 100:
      qpsInc = 20
    elif self.maxQPS > 50:
      qpsInc = 10
    else:
      qpsInc = 5

    self.maxQPS = math.ceil(self.maxQPS / qpsInc) * qpsInc

    self.xPixPerCat = 2 * BAR_WIDTH + X_SPACER2
    self.yPixPerQPS = (HEIGHT - 2 * Y_SPACER) / self.maxQPS
    # print 'max %s' % self.maxQPS

    WIDTH = (1 + len(data)) * X_SPACER1 + self.xPixPerCat * len(data) + 2 * X_AXIS_GAP

    i = Image.new("RGB", (WIDTH, HEIGHT), "white")
    d = ImageDraw.Draw(i)

    d.text((5, HEIGHT / 2), "Q/S", fill="black", font=BOLD_FONT)

    qps = qpsInc
    while True:
      y = self.qpsToY(qps)
      d.text((X_AXIS_GAP, y - 15), "%d" % qps, fill="black", font=FONT)
      d.line((X_AXIS_GAP, y, WIDTH - X_AXIS_GAP, y), fill="#cccccc")
      if qps > self.maxQPS:
        break
      qps += qpsInc

    x = X_AXIS_GAP + X_SPACER1
    for idx, (cat, minBase, maxBase, minCmp, maxCmp, pValue) in enumerate(data):
      avgBase = (minBase + maxBase) / 2
      avgCmp = (minCmp + maxCmp) / 2

      # write category label
      textWidth = len(cat) * 7
      d.text((x + BAR_WIDTH + X_SPACER2 / 2 - textWidth / 2, HEIGHT - 20), cat, fill="black", font=FONT)

      # write pct diff
      maxQPS = max(maxBase, maxCmp)
      if avgBase != 0.0:
        d.text((x + BAR_WIDTH + X_SPACER2 / 2 - 10, self.qpsToY(maxQPS) - 20), "%d%%" % (100 * (avgCmp - avgBase) / avgBase), fill="black", font=FONT)

      # draw vertical error bars
      y0 = self.qpsToY(minBase)
      y1 = self.qpsToY(maxBase)
      d.line((x + BAR_WIDTH / 2, y0, x + BAR_WIDTH / 2, y1), fill="black")
      d.line((x + BAR_WIDTH / 2 - 4, y0, x + BAR_WIDTH / 2 + 4, y0), fill="black")
      d.line((x + BAR_WIDTH / 2 - 4, y1, x + BAR_WIDTH / 2 + 4, y1), fill="black")

      # draw horizontal bar on the avg
      y = self.qpsToY(avgBase)
      if 0:
        print("idx %s" % idx)
        print("  x %s-%s" % (x, x + BAR_WIDTH))
        print("  y %s" % y)
      d.rectangle((x, y - 1, x + BAR_WIDTH, y + 1), fill="black")

      y = self.qpsToY(avgCmp)
      x += BAR_WIDTH + X_SPACER2

      # draw vertical error bars
      y0 = self.qpsToY(minCmp)
      y1 = self.qpsToY(maxCmp)
      d.line((x + BAR_WIDTH / 2, y0, x + BAR_WIDTH / 2, y1), fill="black")
      d.line((x + BAR_WIDTH / 2 - 4, y0, x + BAR_WIDTH / 2 + 4, y0), fill="black")
      d.line((x + BAR_WIDTH / 2 - 4, y1, x + BAR_WIDTH / 2 + 4, y1), fill="black")

      if 0:
        print("  x %s-%s" % (x, x + BAR_WIDTH))
        print("  y %s" % y)
        print("  avg %s" % avgCmp)
      # TODO: black, if it's "close"?
      if avgCmp < avgBase:
        color = "red"
      else:
        color = "lightgreen"
      d.rectangle((x, y - 1, x + BAR_WIDTH, y + 1), fill=color)
      x += BAR_WIDTH + X_SPACER1

    i.save(fileOut)

  def qpsToY(self, qps):
    return Y_SPACER + self.yPixPerQPS * (self.maxQPS - qps)


if __name__ == "__main__":
  data = (("Fuzzy1", 22.3, 24.4, 33.3, 36.6), ("Fuzzy2", 12.3, 13.1, 20.0, 22.7))

  data = [
    ("PKLookup", 35.857560265234987, 41.378289710787264, 51.528617460645982, 61.464880599732545),
    ("Respell", 19.071494888192849, 21.800752505003842, 18.378227119277543, 21.065411528030449),
    ("AndHighHigh", 13.736825577461657, 15.454975560520644, 13.747837736185634, 15.314499692839323),
    ("AndHighMed", 36.409195096783016, 41.750672452474106, 36.023458435787106, 41.69730960252047),
    ("Fuzzy1", 24.103061606236384, 28.190853848685954, 17.893541912883148, 20.91285731393268),
    ("Fuzzy2", 16.171113072790742, 18.333181438851071, 11.0296599158339, 12.382609567525712),
    ("HighFreqExactPhrase", 9.630899757826171, 11.778752905272396, 9.6493162789689482, 11.689082292731706),
    ("HighFreqSloppyPhrase", 6.4230921881253504, 7.2713839891550691, 6.5258100680809932, 7.6351458984484584),
    ("HighFreqTerm", 67.244955652153479, 90.197317310980139, 66.613067660808611, 98.275992999698786),
    ("IntNumericRange", 7.0017674150715505, 10.902254600787781, 7.2820388314926525, 9.966285683359855),
    ("OrHighHigh", 4.4821194346837441, 6.3570630069159817, 4.349720958797616, 6.1186183314263536),
    ("OrHighMed", 18.360542268510258, 25.208904070484159, 17.614708027927303, 24.559424714272215),
    ("Prefix3", 19.61953664268777, 25.562940506150742, 19.829139905250635, 25.542258599697639),
    ("SpanNear", 6.5911522009042685, 8.4434808602986724, 6.8610194733453289, 8.3953376799799386),
    ("Wildcard", 22.927185858877859, 29.481936489645264, 23.459234350158496, 28.316063780077645),
  ]

  QPSChart(data, "/x/tmp/out.png")

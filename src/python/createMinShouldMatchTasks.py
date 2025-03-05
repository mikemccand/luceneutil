import math
import random
import re

reTerm = re.compile(r"^(.*?) ([\d,]+)$")
f = open("topTerms20130102.txt")
terms = []

for line in f.readlines():
  if line == "":
    continue
  m = reTerm.match(line.strip())

  term, freq = m.groups()
  freq = int(freq.replace(",", ""))

  if term.find(":") != -1:
    continue
  terms.append((term, freq))

minTermsInT1 = 25

startIndex = 0
while True:
  freq = terms[startIndex][1]
  freq2 = terms[startIndex + minTermsInT1 - 1][1]
  if float(freq2) / freq >= 0.5:
    break
  startIndex += 1

print("startIndex %s" % startIndex)

topFreq = terms[startIndex][1]
byCat = {}
for term, freq in terms[startIndex:]:
  cat = "T%d" % int(math.log(float(topFreq) / freq) / math.log(2.0))
  if cat not in byCat:
    byCat[cat] = []
  byCat[cat].append((term, freq))

keys = byCat.keys()
keys.sort()
for key in keys:
  print("%s: %d terms" % (key, len(byCat[key])))

if True:
  for i in range(10000):
    numTerms = random.choice((5, 10, 15, 20, 25))
    minShouldMatch = random.randint(2, numTerms)
    numHighFreqTerms = int(random.random() * numTerms)
    highTermCat = random.randint(0, 2)
    lowTermCat = highTermCat + 6
    if numHighFreqTerms > 0:
      terms = random.sample(byCat["T%d" % highTermCat], numHighFreqTerms)
    else:
      terms = []
    numLowFreqTerms = numTerms - numHighFreqTerms
    if numLowFreqTerms > 0:
      terms += random.sample(byCat["T%d" % lowTermCat], numLowFreqTerms)

    random.shuffle(terms)

    label = "%dTerms%dHigh%dMSM" % (numTerms, numHighFreqTerms, minShouldMatch)
    print("%s: %s +minShouldMatch=%d" % (label, " ".join([x[0] for x in terms]), minShouldMatch))

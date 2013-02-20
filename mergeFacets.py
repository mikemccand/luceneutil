import sys
import random

# TODO
#   - only pass necessary values out to each shard on re-iter
#   - don't increase topN unless we have to
#   - try different facet cases
#     - the adversary
#     - draws-from-same-model
#     - fully orthogonal

class FacetShard:

  def __init__(self, r, facetValues):
    numResults = r.randint(0, len(facetValues))
    #numResults = r.randint(0, 20)
    numResults = min(numResults, len(facetValues))
    r.shuffle(facetValues)
    results = {}
    for value in facetValues[:numResults]:
      results[value] = r.randint(1, 1000)
    results = results.items()
    results.sort(cmpByCountThenLabel)
    self.results = results

  def getHits(self, topN=None, specificValues=None):
    if topN is None:
      assert specificValues is None
      return self.results, None, None
    else:
      l = self.results[:topN]
      if specificValues is not None:
        l2 = []
        seen = set()
        for value, count in self.results:
          if value in specificValues:
            seen.add(value)
            l2.append((value, count))
        for value in specificValues:
          if value not in seen:
            l2.append((value, 0))
      else:
        l2 = None
          
      return l, len(self.results), l2

def randomString(r):
  len = r.randint(1, 10)
  l = []
  for i in xrange(len):
    l.append(chr(97 + r.randint(0, 25)))
  return ''.join(l)

# TODO: we could in theory "know" ahead of time, given a field's facet
# distribution, what a reasonable starting mult factor is.  Eg a big
# flat field (username) likely needs higher mult?
    
def merge(shards, topN):
  if VERBOSE:
    print '  merge topN=%s' % topN

  mult = 2
  values = None
  lTopN = None
  
  # holds (truncated, map<value,count>, and lowestCount) for each shard:
  shardHits = [(False, None, None)] * len(shards)

  iterCount = 0

  while True:
    if VERBOSE:
      print '    cycle mult=%s' % mult

    iterCount += 1

    if lTopN is not None:
      specificValues = [x[0] for x in lTopN]
    else:
      specificValues = None

    if VERBOSE:
      print '    query shards'
    for i in xrange(len(shards)):
      exhausted, shardValues, lowestCount = shardHits[i]
      if specificValues is not None:
        shardMissingValues = []
        for label in specificValues:
          if label is not None and label not in shardValues:
            shardMissingValues.append(label)
      else:
        shardMissingValues = None
        
      if not exhausted:
        # nocommit make this a searchAfter ... ie if we already got the
        # first 10, then don't get them again
        hits, totalHitCount, newValues = shards[i].getHits(mult*topN, shardMissingValues)
        exhausted = totalHitCount <= mult*topN

        if shardValues is None:
          shardValues = {}
            
        for value, count in hits:
          # nocommit re-enable:
          #assert value not in shardValues
          shardValues[value] = count

        if exhausted:
          lowestCount = 0
        else:
          lowestCount = hits[-1][1]

        if newValues is not None:
          for value, count in newValues:
            shardValues[value] = count

        if VERBOSE:
          print '      shard %d: totalHits=%s len(hits)=%s exhausted=%s lowestCount=%d values=%s' % \
                (i, totalHitCount, len(hits), exhausted, lowestCount, shardMissingValues)
          for value, count in hits:
            print '        %s: count=%s' % (value, count)
          if newValues is not None:
            for value, count in newValues:
              print '       *%s: count=%d' % (value, count)
        shardHits[i] = (exhausted, shardValues, lowestCount)
      else:
        if VERBOSE:
          print '      shard %d: skip exhausted=%s len(shardMissingValues)=%s' % \
                (i, exhausted, len(shardMissingValues))

    # nocommit must handle the "all shards have 0 facets" case ... we
    # will exc below

    # maps value -> [totalCount, missingFromSomeShards]
    merged = {}

    # Initial merge:
    sumLowestCount = 0
    for exhausted, shardValues, lowestCount in shardHits:
      if not exhausted:
        sumLowestCount += lowestCount
      for value, count in shardValues.items():
        if value not in merged:
          merged[value] = [0, False]
        l = merged[value]
        l[0] += count

    # Second pass merge: add in proxy counts for values missing from
    # shards, to see if they might penetrate the requested topN:
    for value, l in merged.items():
      #print '  merged value %s' % value
      for shard, (exhausted, shardValues, lowestCount) in enumerate(shardHits):
        #print '    shard %d, exhaused %s, values %s' % (shard, exhausted, shardValues)
        if not exhausted and value not in shardValues:
          #if VERBOSE:
          #  print '      shard %d missing value %s' % (shard, value)
          # nocommit this could be "smarter", eg a pro-rated estimate:
          l[0] += lowestCount
          l[1] = True

    l = merged.items()

    # Add entry for an unknown label that we haven't seen yet but
    # could sum to sumLowestCount:
    if sumLowestCount > 0:
      # nocommit how come test doesn't fail if i comment this out!
      l.append((None, [sumLowestCount, True]))
    
    l.sort(cmpByCountThenLabel2)

    retry = False
    lTopN =  l[:topN]

    if sumLowestCount == 0:
      break
    
    if VERBOSE:
      print '    merged:'
    sawNone = False
    for value, (count, someMissing) in lTopN:
      if value is None:
        sawNone = True
      if VERBOSE:
        print '      %s: count=%d, someMissing=%s' % (value, count, someMissing)
      if someMissing:
        if VERBOSE:
          print '        retry'
        retry = True
    if VERBOSE:
      for value, (count, someMissing) in l[topN:]:
        if VERBOSE:
          print '    **%s: count=%d, someMissing=%s' % (value, count, someMissing)

    if retry:
      # nocommit sometimes ... we don't need to increase mult, ie, we
      # only need to request certain values
      # nocommit why True or needed...
      if sawNone:
        mult *= 2
      if VERBOSE:
        print '  run again with mult=%s' % mult
      continue
    else:
      break

  print '    %d iters' % iterCount

  # l is list of (value, ([shardSeenCount, totalCount])), sorted by totalCount descending
  return [(x[0], x[1][0]) for x in l[:topN]]

def test(seedIn):

  while True:
    if VERBOSE:
      print
      print 'Test: cycle'

    if seedIn is None:
      seed = random.randint(-sys.maxint-1, sys.maxint)
    else:
      seed = seedIn

    r = random.Random(seed)

    facetValues = set()
    numFacetValues = r.randint(10, 100)
    #numFacetValues = 10
    while len(facetValues) < numFacetValues:
      facetValues.add(randomString(r))
    facetValues = list(facetValues)

    # nocommit
    numShards = r.randint(1, 100)
    #numShards = r.randint(1, 10)
    if True or VERBOSE:
      print '  seed %s, %d shards, %d values' % (seed, numShards, numFacetValues)
      
    shards = []
    for i in range(numShards):
      shard = FacetShard(r, facetValues)
      if VERBOSE:
        print '  shard %d: %d values' % (i, len(shard.results))
        for value, count in shard.results:
          print '    %s: count=%d' % (value, count)
      shards.append(shard)

    # Get correct fully merged result:
    allResults = {}
    for shard in shards:
      for value, count in shard.getHits()[0]:
        if value not in allResults:
          allResults[value] = 0
        allResults[value] += count

    allResults = allResults.items()

    allResults.sort(cmpByCountThenLabel)
    
    for topN in xrange(1, numFacetValues+3):
      # nocommit
      #topN = r.randint(1, 1000)
      #topN = 2
      if True or VERBOSE:
        print '  iter: topN=%d' % topN

      expected = allResults[:topN]
      actual = merge(shards, topN)
      if VERBOSE:
        print '    expected'
        for value, count in expected:
          print '      %s: count=%d' % (value, count)
        print '    actual'
        for value, count in actual:
          print '      %s: count=%d' % (value, count)

      if actual != expected:
        raise RuntimeError('FAIL: seed=%s' % seed)

def cmpByCountThenLabel(a, b):
  c = cmp(b[1], a[1])
  if c != 0:
    return c
  return cmp(a[0], b[0])

def cmpByCountThenLabel2(a, b):
  c = cmp(b[1][0], a[1][0])
  if c != 0:
    return c
  return cmp(a[0], b[0])

if __name__ == '__main__':
  if not __debug__:
    raise RuntimeError('please run python without -O')
  VERBOSE = '-verbose' in sys.argv
  seed = None
  print 'argv %s' % sys.argv
  for i in xrange(len(sys.argv)):
    if sys.argv[i] == '-seed':
      seed = int(sys.argv[i+1])
      break
  test(seed)

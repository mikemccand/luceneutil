import sys

def loadResults(fileName):
  allAnswers = {}
  answers = []
  with open(fileName) as f:
    for line in f.readlines():
      line = line.rstrip()
      if line.startswith('prefixLen='):
        prefixLen = int(line[10:])
        print prefixLen
      elif line.startswith('  q='):
        if len(answers) > 0:
          allAnswers[query] = answers
        query = line[4:]
        answers = []
        #print '  %s' % query
      elif line.startswith('    '):
        answers.append(line[4:])
  print '%s: %d results' % (fileName, len(allAnswers))
  return allAnswers

a = loadResults(sys.argv[1])
b = loadResults(sys.argv[2])

for key, answersA in a.items():
  if key in b:
    answersB = b[key]
    if answersB != answersA:
      print '%s' % key
      for i in xrange(max(len(answersA), len(answersB))):
        if i >= len(answersA):
          xa = ''
        else:
          xa = answersA[i]
        if i >= len(answersB):
          xb = ''
        else:
          xb = answersB[i]
        if xa != xb:
          print '  * %d %s vs %s' % (i, xa, xb)
        else:
          print '    %d %s' % (i, xb)
          

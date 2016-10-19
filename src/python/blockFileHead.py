import sys

limit = int(sys.argv[2])

with open(sys.argv[1], 'r') as f:
  header = f.readline()
  print(header.rstrip())
  docCount = 0
  while True:
    header = f.readline()
    bytes, docs = [int(x) for x in header.strip().split()]

    if docCount + docs <= limit:
      print(header.strip())
      sys.stdout.write(f.read(bytes))
      docCount += docs
    else:
      docs = f.read(bytes).split('\n')
      docs = docs[:limit-docCount]
      s = '\n'.join(docs)+'\n'
      print('%d %d' % (len(s), limit-docCount))
      sys.stdout.write(s)
      break
        
      

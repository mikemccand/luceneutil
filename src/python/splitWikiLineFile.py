import sys

f = open(sys.argv[1], "r")
fOut = open(sys.argv[2], "w")
numCharLimit = int(sys.argv[3])

# copy header over
l = f.readline()
fOut.write(l)

leftover = ""
doc_count = 0
skip_doc_count = 0
output_doc_count = 0

while True:
  if leftover == "":
    l = f.readline()
    doc_count += 1
    if l == "":
      break
    l = l.strip().split("\t", 2)
    if len(l) != 3:
      # print('failed to get 3 elems: %d, doc count %s, line %s; skipping' % (len(l), doc_count, l))
      skip_doc_count += 1
      continue
    else:
      title = l[0]
      date = l[1]
      leftover = l[2]

  if len(leftover) <= numCharLimit:
    fOut.write("%s\t%s\t%s\n" % (title, date, leftover))
    output_doc_count += 1
    leftover = ""
  else:
    spot = numCharLimit
    # TODO: this is super hackity!!  change to proper unicode word break splitting
    while spot >= 0 and leftover[spot] != " ":
      spot -= 1

    if spot == 0:
      spot = numCharLimit

    chunk = leftover[:spot]
    fOut.write("%s\t%s\t%s\n" % (title, date, chunk))
    leftover = leftover[spot:]
    output_doc_count += 1

print("%d input documents (%d skipped due to empty body text))" % (doc_count, skip_doc_count))
print("%d output documents" % output_doc_count)

f.close()
fOut.close()

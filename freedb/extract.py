import tarfile
import re
import codecs

fromLatin1 = codecs.getdecoder('Latin1')
fromUTF8 = codecs.getdecoder('UTF-8')
toUTF8 = codecs.getencoder('UTF-8')
ascii = re.compile('^[ -z]+$')

def isascii(s):
  return ascii.match(s) is not None

t = tarfile.open('freedb-complete-20121101.tar', encoding='UTF-8')
fOut = open('all.txt', 'wb')
for ent in t:
  #if ent.name.find('data') != -1:
  #  continue
  #print '%s: %d %s' % (ent.name, ent.size, ent.isfile())
  if ent.isfile():
    discID = None
    title = None
    songs = []
    s = t.extractfile(ent).read()
    for line in s.split('\n'):
      line = line.strip()
      if line.startswith('DISCID='):
        discID = line[7:]
      elif line.startswith('DTITLE='):
        title = line[7:]
      elif line.startswith('TTITLE'):
        i = line.find('=')
        if i != -1:
          s = line[i+1:].strip()
          if len(s) > 0:
            songs.append(s)
        else:
          print 'missing =: %s' % s
    if title is not None and discID is not None and len(songs) > 0:
      if True:
        line = '%s\t%s\t%s' % \
                     (discID, title, '\t'.join(songs))
        line = line.replace('\r', ' ')
        line = line.replace('\n', ' ')
        try:
          line = fromUTF8(line)[0]
        except:
          line = fromLatin1(line)[0]
        line = toUTF8(line)[0]
        fOut.write(line+'\n')
      if False:
        if isascii(title):
          for song in songs:
            if not isascii(song):
              break
          else:
            fOut.write('%s\t%s\t%s\n' % \
                         (discID, title, '\t'.join(songs)))
    else:
      print
      print
      print 'FAILED'
      print s

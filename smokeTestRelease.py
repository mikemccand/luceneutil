import os
import shutil
import hashlib
import httplib
import re
import urllib2
import urlparse
import sys
import HTMLParser

# This tool expects to find /lucene and /solr off the base URL

# http://s.apache.org/lusolr32rc2

# TODO
#   - verify KEYS contains key that signed the release
#   - make sure changes HTML looks ok
#   - check maven
#   - check JAR manifest version
#   - check license/notice exist
#   - check no "extra" files
#   - make sure jars exist inside bin release
#   - run "ant test"
#   - make sure docs exist
#   - use java5 for lucene/modules

reHREF = re.compile('<a href="(.*?)">(.*?)</a>')

# nocommit
DEBUG = True

def getHREFs(urlString):

  # Deref any redirects
  while True:
    url = urlparse.urlparse(urlString)
    h = httplib.HTTPConnection(url.netloc)
    h.request('GET', url.path)
    r = h.getresponse()
    newLoc = r.getheader('location')
    if newLoc is not None:
      urlString = newLoc
    else:
      break

  links = []
  for subUrl, text in reHREF.findall(urllib2.urlopen(urlString).read()):
    fullURL = urlparse.urljoin(urlString, subUrl)
    links.append((text, fullURL))
  return links

def download(name, urlString, tmpDir):
  fileName = '%s/%s' % (tmpDir, name)
  if DEBUG and os.path.exists(fileName):
    if fileName.find('.asc') == -1:
      print '    already done: %.1f MB' % (os.path.getsize(fileName)/1024./1024.)
    return
  fIn = urllib2.urlopen(urlString)
  fOut = open(fileName, 'wb')
  success = False
  try:
    while True:
      s = fIn.read(65536)
      if s == '':
        break
      fOut.write(s)
    fOut.close()
    fIn.close()
    success = True
  finally:
    fIn.close()
    fOut.close()
    if not success:
      os.remove(fileName)
  if fileName.find('.asc') == -1:
    print '    %.1f MB' % (os.path.getsize(fileName)/1024./1024.)
    
def load(urlString):
  return urllib2.urlopen(urlString).read()
  
def checkSigs(project, urlString, version, tmpDir):

  print '  test basics...'
  ents = getDirEntries(urlString)
  artifact = None
  keysURL = None
  changesURL = None
  mavenURL = None
  expectedSigs = ['asc', 'md5', 'sha1']
  artifacts = []
  for text, subURL in ents:
    if text == 'KEYS':
      keysURL = subURL
    elif text == 'maven/':
      mavenURL = subURL
    elif text.startswith('changes-'):
      if text != 'changes-%s/' % version:
        raise RuntimeError('%s: found %s vs expected changes-%s/' % (project, text, version))
      changesURL = subURL
    elif artifact == None:
      artifact = text
      artifactURL = subURL
      if project == 'solr':
        expected = 'apache-solr-%s' % version
      else:
        expected = 'lucene-%s' % version
      if not artifact.startswith(expected):
        raise RuntimeError('%s: unknown artifact %s: expected prefix %s' % (project, text, expected))
      sigs = []
    elif text.startswith(artifact + '.'):
      sigs.append(text[len(artifact)+1:])
    else:
      if sigs != expectedSigs:
        raise RuntimeError('%s: artifact %s has wrong sigs: expected %s but got %s' % (project, artifact, expectedSigs, sigs))
      artifacts.append((artifact, artifactURL))
      artifact = text
      artifactURL = subURL
      sigs = []

  if sigs != []:
    artifacts.append((artifact, artifactURL))
    if sigs != expectedSigs:
      raise RuntimeError('%s: artifact %s has wrong sigs: expected %s but got %s' % (project, artifact, expectedSigs, sigs))

  if project == 'lucene':
    expected = ['lucene-%s-src.tgz' % version,
                'lucene-%s.tgz' % version,
                'lucene-%s.zip' % version]
  else:
    expected = ['apache-solr-%s-src.tgz' % version,
                'apache-solr-%s.tgz' % version,
                'apache-solr-%s.zip' % version]

  actual = [x[0] for x in artifacts]
  if expected != actual:
    raise RuntimeError('%s: wrong artifacts: expected %s but got %s' % (expected, actual))
                
  if keysURL is None:
    raise RuntimeError('%s is missing KEYS' % project)

  download('%s.KEYS' % project, keysURL, tmpDir)

  keysFile = '%s/%s.KEYS' % (tmpDir, project)

  # Set up clean gpg world; import keys file:
  gpgHomeDir = '%s/%s.gpg' % (tmpDir, project)
  if os.path.exists(gpgHomeDir):
    shutil.rmtree(gpgHomeDir)
  os.makedirs(gpgHomeDir, 0700)
  run('gpg --homedir %s --import %s' % (gpgHomeDir, keysFile),
      '%s/%s.gpg.import.log 2>&1' % (tmpDir, project))

  if mavenURL is None:
    raise RuntimeError('%s is missing maven' % project)

  if changesURL is None and project == 'lucene':
    raise RuntimeError('%s is missing changes-%s' % (project, version))

  for artifact, urlString in artifacts:
    print '  download %s...' % artifact
    download(artifact, urlString, tmpDir)
    verifyDigests(artifact, urlString, tmpDir)

    print '    verify sig'
    # Test sig
    download(artifact + '.asc', urlString + '.asc', tmpDir)
    sigFile = '%s/%s.asc' % (tmpDir, artifact)
    artifactFile = '%s/%s' % (tmpDir, artifact)
    logFile = '%s/%s.%s.gpg.verify.log' % (tmpDir, project, artifact)
    run('gpg --homedir %s --verify %s %s' % (gpgHomeDir, sigFile, artifactFile),
        logFile)
    # Forward any GPG warnings:
    f = open(logFile, 'rb')
    for line in f.readlines():
      if line.lower().find('warning') != -1:
        print '      GPG: %s' % line.strip()
    f.close()

def run(command, logFile):
  if os.system('%s > %s 2>&1' % (command, logFile)):
    raise RuntimeError('command "%s" failed; see log file %s' % (command, logFile))
    
def verifyDigests(artifact, urlString, tmpDir):
  print '    verify md5/sha1 digests'
  md5Expected, t = load(urlString + '.md5').strip().split()
  if t != '*'+artifact:
    raise RuntimeError('MD5 %s.md5 lists artifact %s but expected *%s' % (urlString, t, artifact))
  
  sha1Expected, t = load(urlString + '.sha1').strip().split()
  if t != '*'+artifact:
    raise RuntimeError('SHA1 %s.sha1 lists artifact %s but expected *%s' % (urlString, t, artifact))
  
  m = hashlib.md5()
  s = hashlib.sha1()
  f = open('%s/%s' % (tmpDir, artifact))
  while True:
    x = f.read(65536)
    if x == '':
      break
    m.update(x)
    s.update(x)
  f.close()
  md5Actual = m.hexdigest()
  sha1Actual = s.hexdigest()
  if md5Actual != md5Expected:
    raise RuntimeError('MD5 digest mismatch for %s: expected %s but got %s' % (artifact, md5Expected, md5Actual))
  if sha1Actual != sha1Expected:
    raise RuntimeError('SHA1 digest mismatch for %s: expected %s but got %s' % (artifact, sha1Expected, sha1Actual))
  
def getDirEntries(urlString):
  links = getHREFs(urlString)
  for i, (text, subURL) in enumerate(links):
    if text == 'Parent Directory':
      return links[(i+1):]

def main():

  if len(sys.argv) != 4:
    print
    print 'Usage python -u %s BaseURL version tmpDir' % sys.argv[0]
    print
    sys.exit(1)

  baseURL = sys.argv[1]
  version = sys.argv[2]
  tmpDir = sys.argv[3]

  if not DEBUG:
    if os.path.exists(tmpDir):
      raise RuntimeError('temp dir %s exists; please remove first' % tmpDir)
    os.makedirs(tmpDir)
  
  lucenePath = None
  solrPath = None
  print 'Load release URL...'
  for text, subURL in getDirEntries(baseURL):
    if text.lower().find('lucene') != -1:
      lucenePath = subURL
    elif text.lower().find('solr') != -1:
      solrPath = subURL

  if lucenePath is None:
    raise RuntimeError('could not find lucene subdir')
  if solrPath is None:
    raise RuntimeError('could not find solr subdir')

  print 'Test Lucene...'
  checkSigs('lucene', lucenePath, version, tmpDir)
  print 'Test Solr...'
  checkSigs('solr', solrPath, version, tmpDir)

if __name__ == '__main__':
  main()
  

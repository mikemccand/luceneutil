import os
import sys
import setup
import time
import tempfile
import urllib.parse
import subprocess
import wikiXMLToText
import WikipediaExtractor
import createLineFileDocsWithRandomLabel
import shutil
import bz2
import urllib.request

# See details in https://github.com/mikemccand/luceneutil/pull/69

# NOTE!  This requires ~30 GB temp space, and takes ~2 hours to run on fastish CPU (as of June 2020)

# TODO
#   - what about the imageCount, sectionCount, columns, etc.?
#   - make the "small" docs file
#   - there are MANY 0-byte body line file docs -- filter them out from "big" line docs
#   - vectors
#   - make binary form too
#   - split into LineDocsFile
#   - process into 1 KB sized file too
#   - convert both to binary format

# hmm what really is the difference!? -- multistream is multiple parts!  (and previously I was only downloading the first part!?)
#SOURCE_URL = 'https://dumps.wikimedia.org/enwiki/20201201/enwiki-20201201-pages-articles.xml.bz2'
#SOURCE_URL = 'https://dumps.wikimedia.org/enwiki/20201201/enwiki-20201201-pages-articles-multistream.xml.bz2'
SOURCE_URL = 'https://dumps.wikimedia.org/enwiki/latest/enwiki-latest-pages-articles.xml.bz2'
    

def to_gb(x):
  return x / 1024 / 1024 / 1024

def maybe_create_binary_file(text_file_name):
  binary_file_name = text_file_name.replace('.txt', '.bin')

  if not os.path.exists(binary_file_name):
    t0 = time.time()
    subprocess.check_call([sys.executable,
                           'src/python/buildBinaryLineDocs.py',
                           text_file_name,
                           binary_file_name])
    t1 = time.time()
    print('%.1f sec to create binary line file %s' % (t1 - t0, binary_file_name))

  print('binary line file is %.2f GB' % to_gb(os.path.getsize(binary_file_name)))

def main():

  if sys.version_info < (3, 6):
    raise RuntimeError('Python 3.6 or higher is required; got: %s' % sys.version)
  
  if len(sys.argv) != 2:
    raise RuntimeError('Usage: python3.8 -u createEnglishWikipediaLineDocsFile.py /path/to/enwiki-output-file-lines.txt')

  if not os.path.exists('src/python/combineWikiFiles.py'):
    raise RuntimeError('Please run this in /l/util (or your equivalent!) working directory')

  big_line_file_name = sys.argv[1]
  if not big_line_file_name.endswith('.txt'):
    raise RuntimeError('output file must have .txt suffix; got: "%s"' % big_line_file_name)

  url = urllib.parse.urlparse(SOURCE_URL)

  # nocommit
  #with tempfile.TemporaryDirectory() as tmp_dir_name:
  tmp_dir_name = '/b0/tmp'

  if True:

    print(f'Using temp dir "{tmp_dir_name}"')

    free_bytes = shutil.disk_usage(tmp_dir_name).free

    # nocommit
    if free_bytes < 30 * 1024 * 1024 * 1024:
      raise RuntimeError('Only %.2f GB free space in tempdir; need ~30 GB free' % (free_bytes / 1024 / 1024 / 1024))

    local_file_name = os.path.join(tmp_dir_name, os.path.split(url.path)[-1])
    if not os.path.exists(local_file_name):
      # download Wikipedia en export, ~19.9 GB as of Oct 9 2023
      print('Downloading %s to %s...' % (SOURCE_URL, local_file_name))
      t0 = time.time()
      setup.Downloader(SOURCE_URL, local_file_name).download()
      t1 = time.time()
      print(f'\n{t1-t0:.1f} sec to download source XML')

    # run wikiXMLToText first, creating first enwiki-lines.txt:
    line_file_name = os.path.join(tmp_dir_name, 'enwiki-lines.txt')
    if not os.path.exists(line_file_name):
      print(f'now bunzip & convert XML to text')
      t0 = time.time()
      with bz2.open(local_file_name, 'rt', encoding='utf-8') as f_in, open(line_file_name, 'w', encoding='utf-8') as f_out:
        wikiXMLToText.convert(f_in, f_out)
      t1 = time.time()
      print('%.1f sec to run wikiXMLToText' % (t1 - t0))

    # ~60.95 GB
    print('first file is %.2f GB' % (to_gb(os.path.getsize(line_file_name))))

    # run WikipediaExtractor second, creating extracted/AA/wiki_{00,01,02,...}:
    wiki_extract_output_dir = os.path.join(tmp_dir_name, 'wiki')
    extract_file_name = os.path.join(wiki_extract_output_dir, 'AA', 'wiki_00')
    if not os.path.exists(extract_file_name):
      splitter = WikipediaExtractor.OutputSplitter(False, sys.maxsize, wiki_extract_output_dir)
      with bz2.open(local_file_name, 'rb') as f_in:
        t0 = time.time()
        WikipediaExtractor.process_data(f_in, splitter)
        t1 = time.time()
        print('%.1f sec to run WikipediaExtractor' % (t1 - t0))
      splitter.close()

    # ~15.88 GB
    print('second file is %.2f GB' % to_gb(os.path.getsize(extract_file_name)))

    pre_random_line_file_name = os.path.join(tmp_dir_name, 'enwiki-pre-random-labels.txt')

    # finally, run combineWikiFiles.py to merge the two into final lines file, and keep only first three columns:
    if not os.path.exists(pre_random_line_file_name):
      t0 = time.time()
      subprocess.check_call([sys.executable,
                             'src/python/combineWikiFiles.py',
                             '-only-three-columns',
                             line_file_name,
                             extract_file_name,
                             pre_random_line_file_name])
      t1 = time.time()
      print('%.1f sec to run combineWikiFiles' % (t1 - t0))

    # ~13.08 GB
    print('big line file is %.2f GB' % to_gb(os.path.getsize(pre_random_line_file_name)))

    # add random label column
    if not os.path.exists(big_line_file_name):
      print('now add random label column')
      createLineFileDocsWithRandomLabel.createLineFileDocsWithRandomLabels(pre_random_line_file_name, big_line_file_name)

    # ~13.08 GB
    print('big line file with random labels is %.2f GB' % to_gb(os.path.getsize(big_line_file_name)))

    # create binary version for big file:
    maybe_create_binary_file(big_line_file_name)

    # create ~1K sized docs file:
    medium_line_file_name = big_line_file_name.replace('.txt', '-medium.txt')
    if not os.path.exists(medium_line_file_name):
      t0 = time.time()
      subprocess.check_call([sys.executable,
                             'src/python/splitWikiLineFile.py',
                             big_line_file_name,
                             medium_line_file_name,
                             '1024'])
      t1 = time.time()
      print('%.1f sec to create big medium line file' % (t1 - t0))

    # create binary version for medium file:
    maybe_create_binary_file(medium_line_file_name)

if __name__ == '__main__':
  main()

                     

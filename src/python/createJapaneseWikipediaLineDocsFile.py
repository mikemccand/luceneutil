import os
import sys
import setup
import tempfile
import urllib.parse
import subprocess
import wikiXMLToText
import WikipediaExtractor
import shutil
import bz2

# See details in https://github.com/mikemccand/luceneutil/pull/69

# NOTE!  This requires ~30 GB temp space, and takes ~2 hours to run on fastish CPU (as of June 2020)

SOURCE_URL = 'https://dumps.wikimedia.org/jawiki/20200620/jawiki-20200620-pages-articles.xml.bz2'

def main():

  if sys.version_info < (3, 6):
    raise RuntimeError('Python 3.6 or higher is required; got: %s' % sys.version)
  
  if len(sys.argv) != 2:
    raise RuntimeError('Usage: python3.8 -u createJapaneseWikipediaLineDocsFile.py /path/to/jawiki-20200620-lines.txt')

  final_line_file_name = sys.argv[1]
    
  url = urllib.parse.urlparse(SOURCE_URL)

  with tempfile.TemporaryDirectory() as tmp_dir_name:

    print(f'Using temp dir "{tmp_dir_name}"')

    free_bytes = shutil.disk_usage(tmp_dir_name).free
    if free_bytes < 39 * 1024 * 1024 * 1024:
      raise RuntimeError('Only %.2f GB free space in tempdir; need ~39 GB free' % (free_bytes / 1024 / 1024 / 1024))

    # download Wikipedia ja export, ~3 GB
    local_file_name = os.path.join(tmp_dir_name, os.path.split(url.path)[-1])
    print('Downloading %s to %s...' % (SOURCE_URL, local_file_name))
    setup.Downloader(SOURCE_URL, local_file_name).download()

    # run wikiXMLToText first, creating first jawiki-lines.txt:
    line_file_name = os.path.join(tmp_dir_name, 'jawiki-lines.txt')
    with bz2.open(local_file_name, 'rt', encoding='utf-8') as f_in, open(line_file_name, 'w', encoding='utf-8') as f_out:
      wikiXMLToText.convert(f_in, f_out)

    # ~12 GB
    print('first file is %s bytes' % os.path.getsize(line_file_name))

    # run WikipediaExtractor second, creating extracted/AA/wiki_00:
    wiki_extract_output_dir = os.path.join(tmp_dir_name, 'wiki')
    splitter = WikipediaExtractor.OutputSplitter(False, sys.maxsize, wiki_extract_output_dir)
    with bz2.open(local_file_name, 'rb') as f_in:    
      WikipediaExtractor.process_data(f_in, splitter)
    splitter.close()

    extract_file_name = os.path.join(wiki_extract_output_dir, 'AA', 'wiki_00')

    # ~12 GB
    print('second file is %s bytes' % os.path.getsize(extract_file_name))

    # finally, run combineWikiFiles.py to merge the two into final lines file, and keep only first three columns:
    subprocess.run([sys.executable,
                    'src/python/combineWikiFiles.py',
                    '-only-three-columns',
                    line_file_name,
                    extract_file_name,
                    final_line_file_name])

    # ~12 GB
    print('final file is %s bytes' % os.path.getsize(final_line_file_name))

if __name__ == '__main__':
  main()

                     

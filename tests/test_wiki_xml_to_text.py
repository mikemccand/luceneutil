import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SCRIPT = Path(__file__).resolve().parents[1] / "src" / "python" / "wikiXMLToText.py"
COMBINE = SCRIPT.with_name("combineWikiFiles.py")


class TestWikiXmlToText(unittest.TestCase):
  def test_cli_writes_utf8_line_docs(self):
    xml = """<mediawiki xmlns="http://www.mediawiki.org/xml/export-0.10/">
  <page>
    <title>東京</title>
    <revision>
      <timestamp>2020-01-02T03:04:05Z</timestamp>
      <contributor><username>Tester</username></contributor>
      <text>本文 [[Category:都市]]</text>
    </revision>
  </page>
</mediawiki>
"""
    with tempfile.TemporaryDirectory() as directory:
      source = Path(directory) / "wiki.xml"
      result = Path(directory) / "wiki.tsv"
      source.write_text(xml, encoding="utf-8")
      process = subprocess.run([sys.executable, str(SCRIPT), str(source), str(result)], capture_output=True, text=True, check=False)
      self.assertEqual(process.returncode, 0, process.stderr)
      header, row = result.read_text(encoding="utf-8").splitlines()

      extracted = Path(directory) / "extracted.txt"
      combined = Path(directory) / "combined.tsv"
      extracted.write_text('<doc id="1" title="東京">\n東京 \u2029 Clean body\n</doc>\n', encoding="utf-8")
      process = subprocess.run([sys.executable, str(COMBINE), str(result), str(extracted), str(combined)], capture_output=True, text=True, check=False)
      self.assertEqual(process.returncode, 0, process.stderr)
      combined_row = combined.read_text(encoding="utf-8").splitlines()[1]

    self.assertEqual(header.split("\t")[:4], ["FIELDS_HEADER_INDICATOR###", "title", "timestamp", "text"])
    fields = row.split("\t")
    self.assertEqual(fields[0], "東京")
    self.assertEqual(fields[2], "本文 [[Category:都市]]")
    self.assertEqual(fields[3], "Tester")
    self.assertEqual(fields[5], "都市")
    self.assertEqual(combined_row.split("\t")[2], "Clean body")


if __name__ == "__main__":
  unittest.main()

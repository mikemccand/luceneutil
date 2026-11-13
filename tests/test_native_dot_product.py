import re
import shutil
import subprocess
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RUNNING_MARKER = "RUNNING native vectorized code"


def java_major_version():
  """Returns the major version of the java on the path, or 0 if there is none."""
  if shutil.which("java") is None:
    return 0
  proc = subprocess.run(["java", "-version"], capture_output=True, text=True, check=False)
  match = re.search(r'version "(\d+)', proc.stderr)
  return int(match.group(1)) if match else 0


@unittest.skipUnless(shutil.which("gcc") and shutil.which("make"), "needs gcc and make")
@unittest.skipUnless(java_major_version() >= 22, "needs java 22+ (final foreign function api)")
class TestNativeDotProduct(unittest.TestCase):
  def test_make_then_run(self):
    # make native-run builds the .so/.dylib/.dll for this cpu then calls it from java
    proc = subprocess.run(["make", "native-run"], cwd=ROOT, capture_output=True, text=True, check=False)
    output = proc.stdout + proc.stderr
    self.assertEqual(0, proc.returncode, output)
    # the c code prints this the first time it is called, proving the native code ran
    self.assertIn(RUNNING_MARKER, output)
    self.assertIn("OK", proc.stdout)


if __name__ == "__main__":
  unittest.main()

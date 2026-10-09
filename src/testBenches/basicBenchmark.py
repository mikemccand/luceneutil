import os
import sys
import shutil

from pathlib import Path

"""
This is a very basic benchmark run used to make sure the index still builds and search still runs after
some change to benchmark code or lucene itself
"""

src_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
luceneutil_folder = os.path.dirname(src_folder)
python_folder = os.path.join(src_folder, "python")

# add python folder into source path so we can find them
if python_folder not in sys.path:
  sys.path.insert(0, python_folder)

import competition
import constants
import common
import benchUtil
from competition import Data

testData = Data("test10k", os.path.join(luceneutil_folder, "resources", "enwiki-20120502-test-10k.txt"), 10000, constants.WIKI_MEDIUM_TASKS_1MDOCS_FILE)
lucene_dir = common.getLuceneDirFromGradleProperties()

comp = competition.Competition(verifyCounts=True, taskRepeatCount=4, jvmCount=1)

index = comp.newIndex(
  lucene_dir,
  testData,
  addDVFields=True,
  useCMS=True,
  mergePolicy="TieredMergePolicy",
  facets=(
    ("taxonomy:Date", "Date"),
    ("taxonomy:Month", "Month"),
    ("taxonomy:DayOfYear", "DayOfYear"),
    ("sortedset:Date", "Date"),
    ("sortedset:Month", "Month"),
    ("sortedset:DayOfYear", "DayOfYear"),
    ("taxonomy:RandomLabel", "RandomLabel"),
    ("sortedset:RandomLabel", "RandomLabel"),
  ),
)

index_path = Path(benchUtil.nameToIndexPath(index.getName()))
# Remove previously created index to ensure we always reindex in test run
if index_path.is_dir():
  shutil.rmtree(index_path)

comp.competitor("left", lucene_dir, index=index, searchConcurrency=0)
comp.competitor("right", lucene_dir, index=index, searchConcurrency=0)

comp.benchmark("test_run")

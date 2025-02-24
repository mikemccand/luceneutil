#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import competition
import os

# Script to compare performance of sandbox and main facets modules
if __name__ == '__main__':
  parser = argparse.ArgumentParser(prog='Local Benchmark Run',
                                   description='Run a local benchmark on provided source dataset.')
  parser.add_argument('-s', '-source', '--source',
                      help='Data source to run the benchmark on.')
  parser.add_argument('-searchConcurrency', '--searchConcurrency', default='-1', type=int,
                      help='Search concurrency, 0 for disabled, -1 for using all cores')
  parser.add_argument('-l', '--lucene-dir', default=os.environ.get('BASELINE') or 'lucene_baseline',
                      help='Path to lucene repo to be used for comparison')
  args = parser.parse_args()
  print('Running benchmarks with the following args: %s' % args)

  sourceData = competition.sourceData(args.source)
  countsAreCorrect = args.searchConcurrency != 0
  # taskCountPerCat is high to run all tasks in the file
  comp =  competition.Competition(verifyCounts = not countsAreCorrect, taskCountPerCat = 100000)

  index = comp.newIndex(args.lucene_dir, sourceData,
                        addDVFields = True,
                        useCMS = True,
                        mergePolicy = 'TieredMergePolicy',
                        facets = (('taxonomy:Date', 'Date'),
                                  ('taxonomy:Month', 'Month'),
                                  ('taxonomy:DayOfYear', 'DayOfYear'),
                                  ('sortedset:Date', 'Date'),
                                  ('sortedset:Month', 'Month'),
                                  ('sortedset:DayOfYear', 'DayOfYear'),
                                  ('taxonomy:RandomLabel', 'RandomLabel'),
                                  ('sortedset:RandomLabel', 'RandomLabel')))
  # create a competitor named baseline with sources in the ../trunk folder
  comp.competitor('post_collection_facets', args.lucene_dir,
                  index = index, searchConcurrency = args.searchConcurrency,
                  testContext="facetMode:POST_COLLECTION", pk = False, groupByCat = True)
  comp.competitor('during_collection_facets', args.lucene_dir,
                  index = index, searchConcurrency = args.searchConcurrency,
                  testContext="facetMode:DURING_COLLECTION", pk = False, groupByCat = True)

  # start the benchmark - this can take long depending on your index and machines
  comp.benchmark("facet_implementations")

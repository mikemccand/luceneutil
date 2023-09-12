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

# simple example that runs benchmark with WIKI_MEDIUM source and taks files 
# Baseline here is ../lucene_baseline versus ../lucene_candidate
if __name__ == '__main__':
  parser = argparse.ArgumentParser(prog='Local Benchmark Run',
                                   description='Run a local benchmark on provided source dataset.')
  parser.add_argument('-s', '-source', '--source',
                      help='Data source to run the benchmark on.')
  parser.add_argument('-c', '-concurrentSearches', '--concurrentSearches', action='store_true',
                      help='Run concurrent searches')
  args = parser.parse_args()

  sourceData = competition.sourceData(args.source)
  comp =  competition.Competition()

  index = comp.newIndex('lucene_baseline', sourceData,
                        addDVFields = True,
                        facets = (('taxonomy:Date', 'Date'),
                                  ('taxonomy:Month', 'Month'),
                                  ('taxonomy:DayOfYear', 'DayOfYear'),
                                  ('sortedset:Date', 'Date'),
                                  ('sortedset:Month', 'Month'),
                                  ('sortedset:DayOfYear', 'DayOfYear'),
                                  ('taxonomy:RandomLabel', 'RandomLabel'),
                                  ('sortedset:RandomLabel', 'RandomLabel')))

  # create a competitor named baseline with sources in the ../trunk folder
  comp.competitor('baseline', 'lucene_baseline',
                  index = index, concurrentSearches = args.concurrentSearches)

  # use the same index here
  # create a competitor named my_modified_version with sources in the ../patch folder
  # note that we haven't specified an index here, luceneutil will automatically use the index from the base competitor for searching 
  # while the codec that is used for running this competitor is taken from this competitor.
  comp.competitor('my_modified_version', 'lucene_candidate',
                  index = index, concurrentSearches = args.concurrentSearches)

  # start the benchmark - this can take long depending on your index and machines
  comp.benchmark("baseline_vs_patch")
  

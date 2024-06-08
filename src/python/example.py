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
  parser.add_argument('-searchConcurrency', '--searchConcurrency',
                      help='Search concurrency, 0 for disabled, -1 for using all cores')
  parser.add_argument('-b', '--baseline', default='lucene_baseline',
                      help='Path to lucene repo to be used for baseline')
  parser.add_argument('-c', '--candidate', default='lucene_candidate',
                      help='Path to lucene repo to be used for candidate')
  parser.add_argument('-r', '--reindex', action='store_true',
                      help='Reindex data for candidate run')
  args = parser.parse_args()
  print('Running benchmarks with the following args: %s' % args)

  sourceData = competition.sourceData(args.source)
  comp =  competition.Competition()

  index = comp.newIndex(args.baseline, sourceData,
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
  comp.competitor('baseline', args.baseline,
                  index = index, searchConcurrency = args.searchConcurrency)

  # use the same index as baseline unless --reindex was passed.
  # create a competitor named my_modified_version (or provided candidate name) with sources in the ../patch folder
  # if --reindex flag is not used, luceneutil will automatically use the index from the base competitor for searching
  # while the codec that is used for running this competitor is taken from this competitor.
  candidate_index = index
  if args.reindex:
    candidate_index = comp.newIndex(args.candidate, sourceData,
                        addDVFields = True,
                        extraNamePart = 'candidate',
                        facets = (('taxonomy:Date', 'Date'),
                                  ('taxonomy:Month', 'Month'),
                                  ('taxonomy:DayOfYear', 'DayOfYear'),
                                  ('sortedset:Date', 'Date'),
                                  ('sortedset:Month', 'Month'),
                                  ('sortedset:DayOfYear', 'DayOfYear'),
                                  ('taxonomy:RandomLabel', 'RandomLabel'),
                                  ('sortedset:RandomLabel', 'RandomLabel')))
  comp.competitor('my_modified_version', args.candidate,
                  index = candidate_index, searchConcurrency = args.searchConcurrency)

  # start the benchmark - this can take long depending on your index and machines
  comp.benchmark("baseline_vs_patch")
  

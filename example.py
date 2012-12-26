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

from competition import Competition
from competition import WIKI_MEDIUM

# simple example that runs benchmark with WIKI_MEDIUM source and taks files 
# Baseline here is ../trunk versus ../patch
if __name__ == '__main__':
  # debug=True uses a smaller number of documents and less iterations when searching
  comp =  Competition(debug=True)

  index = comp.newIndex('trunk', WIKI_MEDIUM)
  # create a competitor named baseline with sources in the ../trunk folder
  comp.competitor('baseline', 'trunk',
                  index = index)

  # use the same index here
  # create a competitor named my_modified_version with sources in the ../patch folder
  # note that we haven't specified an index here, luceneutil will automatically use the index from the base competitor for searching 
  # while the codec that is used for running this competitor is taken from this competitor.
  comp.competitor('my_modified_version', 'patch',
                  index = index)

  # start the benchmark - this can take long depending on your index and machines
  comp.benchmark("trunk_vs_patch")
  

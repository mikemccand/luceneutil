#!/usr/bin/env/python

import os
import subprocess
import benchUtil
import constants

# Measure vector search recall and latency while exploring hyperparameters

# SETUP:
### Download and extract data files: Wikipedia line docs + GloVe
# python src/python/setup.py
# cd ../data
# unzip glove.6B.zip
# unlzma enwiki-20120502-lines-1k.txt.lzma
### Create document and task vectors
# ant vectors100

LUCENE_CHECKOUT = 'lucene_candidate'

PARAMS = ('ndoc', 'maxConn', 'beamWidthIndex', 'fanout')
VALUES = {
    'ndoc': (10000, 100000, 1000000),
    'maxConn': (32, 64, 96),
    'beamWidthIndex': (250, 500),
    'fanout': (20, 100, 250)
}

indexes = [0, 0, 0, -1]

def advance(ix):
    for i in reversed(range(len(ix))):
        param = PARAMS[i]
        j = ix[i] + 1
        if ix[i] == len(VALUES[param]) - 1:
            ix[i] = 0
        else:
            ix[i] += 1
            return True
    return False

def benchmark_knn(checkout):
    last_indexes = (-1, -1, -1)
    print("recall\tlatency\tnDoc\tfanout\tmaxConn\tbeamWidth\tvisited\tindex ms")
    cp = benchUtil.classPathToString(benchUtil.getClassPath(checkout))
    while advance(indexes):
        params = {}
        for (i, p) in enumerate(PARAMS):
            value = VALUES[p][indexes[i]]
            #print(p + ' ' + str(value))
            params[p] = value
        #print(params)
        args = [a for (k, v) in params.items() for a in ('-' + k, str(v))]
        if last_indexes != indexes[:3]:
            last_indexes = indexes[:3]
            args += [ '-reindex' ]

        docVectors = '%s/data/enwiki-20120502-lines-1k-100d.vec' % constants.BASE_DIR
        queryVectors = '%s/util/tasks/vector-task-100d.vec' % constants.BASE_DIR
        cmd = ['java',
               '-cp', cp,
               'org.apache.lucene.util.hnsw.KnnGraphTester'] + args + [
                   '-quiet',
                   '-dim', '100',
                   '-docs', docVectors,
                   '-search', queryVectors]
        #print(cmd)
        subprocess.run(cmd)

benchmark_knn(LUCENE_CHECKOUT)

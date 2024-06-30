#!/usr/bin/env/python

import subprocess
import benchUtil
import constants

# Measure vector search recall and latency while exploring hyperparameters

# SETUP:
### Download and extract data files: Wikipedia line docs + GloVe
# python src/python/setup.py -download
# cd ../data
# unzip glove.6B.zip
# unlzma enwiki-20120502-lines-1k.txt.lzma
### Create document and task vectors
# ant vectors100
#
# then run this file: python src/python/knnPerfTest.py
#
# you may want to modify the following settings:


# Where the version of Lucene is that will be tested. Expected to be in the base dir above luceneutil.
#LUCENE_CHECKOUT = 'baseline'
# LUCENE_CHECKOUT = 'candidate'
LUCENE_CHECKOUT = 'trunk'

# e.g. to compile KnnIndexer:
#
#   javac -d build -cp /l/trunk/lucene/core/build/libs/lucene-core-10.0.0-SNAPSHOT.jar:/l/trunk/lucene/join/build/libs/lucene-join-10.0.0-SNAPSHOT.jar src/main/knn/*.java src/main/WikiVectors.java src/main/perf/VectorDictionary.java
#

# test parameters. This script will run KnnGraphTester on every combination of these parameters
VALUES = {
    #'ndoc': (10000, 100000, 1000000),
    #'ndoc': (10000, 100000, 200000, 500000),
    #'ndoc': (10000, 100000, 200000, 500000),
    'ndoc': (250_000,),
    #'ndoc': (100000,),
    #'maxConn': (32, 64, 96),
    'maxConn': (64, ),
    #'beamWidthIndex': (250, 500),
    'beamWidthIndex': (250, ),
    #'fanout': (20, 100, 250)
    'fanout': (20,),
    #'quantize': None,
    'quantizeBits': (32, 4, 7, 8),
    'numMergeWorker': (12,),
    'numMergeThread': (4,),
    'encoding': ('float32',),
    #'quantize': (True,),
    #'fanout': (0,),
    #'topK': (10,),
    #'niter': (10,),
}

def advance(ix, values):
    for i in reversed(range(len(ix))):
        # scary to rely on dict key enumeration order?  but i guess if dict never changes while we do this, it's stable?
        param = list(values.keys())[i]
        #print("advance " + param)
        if type(values[param]) in (list, tuple) and ix[i] == len(values[param]) - 1:
            ix[i] = 0
        else:
            ix[i] += 1
            return True
    return False

def run_knn_benchmark(checkout, values):
    indexes = [0] * len(values.keys())
    indexes[-1] = -1
    args = []
    #dim = 100
    #doc_vectors = constants.GLOVE_VECTOR_DOCS_FILE
    #query_vectors = '%s/luceneutil/tasks/vector-task-100d.vec' % constants.BASE_DIR
    dim = 768
    doc_vectors = '%s/data/enwiki-20120502-lines-1k-mpnet.vec' % constants.BASE_DIR
    query_vectors = '%s/luceneutil/tasks/vector-task-mpnet.vec' % constants.BASE_DIR
    query_vectors = doc_vectors
    #dim = 384
    #doc_vectors = '%s/data/enwiki-20120502-lines-1k-minilm.vec' % constants.BASE_DIR
    #query_vectors = '%s/luceneutil/tasks/vector-task-minilm.vec' % constants.BASE_DIR
    #dim = 300
    #doc_vectors = '%s/data/enwiki-20120502-lines-1k-300d.vec' % constants.BASE_DIR
    #query_vectors = '%s/luceneutil/tasks/vector-task-300d.vec' % constants.BASE_DIR
    #dim = 256
    #doc_vectors = '/d/electronics_asin_emb.bin'
    #query_vectors = '/d/electronics_query_vectors.bin'

    # Cohere dataset
    #dim = 768
    #doc_vectors = '%s/data/cohere-wikipedia-768.vec' % constants.BASE_DIR
    #query_vectors = '%s/data/cohere-wikipedia-queries-768.vec' % constants.BASE_DIR
    cp = benchUtil.classPathToString(benchUtil.getClassPath(checkout))
    cmd = constants.JAVA_EXE.split(' ') + ['-cp', cp,
           '--add-modules', 'jdk.incubator.vector',
           '-Dorg.apache.lucene.store.MMapDirectory.enableMemorySegments=false',
           'knn.KnnGraphTester']
    print("recall\tlatency\tnDoc\tfanout\tmaxConn\tbeamWidth\tvisited\tindex ms")
    while advance(indexes, values):
        print('\nNEXT:')
        pv = {}
        args = []
        do_quantize = False
        for (i, p) in enumerate(values.keys()):
            if values[p]:
                value = values[p][indexes[i]]
                if p == 'quantizeBits':
                    if value != 32:
                        pv[p] = value
                        print(f'  -{p}={value}')
                        print(f'  -quantize')
                        args += ['-quantize']
                else:
                    print(f'  -{p}={value}')
                    pv[p] = value
            else:
                args += ['-' + p]
                print(f'  -{p}')
        args += [a for (k, v) in pv.items() for a in ('-' + k, str(v)) if a]

        this_cmd = cmd + args + [
            '-dim', str(dim),
            '-docs', doc_vectors,
            '-reindex',
            '-search', query_vectors,
            # '-numMergeThread', '8', '-numMergeWorker', '8',
            # '-forceMerge',
            '-quiet']
        print(f'  cmd: {this_cmd}')
        subprocess.run(this_cmd, check=True)


run_knn_benchmark(LUCENE_CHECKOUT, VALUES)

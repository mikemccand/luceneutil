/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package perf;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.queryparser.classic.QueryParser;

import java.io.IOException;
import java.util.Random;

/**
 * Create TaskParser, this class should be thread-safe
 */
public class TaskParserFactory {
    private final IndexState indexState;
    private final String field;
    private final String fieldForQueryParser;
    private final Analyzer analyzer;
    private final int topN;
    private final Random random;
    private final VectorDictionary vectorDictionary;
    private final boolean doStoredLoads;

    public TaskParserFactory(IndexState state,
                             String field,
                             Analyzer analyzer,
                             String fieldForQueryParser,
                             int topN,
                             Random random,
                             VectorDictionary vectorDictionary,
                             boolean doStoredLoads) {
        this.indexState = state;
        this.field = field;
        this.fieldForQueryParser = fieldForQueryParser;
        this.analyzer = analyzer;
        this.topN = topN;
        this.random = random;
        this.vectorDictionary = vectorDictionary;
        this.doStoredLoads = doStoredLoads;
    }

    public TaskParser getTaskParser() throws IOException {
        QueryParser qp = new QueryParser(fieldForQueryParser, analyzer);
        return new TaskParser(indexState, qp, field, topN, random, vectorDictionary, doStoredLoads);
    }
}

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
    private final VectorDictionary<?> vectorDictionary;
    private final boolean doStoredLoads;

    public TaskParserFactory(IndexState state,
                             String field,
                             Analyzer analyzer,
                             String fieldForQueryParser,
                             int topN,
                             Random random,
                             VectorDictionary<?> vectorDictionary,
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

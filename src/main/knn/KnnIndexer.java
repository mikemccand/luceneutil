package knn;

import knn.KnnGraphTester;
import knn.VectorReader;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.lucene95.Lucene95Codec;
import org.apache.lucene.codecs.lucene95.Lucene95HnswVectorsFormat;
import org.apache.lucene.document.*;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.PrintStreamInfoStream;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;

public class KnnIndexer {

    Path docsPath;
    Path indexPath;
    int maxConn;
    int beamWidth;
    VectorEncoding vectorEncoding;
    int dim;
    VectorSimilarityFunction similarityFunction;
    int numDocs;
    boolean quiet;

    public KnnIndexer(Path docsPath, Path indexPath, int maxConn, int beamWidth, VectorEncoding vectorEncoding, int dim,
                      VectorSimilarityFunction similarityFunction, int numDocs, boolean quiet) {
        this.docsPath = docsPath;
        this.indexPath = indexPath;
        this.maxConn = maxConn;
        this.beamWidth = beamWidth;
        this.vectorEncoding = vectorEncoding;
        this.dim = dim;
        this.similarityFunction = similarityFunction;
        this.numDocs = numDocs;
        this.quiet = quiet;
    }

    public int createIndex() throws IOException {
        IndexWriterConfig iwc = new IndexWriterConfig().setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        iwc.setCodec(
                new Lucene95Codec() {
                    @Override
                    public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                        return new Lucene95HnswVectorsFormat(maxConn, beamWidth);
                    }
                });
        // iwc.setMergePolicy(NoMergePolicy.INSTANCE);
        iwc.setRAMBufferSizeMB(1994d);
        iwc.setUseCompoundFile(false);
        // iwc.setMaxBufferedDocs(10000);

        FieldType fieldType =
                switch (vectorEncoding) {
                    case BYTE -> KnnByteVectorField.createFieldType(dim, similarityFunction);
                    case FLOAT32 -> KnnFloatVectorField.createFieldType(dim, similarityFunction);
                };
        if (quiet == false) {
            iwc.setInfoStream(new PrintStreamInfoStream(System.out));
            System.out.println("creating index in " + indexPath);
        }
        long start = System.nanoTime();
        try (FSDirectory dir = FSDirectory.open(indexPath);
             IndexWriter iw = new IndexWriter(dir, iwc)) {
            try (FileChannel in = FileChannel.open(docsPath)) {
                VectorReader vectorReader = VectorReader.create(in, dim, vectorEncoding);
                for (int i = 0; i < numDocs; i++) {
                    Document doc = new Document();
                    switch (vectorEncoding) {
                        case BYTE -> doc.add(
                                new KnnByteVectorField(
                                        KnnGraphTester.KNN_FIELD, ((VectorReaderByte) vectorReader).nextBytes(), fieldType));
                        case FLOAT32 -> doc.add(
                                new KnnFloatVectorField(KnnGraphTester.KNN_FIELD, vectorReader.next(), fieldType));
                    }
                    doc.add(new StoredField(KnnGraphTester.ID_FIELD, i));
                    iw.addDocument(doc);

                    if (quiet == false && i % 10000 == 0) {
                        System.out.println("Done indexing " + (i+1) + " documents.");
                    }
                }
                if (quiet == false) {
                    System.out.println("Done indexing " + numDocs + " documents; now flush");
                }
            }
        }
        long elapsed = System.nanoTime() - start;
        if (quiet == false) {
            System.out.println(
                    "Indexed " + numDocs + " documents in " + TimeUnit.NANOSECONDS.toSeconds(elapsed) + "s");
        }
        return (int) TimeUnit.NANOSECONDS.toMillis(elapsed);
    }
}

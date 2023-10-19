package knn;

import org.apache.lucene.index.VectorEncoding;
import org.apache.lucene.index.VectorSimilarityFunction;

import java.io.IOException;
import java.nio.file.Path;

public class KnnIndexerMain {
    public Path docVectorsPath;
    public Path indexPath;
    public int maxConn;
    public int beamWidth;
    public VectorEncoding vectorEncoding;
    public int dimension;
    public VectorSimilarityFunction similarityFunction;
    public int numDocs;
    boolean quiet = false;

    @Override
    public String toString() {
        return "KnnIndexerMain {" +
                "docVectorsPath='" + docVectorsPath + '\'' +
                ", indexPath='" + indexPath + '\'' +
                ", maxConn=" + maxConn +
                ", beamWidth=" + beamWidth +
                ", vectorEncoding=" + vectorEncoding +
                ", dimension=" + dimension +
                ", similarityFunction=" + similarityFunction +
                ", numDocs=" + numDocs +
                ", quiet=" + quiet +
                '}';
    }

    public static void main(String[] args) throws IOException {
        KnnIndexerMain inputs = new KnnIndexerMain();

        try {
            int i = 0;
            while (i < args.length) {
                switch (args[i].toLowerCase()) {
                    case "-docvectorspath" -> inputs.docVectorsPath = Path.of(args[++i]);
                    case "-indexpath" -> inputs.indexPath = Path.of(args[++i]);
                    case "-maxconn" -> inputs.maxConn = Integer.parseInt(args[++i]);
                    case "-beamwidth" -> inputs.beamWidth = Integer.parseInt(args[++i]);
                    case "-vectorencoding" -> inputs.vectorEncoding = VectorEncoding.valueOf(args[++i]);
                    case "-similarityfunction" -> inputs.similarityFunction = VectorSimilarityFunction.valueOf(args[++i].toUpperCase());
                    case "-numdocs" -> inputs.numDocs = Integer.parseInt(args[++i]);
                    case "-dimension" -> inputs.dimension = Integer.parseInt(args[++i]);
                    case "-quiet" -> inputs.quiet = true;
                    default -> throw new IllegalArgumentException("Cannot recognize the option " + args[i]);
                }
                i++;
            }
        }catch (Exception e) {
            e.printStackTrace();
            System.out.println("Please follow correct usage guidelines:" + inputs.usage());
        }

        if(!inputs.quiet) {
            System.out.println("Creating index with following configurations : " + inputs);
        }

        new KnnIndexer(inputs.docVectorsPath, inputs.indexPath, inputs.maxConn, inputs.beamWidth, inputs.vectorEncoding,
               inputs.dimension, inputs.similarityFunction, inputs.numDocs, inputs.quiet).createIndex();

        if(!inputs.quiet) {
            System.out.println("Successfully created index.");
        }
    }

    public String usage() {
        return "KnnIndexerMain \n" +
                "\t -docVectorsPath : path of the file containing vectors for document. <TODO: what format?>\n"  +
                "\t -indexPath : path of the folder/dir where the index has to be created.\n" +
                "\t -maxConn : maximum connections per node for HNSW graph\n" +
                "\t -beamWidth : beam-width at graph creation time. Same as efConstruction in the HNSW paper.\n" +
                "\t -vectorEncoding: vector encoding. one of constant 'BYTE' or 'FLOAT32'\n" +
                "\t -dimension : dimension / size of the vectors \n" +
                "\t -similarityFunction : similarity function for vector comparison. One of ( EUCLIDEAN, DOT_PRODUCT, COSINE, MAXIMUM_INNER_PRODUCT )\n" +
                "\t -numDocs : number of document vectors to be used from the file\n" +
                "\t -quiet : don't print anything on console if mentioned.\n" ;
    }
}

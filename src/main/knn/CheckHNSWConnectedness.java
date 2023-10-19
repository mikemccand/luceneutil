package knn;

import org.apache.lucene.codecs.KnnVectorsReader;
import org.apache.lucene.codecs.lucene95.Lucene95HnswVectorsReader;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.index.*;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.hnsw.HnswGraph;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.*;

import static org.apache.lucene.search.DocIdSetIterator.NO_MORE_DOCS;

public class CheckHNSWConnectedness {
    private static int getReachableNodes(HnswGraph graph, int level) throws IOException {
        Set<Integer> visited = new HashSet<>();
        Stack<Integer> candidates = new Stack<>();
        candidates.push(graph.entryNode());

        while (!candidates.isEmpty()) {
            int node = candidates.pop();

            if (visited.contains(node)) {
                continue;
            }

            visited.add(node);
            graph.seek(level, node);

            int friendOrd;
            while ((friendOrd = graph.nextNeighbor()) != NO_MORE_DOCS) {
                candidates.push(friendOrd);
            }
        }
        return visited.size();
    }

    private static int getOverallReachableNodes(HnswGraph graph, int numLevels) throws IOException {
        Set<Integer> visited = new HashSet<>();
        Stack<Integer> candidates = new Stack<>();
        candidates.push(graph.entryNode());

        while (!candidates.isEmpty()) {
            int node = candidates.pop();

            if (visited.contains(node)) {
                continue;
            }

            visited.add(node);
            // emulate collapsing all the edges
            for (int level = 0; level < numLevels; level++) {
                try {
                    graph.seek(level, node); // run with -ea (Assertions enabled)
                    int friendOrd;
                    while ((friendOrd = graph.nextNeighbor()) != NO_MORE_DOCS) {
                        candidates.push(friendOrd);
                    }
                } catch (AssertionError ae) { // TODO: seriously? Bad but have to do it for this test, as the API does not throw error.
//                    You will get this error when the graph.seek() tries to seek a node which is not present in that level.
                }
            }
        }
        return visited.size();
    }

    public static List<int[]> checkConnected(String index, String hnswField) throws IOException, NoSuchFieldException, IllegalAccessException {
        List<int[]> nodeCounts = new ArrayList<>();
        try (FSDirectory dir = FSDirectory.open(Paths.get(index));
             IndexReader indexReader = DirectoryReader.open(dir)) {
            for (LeafReaderContext ctx : indexReader.leaves()) {
                KnnVectorsReader reader = ((PerFieldKnnVectorsFormat.FieldsReader) ((SegmentReader) ctx.reader()).getVectorReader()).getFieldReader(hnswField);

                if (reader != null) {
                    HnswGraph graph = ((Lucene95HnswVectorsReader) reader).getGraph(hnswField);
                    for (int l = 0; l < graph.numLevels(); l++) {
                        int reachableNodes = getReachableNodes(graph, l);
                        int graphSize = graph.getNodesOnLevel(l).size();
                        nodeCounts.add(new int[]{graphSize, reachableNodes});
                    }

                    int totalReachable = getOverallReachableNodes(graph, graph.numLevels());
                    nodeCounts.add(new int[]{graph.size(), totalReachable});
                }
            }
        }

        return nodeCounts;
    }

    public static void main(String[] args) throws IOException, NoSuchFieldException, IllegalAccessException {
        boolean assertOn = false;
        // *assigns* true if assertions are on.
        assert assertOn = true;

        if (!assertOn) {
            throw new IllegalArgumentException("You must enable assertions by setting -ea option to jvm arg for this program to work correctly.");
        }

        String index = args[0];
        String field = args[1];
        System.out.println("For index " + index + " field : " + field);
        List<int[]> nodeCounts = checkConnected(index, field);

        for (int i = 0; i < nodeCounts.size(); i++) {
            int[] counts = nodeCounts.get(i);
            int disconnectedNodeCount = counts[0] - counts[1];
            if (i < nodeCounts.size() - 1) {
                System.out.println("Level = " + i + "\tTotal Nodes = " + counts[0] + "\tReachable Nodes = " + counts[1] +
                        "\tUnreachable Nodes = " + (disconnectedNodeCount) + "\t%Disconnectedness = " + (disconnectedNodeCount * 100f / counts[0]));
            } else {
                // this is the last entry so it has overall connectivity data
                System.out.println("Overall" + "\tTotal Nodes = " + counts[0] + "\tReachable Nodes = " + counts[1] +
                        "\tUnreachable Nodes = " + (disconnectedNodeCount) + "\t%Disconnectedness = " + (disconnectedNodeCount * 100f / counts[0]));
            }
        }
    }
}

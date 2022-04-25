package perf.facets;

/**
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

// javac -d ../../../../build -cp ../../../../../../lucene/lucene/core/build/libs/lucene-core-10.0.0-SNAPSHOT.jar:../../../../../../lucene/lucene/facet/build/libs/lucene-facet-10.0.0-SNAPSHOT.jar:../../../../build BenchmarkFacets.java

// java -cp ../../../../../../lucene/lucene/core/build/libs/lucene-core-10.0.0-SNAPSHOT.jar:../../../../../../lucene/lucene/facet/build/libs/lucene-facet-10.0.0-SNAPSHOT.jar:../../../../build perf.facets.BenchmarkFacets

// java -cp ../../../../../../lucene/lucene/core/build/libs/lucene-core-10.0.0-SNAPSHOT.jar:../../../../../../lucene/lucene/facet/build/libs/lucene-facet-10.0.0-SNAPSHOT.jar:../../../../build perf.facets.BenchmarkFacets ../../../../../indices/NADFacets
import java.io.File;
import java.io.IOException;

import java.util.List;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.facet.Facets;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.sortedset.DefaultSortedSetDocValuesReaderState;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetCounts;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesReaderState;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.FastTaxonomyFacetCounts;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.ReferenceManager;
import org.apache.lucene.store.Directory;

import perf.OpenDirectory;

public class BenchmarkFacets {

    public static double MILLION = 1_000_000;

    public static String SSDV_FIELD_NAME = "address.sortedset";
    public static String TAXO_FIELD_NAME = "address.taxonomy";

    public static void main(String[] args) throws IOException {
        File indexPath = new File(args[0]);

        final Directory dir;
        OpenDirectory od = OpenDirectory.get("NIOFSDirectory");
        dir = od.open(indexPath.toPath());

        IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
        IndexWriter writer = new IndexWriter(dir, iwc);

        final ReferenceManager<IndexSearcher> mgr = new SearcherManager(writer, new SearcherFactory() {
            @Override
            public IndexSearcher newSearcher(IndexReader reader, IndexReader previous) {
                IndexSearcher s = new IndexSearcher(reader);
                s.setQueryCache(null); // don't bench the cache
                return s;
            }
        });

        IndexSearcher s = mgr.acquire();

        final Directory taxoDir;
        File taxoPath = new File(args[0] + "/taxonomy");
        taxoDir = od.open(taxoPath.toPath());
        DirectoryTaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir);

        FacetsConfig config = new FacetsConfig();
        config.setIndexFieldName(TAXO_FIELD_NAME, TAXO_FIELD_NAME);
        config.setHierarchical(TAXO_FIELD_NAME, true);
        config.setHierarchical(SSDV_FIELD_NAME, true);

        try {
            // SSDV state creation
            SortedSetDocValuesReaderState state = new DefaultSortedSetDocValuesReaderState(s.getIndexReader(), config);

            System.out.println("Warming up...");

            // Warmup
            for (int i = 0; i < 5; i++) {
                FacetsCollector c = new FacetsCollector();
                s.search(new MatchAllDocsQuery(), c);
                Facets taxoFacets = new FastTaxonomyFacetCounts(TAXO_FIELD_NAME, taxoReader, config, c);
                Facets ssdvFacets = new SortedSetDocValuesFacetCounts(state, c);
                taxoFacets.getAllDims(5);
                ssdvFacets.getAllDims(5);
                taxoFacets.getTopChildren(10, TAXO_FIELD_NAME, "TX");
                ssdvFacets.getTopChildren(10, SSDV_FIELD_NAME, "TX");
            }

            System.out.println("Number of docs: " + s.getIndexReader().numDocs());
            System.out.println("Number of segments: " + s.getIndexReader().leaves().size());

            System.out.println("Running benchmark...");

            int numIters = 20;

            double totalCreateFastTaxoCountsTimeMS = 0;
            double totalCreateSSDVCountsTimeMS = 0;
            double totalTaxoGetAllDimsTimeMS = 0;
            double totalSSDVGetAllDimsTimeMS = 0;
            double totalTaxoGetChildrenTimeMS = 0;
            double totalSSDVGetChildrenTimeMS = 0;

            for (int i = 1; i < numIters + 1; i++) {
                FacetsCollector c = new FacetsCollector();
                s.search(new MatchAllDocsQuery(), c);

                long createFastTaxoCountsStartNS = System.nanoTime();
                Facets taxoFacets = new FastTaxonomyFacetCounts(TAXO_FIELD_NAME, taxoReader, config, c);
                long createFastTaxoCountsEndNS = System.nanoTime();
                double createFastTaxoCountsTimeMS = (double)(createFastTaxoCountsEndNS - createFastTaxoCountsStartNS) / MILLION;
                totalCreateFastTaxoCountsTimeMS += createFastTaxoCountsTimeMS;

                long createSSDVReaderCountsStartNS = System.nanoTime();
                Facets ssdvFacets = new SortedSetDocValuesFacetCounts(state, c);
                long createSSDVReaderCountsEndNS = System.nanoTime();
                double createSSDVReaderStateTimeMS = (double)(createSSDVReaderCountsEndNS - createSSDVReaderCountsStartNS) / MILLION;
                totalCreateSSDVCountsTimeMS += createSSDVReaderStateTimeMS;

                long taxoGetAllDimsStartNS = System.nanoTime();
                List<FacetResult> results = taxoFacets.getAllDims(5);
                long taxoGetAllDimsEndNS = System.nanoTime();
                double taxoGetAllDimsTimeMS = (double) (taxoGetAllDimsEndNS - taxoGetAllDimsStartNS) / MILLION;
                totalTaxoGetAllDimsTimeMS += taxoGetAllDimsTimeMS;

                long ssdvGetAllDimsStartNS = System.nanoTime();
                results = ssdvFacets.getAllDims(5);
                long ssdvGetAllDimsEndNS = System.nanoTime();
                double ssdvGetAllDimsTimeMS = (double) (ssdvGetAllDimsEndNS - ssdvGetAllDimsStartNS) / MILLION;
                totalSSDVGetAllDimsTimeMS += ssdvGetAllDimsTimeMS;

                long taxoGetChildrenStartNS = System.nanoTime();
                FacetResult result = taxoFacets.getTopChildren(10, TAXO_FIELD_NAME, "TX");
                long taxoGetAllChildrenEndNS = System.nanoTime();
                double taxoGetAllChildrenTimeMS = (double) (taxoGetAllChildrenEndNS - taxoGetChildrenStartNS) / MILLION;
                totalTaxoGetChildrenTimeMS += taxoGetAllChildrenTimeMS;

                long ssdvGetChildrenStartNS = System.nanoTime();
                result = ssdvFacets.getTopChildren(10, SSDV_FIELD_NAME, "TX");
                long ssdvGetChildrenEndNS = System.nanoTime();
                double ssdvGetAllChildrenTimeMS = (double) (ssdvGetChildrenEndNS - ssdvGetChildrenStartNS) / MILLION;
                totalSSDVGetChildrenTimeMS += ssdvGetAllChildrenTimeMS;

                System.out.println("Results after iteration " + i + "\n");

                System.out.println("Time (ms) taken to instanciate FastTaxonomyFacetCounts: " + totalCreateFastTaxoCountsTimeMS / (double) i);
                System.out.println("Time (ms) taken to instanciate SSDVFacetCounts: " + totalCreateSSDVCountsTimeMS / (double) i);
                reportPercentDifference(totalCreateFastTaxoCountsTimeMS / (double) i, totalCreateSSDVCountsTimeMS / (double) i);
                System.out.println("");

                System.out.println("Time (ms) taken to get all dims for taxonomy: " + totalTaxoGetAllDimsTimeMS / (double) i);
                System.out.println("Time (ms) taken to get all dims for SSDV: " + totalSSDVGetAllDimsTimeMS / (double) i);
                reportPercentDifference(totalTaxoGetAllDimsTimeMS / (double) i, totalSSDVGetAllDimsTimeMS / (double) i);
                System.out.println("");

                System.out.println("Time (ms) taken to get top children of \"address.taxonomy/TX\" children for taxonomy: " + totalTaxoGetChildrenTimeMS / (double) i);
                System.out.println("Time (ms) taken to get top \"address.sortedset/TX\" children for SSDV: " + totalSSDVGetChildrenTimeMS / (double) i);
                reportPercentDifference(totalTaxoGetChildrenTimeMS / (double) i, totalSSDVGetChildrenTimeMS / (double) i);

                System.out.print("\n");
            }
        } finally {
            mgr.release(s);
        }
    }

    public static void reportPercentDifference(double taxo, double ssdv) {
        if (taxo == ssdv) {
            System.out.println("No difference");
            return;
        }
        double larger = taxo > ssdv ? taxo : ssdv;
        double smaller = taxo > ssdv ? ssdv : taxo;
        double percent = ((larger - smaller) / smaller) * 100;
        String percentString = String.format("%.1f", percent);
        if (taxo == larger) {
            System.out.println("Taxonomy is " + percentString + "% slower than SSDV");
        } else {
            System.out.println("SSDV is " + percentString + "% slower than Taxonomy");
        }
    }
}
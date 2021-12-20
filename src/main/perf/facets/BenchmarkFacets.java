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

    public static double BILLION = 1000000000;

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
        config.setIndexFieldName("address.taxonomy", "address.taxonomy");
        config.setHierarchical("address.taxonomy", true);

        try {
            System.out.println("Number of docs: " + s.getIndexReader().numDocs());
            System.out.println("Number of segments: " + s.getIndexReader().leaves().size());
            String ssdvFieldName = "address.sortedset";
            String taxoFieldName = "address.taxonomy";

            System.out.println("Testing facet instanciation...");

            FacetsCollector c = new FacetsCollector();
            s.search(new MatchAllDocsQuery(), c);

            long createFastTaxoCountsStart = System.nanoTime();
            Facets taxoFacets = new FastTaxonomyFacetCounts(taxoFieldName, taxoReader, config, c);
            long createFastTaxoCountsEnd = System.nanoTime();
            double createFastTaxoCountsTime = (double)(createFastTaxoCountsEnd - createFastTaxoCountsStart) / BILLION;

            // only benchmark count creation
            SortedSetDocValuesReaderState state = new DefaultSortedSetDocValuesReaderState(s.getIndexReader());
            long createSSDVReaderStateStart = System.nanoTime();
            Facets ssdvFacets = new SortedSetDocValuesFacetCounts(state, c);
            long createSSDVReaderStateEnd = System.nanoTime();
            double createSSDVReaderStateTime = (double)(createSSDVReaderStateEnd - createSSDVReaderStateStart) / BILLION;

            System.out.println("Time (s) taken to instanciate FastTaxonomyFacetCounts: " + createFastTaxoCountsTime);
            System.out.println("Time (s) taken to instanciate SSDVFacetCounts: " + createSSDVReaderStateTime);
            System.out.println("Percent difference: " + getPercentDiff(createFastTaxoCountsTime, createSSDVReaderStateTime) + "%");

            // test getAllDims (there is only 1 dim, address.taxonomy or sortedset.taxonomy)
            System.out.println("Testing getAllDims...");

            long taxoGetAllDimsStart = System.nanoTime();
            List<FacetResult> results = taxoFacets.getAllDims(5);
            long taxoGetAllDimsEnd = System.nanoTime();
            double taxoGetAllDimsTime = (double) (taxoGetAllDimsEnd - taxoGetAllDimsStart) / BILLION;

            long ssdvGetAllDimsStart = System.nanoTime();
            results = ssdvFacets.getAllDims(5);
            long ssdvGetAllDimsEnd = System.nanoTime();
            double ssdvGetAllDimsTime = (double) (ssdvGetAllDimsEnd - ssdvGetAllDimsStart) / BILLION;

            System.out.println("Time (s) taken to get all dims for taxonomy: " + taxoGetAllDimsTime);
            System.out.println("Time (s) taken to get all dims for SSDV: " + ssdvGetAllDimsTime);
            System.out.println("Percent difference: " + getPercentDiff(taxoGetAllDimsTime, ssdvGetAllDimsTime) + "%");

            // test get top children
            System.out.println("Testing getTopChildren...");

            long taxoGetChildrenStart = System.nanoTime();
            FacetResult result = taxoFacets.getTopChildren(10, taxoFieldName, "TX");
            long taxoGetAllChildrenEnd = System.nanoTime();
            double taxoGetAllChildrenTime = (double) (taxoGetAllChildrenEnd - taxoGetAllDimsStart) / BILLION;

            long ssdvGetChildrenStart = System.nanoTime();
            result = ssdvFacets.getTopChildren(10, ssdvFieldName, "TX");
            long ssdvGetChildrenEnd = System.nanoTime();
            double ssdvGetAllChildrenTime = (double) (ssdvGetChildrenEnd - ssdvGetAllDimsStart) / BILLION;

            System.out.println("Time (s) taken to get all dims for taxonomy: " + taxoGetAllChildrenTime);
            System.out.println("Time (s) taken to get all dims for SSDV: " + ssdvGetAllChildrenTime);
            System.out.println("Percent difference: " + getPercentDiff(taxoGetAllChildrenTime, ssdvGetAllChildrenTime) + "%");

        } finally {
            mgr.release(s);
        }
    }

    public static double getPercentDiff(double a, double b) {
        if (a == b) {
            return 100.0;
        }
        double larger = a > b ? a : b;
        double smaller = a > b ? b : a;
        return (larger / smaller) * 100;
    }
}
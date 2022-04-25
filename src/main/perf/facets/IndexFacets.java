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

// javac -d ../../../../build -cp ../../../../../../lucene/lucene/core/build/libs/lucene-core-10.0.0-SNAPSHOT.jar:../../../../../../lucene/lucene/facet/build/libs/lucene-facet-10.0.0-SNAPSHOT.jar:../../../../build IndexFacets.java

// java -cp ../../../../../../lucene/lucene/core/build/libs/lucene-codecs-10.0.0-SNAPSHOT.jar:../../../../../../lucene/lucene/facet/build/libs/lucene-facet-10.0.0-SNAPSHOT.jar:../../../../build perf.facets.IndexFacets

// for indexing NAD facets: java -cp ../../../../../../lucene/lucene/core/build/libs/lucene-core-10.0.0-SNAPSHOT.jar:../../../../../../lucene/lucene/facet/build/libs/lucene-facet-10.0.0-SNAPSHOT.jar:../../../../build perf.facets.IndexFacets ../../../../../data/NAD_taxonomy.txt.gz ../../../../../indices/NADFacets

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.IOException;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.nio.file.Path;
import java.io.Reader;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.Enumeration;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Stream;
import java.util.zip.GZIPInputStream;
import java.util.Comparator;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.sortedset.SortedSetDocValuesFacetField;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;

import perf.OpenDirectory;

public class IndexFacets {

    public static void main(String args[]) throws IOException {
        String facetFile = args[0];
        File indexPath = new File(args[1]);

        Path checkPath = indexPath.toPath();
        deleteDirectoryIfExists(checkPath);

        final Directory dir;
        OpenDirectory od = OpenDirectory.get("NIOFSDirectory");
        dir = od.open(indexPath.toPath());

        IndexWriterConfig iwc = new IndexWriterConfig(new StandardAnalyzer());
        IndexWriter writer = new IndexWriter(dir, iwc);

        final Directory taxoDir;
        File taxoPath = new File(args[1] + "/taxonomy");
        taxoDir = od.open(taxoPath.toPath());
        TaxonomyWriter taxo = new DirectoryTaxonomyWriter(taxoDir, IndexWriterConfig.OpenMode.CREATE);

        InputStream fileStream = new FileInputStream(facetFile);
        InputStream gzipStream = new GZIPInputStream(fileStream);
        Reader decoder = new InputStreamReader(gzipStream, "utf8");
        BufferedReader buffered = new BufferedReader(decoder);
        Stream<String> lines = buffered.lines();

        FacetsConfig config = new FacetsConfig();
        config.setIndexFieldName("address.taxonomy", "address.taxonomy");
        config.setHierarchical("address.taxonomy", true);
        config.setHierarchical("address.sortedset", true);

        lines.limit(60000000).forEach(withCounter((i, line) -> {
            String[] lineArr = line.split(",");

            if (lineArr == null) {
                return;
            }
            for (String label : lineArr) {
                if (label == null || label.isEmpty()) {
                    return;
                }
            }

            Document doc = new Document();
            doc.add(new SortedSetDocValuesFacetField("address.sortedset", lineArr));

            Document doc2 = new Document();
            doc2.add(new FacetField("address.taxonomy", lineArr));

            try {
                writer.addDocument(config.build(doc));
                writer.addDocument(config.build(taxo, doc2));
            } catch (Exception e) {
                System.out.println("Problem adding document");
            }

            if (i % 100000 == 0) {
                System.out.println("Indexed " + i + " documents");
            }
        }));

        writer.commit();
        writer.forceMerge(1);
        taxo.commit();
        writer.close();
        taxo.close();
        dir.close();
        taxoDir.close();
    }

    public static <T> Consumer<T> withCounter(BiConsumer<Integer, T> consumer) {
        AtomicInteger counter = new AtomicInteger(0);
        return item -> consumer.accept(counter.getAndIncrement(), item);
    }

    private static void deleteDirectoryIfExists(Path path) throws IOException {
        if (Files.exists(path)) {
            try(Stream<Path> walk = Files.walk(path)) {
                walk.sorted(Comparator.reverseOrder()).forEach(IndexFacets::deleteFile);
            }
        }
    }

    private static void deleteFile(Path path) {
        try {
            Files.delete(path);
        } catch (IOException e) {
            e.printStackTrace();
            System.exit(-1);
        }
    }
}

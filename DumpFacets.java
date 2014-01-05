import java.io.File;
import java.io.IOException;

import org.apache.lucene.facet.taxonomy.CategoryPath;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.index.AtomicReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.SlowCompositeReaderWrapper;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

// javac -cp /l/trunk/lucene/build/core/classes/java:/l/trunk/lucene/build/facet/classes/java:/l/trunk/lucene/build/codecs/classes/java /l/util/DumpFacets.java 

// java -cp /l/util:/l/trunk/lucene/build/core/classes/java:/l/trunk/lucene/build/facet/classes/java:/l/trunk/lucene/build/codecs/classes/java DumpFacets /s2/scratch/indices/wikibig.trunk.facets.all.noparents.Lucene45.Lucene41.nd6.64758M/{index,facets} $all

public class DumpFacets {
  public static void main(String[] args) throws IOException {
    Directory dir = FSDirectory.open(new File(args[0]));
    AtomicReader r = SlowCompositeReaderWrapper.wrap(DirectoryReader.open(dir));

    Directory tdir = FSDirectory.open(new File(args[1]));
    TaxonomyReader tr = new DirectoryTaxonomyReader(tdir);
    System.out.println("ords");
    for(int ord=0;ord<tr.getSize();ord++) {
      System.out.println("  " + ord + " " + tr.getPath(ord).toString('\u001f'));
    }
    BinaryDocValues bdv = r.getBinaryDocValues(args[2]);
    System.out.println("maxDoc=" + r.maxDoc() + " r=" + r);
    BytesRef buf = new BytesRef();
    for(int i=0;i<r.maxDoc();i++) {
      System.out.println(""+i);
      bdv.get(i, buf);
      if (buf.length > 0) {
        // this document has facets
        final int end = buf.offset + buf.length;
        int ord = 0;
        int offset = buf.offset;
        int prev = 0;
        while (offset < end) {
          byte b = buf.bytes[offset++];
          if (b >= 0) {
            prev = ord = ((ord << 7) | b) + prev;
            System.out.println("  " + ord);
            ord = 0;
          } else {
            ord = (ord << 7) | (b & 0x7F);
          }
        }
      }
    }
    r.close();
    dir.close();
  }
}

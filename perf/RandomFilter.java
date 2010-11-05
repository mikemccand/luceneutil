package perf;

import java.util.Random;

import org.apache.lucene.index.IndexReader;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.Filter;
import org.apache.lucene.util.OpenBitSet;

class RandomFilter extends Filter {
  final double pctKeep;

  public RandomFilter(double pctKeep) {
    this.pctKeep = pctKeep;
  }

  @Override
  public DocIdSet getDocIdSet(IndexReader r) {
    final Random rand = new Random(42);
    final int maxDoc = r.maxDoc();
    OpenBitSet bits = new OpenBitSet(maxDoc);
    for(int docID = 0;docID<maxDoc;docID++) {
      if (rand.nextDouble() <= pctKeep) {        
        bits.fastSet(docID);
      }
    }

    System.out.println("rfilt " + bits.cardinality());
    return bits;
  }
}
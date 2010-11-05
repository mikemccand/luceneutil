package perf;
import org.apache.lucene.search.Filter;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Sort;


public class QueryAndSort {
  final Query q;
  final Sort s;
  final Filter f;

  public QueryAndSort(Query q, Sort s, Filter f) {
    this.q = q;
    this.s = s;
    this.f = f;
  }
}
package perf;

import java.io.File;
import java.io.IOException;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.FieldsEnum;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

public class ShowFields {

  
  public static void main(String[] args) throws CorruptIndexException, IOException {
    IndexReader reader = IndexReader.open(FSDirectory.open(new File("/home/simon/work/projects/lucene/bench/indices/Standard.work.trunk.wiki.nd0.1M/index")));
    Fields fields = MultiFields.getFields(reader);
    FieldsEnum iterator = fields.iterator();
    String name;
    while((name = iterator.next()) != null) {
      System.out.println(name);
      if(name.equals("docdate")) {
        TermsEnum terms = iterator.terms();
        BytesRef ref;
        int i = 0;
        while((ref = terms.next()) != null) {
          System.out.println(ref.utf8ToString());
          if(i++ == 10)
            break;
        }
      }
        
    }
  }
}

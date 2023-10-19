package perf;
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
import java.io.File;
import java.io.IOException;
import java.util.Iterator;

import org.apache.lucene.index.CorruptIndexException;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.Fields;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.MultiFields;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BytesRef;

public class ShowFields {

  
  public static void main(String[] args) throws CorruptIndexException, IOException {
    DirectoryReader reader = DirectoryReader.open(FSDirectory.open(new File("/home/simon/work/projects/lucene/bench/indices/Standard.work.trunk.wiki.nd0.1M/index")));
    Fields fields = MultiFields.getFields(reader);
    for(String name : fields) {
      System.out.println(name);
      if(name.equals("docdate")) {
        TermsEnum terms = fields.terms(name).iterator(null);
        BytesRef ref;
        int i = 0;
        while((ref = terms.next()) != null) {
          System.out.println(ref.utf8ToString());
          if(i++ == 10) {
            break;
          }
        }
      }
    }
  }
}

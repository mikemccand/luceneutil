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

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.apache.lucene.facet.index.FacetFields;
import org.apache.lucene.facet.params.CategoryListParams.OrdinalPolicy;
import org.apache.lucene.facet.params.CategoryListParams;

class FacetGroup {
  final String groupName;
  final List<String> fields;
  final CategoryListParams clp;

  // Used only during indexing:
  FacetFields builder;

  public FacetGroup(String arg) {
    int i = arg.indexOf(':');
    if (i == -1) {
      throw new IllegalArgumentException("facetGroup must be for groupName:ordPolicy:field,field,field; got \"" + arg + "\"");
    }
    groupName = arg.substring(0, i);

    int j = arg.indexOf(':', i+1);
    if (j == -1) {
      throw new IllegalArgumentException("facetGroup must be for groupName:ordPolicy:field,field,field; got \"" + arg + "\"");
    }
    String s = arg.substring(i+1, j);
    final OrdinalPolicy ordPolicy;
    if (s.equals("noparents")) {
      ordPolicy = OrdinalPolicy.NO_PARENTS;
    } else if (s.equals("allparents")) {
      ordPolicy = OrdinalPolicy.ALL_PARENTS;
    } else if (s.equals("allbutdim")) {
      ordPolicy = OrdinalPolicy.ALL_BUT_DIMENSION;
    } else {
      throw new IllegalArgumentException("ordPolicy must be \"noparents\" or \"allparents\" or \"allbutdim\"; got \"" + s + "\"");
    }
    System.out.println("OrdPolicy=" + ordPolicy);
    fields = Arrays.asList(arg.substring(j+1).split(","));
    clp = new CategoryListParams("$" + groupName) {
        @Override
        public OrdinalPolicy getOrdinalPolicy(String fieldName) {
          return ordPolicy;
        }
      };
  }

  @Override
  public String toString() {
    return "FacetGroup<" + groupName + ":" + fields + ">";
  }
}


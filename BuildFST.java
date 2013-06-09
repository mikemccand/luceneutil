/*
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

import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.IntsRef;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Util;

// pushd ../buildfst/lucene/core; ant jar; popd; javac -cp /l/buildfst/lucene/build/core/lucene-core-4.4-SNAPSHOT.jar BuildFST.java; java -cp .:/l/buildfst/lucene/build/core/lucene-core-4.4-SNAPSHOT.jar BuildFST foo/bar feo/baz > /x/tmp/out.dot; dot -Tpng /x/tmp/out.dot > /x/tmp/out.png

// javac -cp /l/buildfst/lucene/build/core/lucene-core-4.4-SNAPSHOT.jar BuildFST.java 

public class BuildFST {

  static class Pair implements Comparable<Pair> {
    BytesRef input;
    BytesRef output;

    public Pair(BytesRef input, BytesRef output) {
      this.input = input;
      this.output = output;
    }

    @Override
    public int compareTo(Pair other) {
      return input.compareTo(other.input);
    }
  }

  public static void main(String[] args) throws IOException {
    Pair[] inputs = new Pair[args.length];
    for(int i=0;i<args.length;i++) {
      String[] pair = args[i].split("/");
      if (pair.length != 2) {
        throw new RuntimeException("each arg should be input/output pair");
      }
      inputs[i] = new Pair(new BytesRef(pair[0]), new BytesRef(pair[1]));
    }
    Arrays.sort(inputs);

    final ByteSequenceOutputs outputs = ByteSequenceOutputs.getSingleton();
    Builder<BytesRef> b = new Builder<BytesRef>(FST.INPUT_TYPE.BYTE1, outputs);
    for(Pair pair : inputs) {
      b.add(Util.toIntsRef(pair.input, new IntsRef()), pair.output);
    }
    FST<BytesRef> fst = b.finish();
    Util.toDot(fst, new PrintWriter(System.out), true, true);
  }
}

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
import org.apache.lucene.util.IntsRefBuilder;
import org.apache.lucene.util.fst.Builder;
import org.apache.lucene.util.fst.ByteSequenceOutputs;
import org.apache.lucene.util.fst.FST;
import org.apache.lucene.util.fst.Outputs;
import org.apache.lucene.util.fst.PositiveIntOutputs;
import org.apache.lucene.util.fst.Util;

// pushd ../buildfst/lucene/core; ant jar; popd; javac -cp /l/buildfst/lucene/build/core/lucene-core-4.4-SNAPSHOT.jar BuildFST.java; java -cp .:/l/buildfst/lucene/build/core/lucene-core-4.4-SNAPSHOT.jar BuildFST foo/bar feo/baz > /x/tmp/out.dot; dot -Tpng /x/tmp/out.dot > /x/tmp/out.png

// javac -cp /l/buildfst/lucene/build/core/lucene-core-4.4-SNAPSHOT.jar BuildFST.java 

public class StageTokenizer {

  static class Pair<T> implements Comparable<Pair<T>> {
    BytesRef input;
    T output;

    public Pair(BytesRef input, T output) {
      this.input = input;
      this.output = output;
    }

    @Override
    public int compareTo(Pair<T> other) {
      return input.compareTo(other.input);
    }
  }

  @SuppressWarnings({ "rawtypes", "unchecked" })
	public static void main(String[] args) throws IOException {

    boolean numeric = true;
    boolean negative = false;
    for(int i=0;i<args.length;i++) {
      int j = args[i].lastIndexOf('/');
      if (j != -1) {
        try {
          negative |= Long.parseLong(args[i].substring(j+1)) < 0;
        } catch (NumberFormatException nfe) {
          numeric = false;
          break;
        }
      }
    }

    Outputs outputs;
    if (numeric) {
      if (negative) {
        throw new RuntimeException("can only handle numeric outputs >= 0");
      }
      outputs = PositiveIntOutputs.getSingleton();
    } else {
      outputs = ByteSequenceOutputs.getSingleton();
    }
    
    Pair<?>[] inputs = new Pair[args.length];
    for(int i=0;i<args.length;i++) {
      int j = args[i].lastIndexOf('/');
      String input;
      Object output;
      if (j == -1) {
        output = outputs.getNoOutput();
        input = args[i];
      } else {
        input = args[i].substring(0, j);
        String outputString = args[i].substring(j+1);
        if (numeric) {
          output = Long.parseLong(outputString);
        } else {
          output = new BytesRef(outputString);
        }
      }
      inputs[i] = new Pair(new BytesRef(input), output);
    }
    Arrays.sort(inputs);

    FST<?> fst;
    if (numeric) {
      Builder<Long> b = new Builder<Long>(FST.INPUT_TYPE.BYTE1, outputs);
      for(Pair pair : inputs) {
        IntsRefBuilder intsBuilder = new IntsRefBuilder();
        Util.toIntsRef(pair.input, intsBuilder);
        b.add(intsBuilder.get(), (Long) pair.output);
      }
      fst = b.finish();
    } else {
      Builder<BytesRef> b = new Builder<BytesRef>(FST.INPUT_TYPE.BYTE1, outputs);
      for(Pair pair : inputs) {
        IntsRefBuilder intsBuilder = new IntsRefBuilder();
        Util.toIntsRef(pair.input, intsBuilder);
        b.add(intsBuilder.get(), (BytesRef) pair.output);
      }
      fst = b.finish();
    }
    Util.toDot(fst, new PrintWriter(System.out), true, true);
  }
}

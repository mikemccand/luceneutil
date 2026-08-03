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

package knn;

import java.io.IOException;
import java.nio.ShortBuffer;
import java.nio.channels.FileChannel;

public class VectorReaderFloat16 extends VectorReader {
  VectorReaderFloat16(FileChannel input, int dim, int bufferSize, int vectorStartIndex) throws IOException {
    super(input, dim, bufferSize, vectorStartIndex);
  }

  @Override
  float[] next() throws IOException {
    readNext();
    ShortBuffer shorts = bytes.asShortBuffer();
    for (int i = 0; i < target.length; i++) {
      target[i] = Float.float16ToFloat(shorts.get(i));
    }
    return target;
  }
}

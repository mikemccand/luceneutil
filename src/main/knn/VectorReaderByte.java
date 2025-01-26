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
import java.nio.channels.FileChannel;

public class VectorReaderByte extends VectorReader {

  VectorReaderByte(FileChannel input, int dim, int bufferSize, int vectorStartIndex) throws IOException {
    super(input, dim, bufferSize, vectorStartIndex);
  }

  @Override
  float[] next() throws IOException {
    readNext();
    for (int i = 0; i < bytes.array().length; i++) {
      target[i] = bytes.array()[i];
    }
    return target;
  }

  byte[] nextBytes() throws IOException {
    readNext();
    return bytes.array();
  }
}

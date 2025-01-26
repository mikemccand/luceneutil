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

//package knn;

import org.apache.lucene.index.VectorEncoding;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

public abstract class VectorReader {
  final float[] target;
  final ByteBuffer bytes;
  final FileChannel input;

  // seek to this vector on init/reset:
  final int vectorStartIndex;

  static VectorReader create(FileChannel input, int dim, VectorEncoding vectorEncoding, int vectorStartIndex) throws IOException {
    int bufferSize = dim * vectorEncoding.byteSize;
    return switch (vectorEncoding) {
      case BYTE -> new VectorReaderByte(input, dim, bufferSize, vectorStartIndex);
      case FLOAT32 -> new VectorReaderFloat32(input, dim, bufferSize, vectorStartIndex);
    };
  }

  VectorReader(FileChannel input, int dim, int bufferSize, int vectorStartIndex) throws IOException {
    this.bytes = ByteBuffer.wrap(new byte[bufferSize]).order(ByteOrder.LITTLE_ENDIAN);
    this.input = input;
    this.vectorStartIndex = vectorStartIndex;
    target = new float[dim];
    reset();
  }

  void reset() throws IOException {
    long pos = vectorStartIndex * (long) bytes.capacity();
    input.position(pos);
  }

  protected final void readNext() throws IOException {
    int bytesRead = this.input.read(bytes);
    if (bytesRead < bytes.capacity()) {
      // wrap around back to the start of the file if we hit the end:
      System.out.println("WARNING: VectorReader hit EOF when reading " + this.input + "; now wrapping around to start of file again");
      this.input.position(0);
      bytesRead = this.input.read(bytes);
      if (bytesRead < bytes.capacity()) {
        throw new IllegalStateException("vector file " + input + " doesn't even have enough bytes for a single vector?  got bytesRead=" + bytesRead);
      }
    }
    bytes.position(0);
  }

  abstract float[] next() throws IOException;
}

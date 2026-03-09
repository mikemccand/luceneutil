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

import org.apache.lucene.index.VectorEncoding;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.MemoryLayout;
import java.lang.foreign.SequenceLayout;
import java.lang.foreign.ValueLayout;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public sealed abstract class VectorReader implements AutoCloseable {
  protected final SequenceLayout vectorLayout;
  protected final MemorySegment rawData;
  private final Arena arena;

  static VectorReader create(Path path, int dim, VectorEncoding vectorEncoding) throws IOException {
    return switch (vectorEncoding) {
      case BYTE -> new VectorReader.Byte(path, dim);
      case FLOAT32 -> new VectorReader.Float32(path, dim);
    };
  }

  protected VectorReader(Path path, int dim, ValueLayout elementLayout) throws IOException {
    this.vectorLayout = MemoryLayout.sequenceLayout(dim, elementLayout);
    try (FileChannel file = FileChannel.open(path, StandardOpenOption.READ)) {
      long size = file.size();
      if (size % vectorLayout.byteSize() != 0) {
        throw new IllegalArgumentException("Vector file is " + size + " bytes, but vector size is " + vectorLayout.byteSize());
      }
      this.arena = Arena.ofShared();
      this.rawData = file.map(FileChannel.MapMode.READ_ONLY, 0, size, arena);
    }
  }

  protected MemorySegment getVectorSlice(long index) {
    long byteOffset = vectorLayout.scale(0L, index);
    return rawData.asSlice(byteOffset, vectorLayout);
  }

  public long getVectorCount() throws IOException {
    return rawData.byteSize() / vectorLayout.byteSize();
  }

  public void close() throws IOException {
    arena.close();
  }

  public static final class Byte extends VectorReader {
    private static ValueLayout.OfByte VALUE_LAYOUT = ValueLayout.JAVA_BYTE;

    public Byte(Path path, int dim) throws IOException {
      super(path, dim, VALUE_LAYOUT);
    }

    public byte[] read(int index) {
      return getVectorSlice(index).toArray(VALUE_LAYOUT);
    }
  }

  public static final class Float32 extends VectorReader {
    private static ValueLayout.OfFloat VALUE_LAYOUT = ValueLayout.JAVA_FLOAT.withOrder(ByteOrder.LITTLE_ENDIAN);

    public Float32(Path path, int dim) throws IOException {
      super(path, dim, VALUE_LAYOUT);
    }

    public float[] read(int index) {
      return getVectorSlice(index).toArray(VALUE_LAYOUT);
    }
  }
}

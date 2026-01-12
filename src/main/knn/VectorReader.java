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
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public sealed abstract class VectorReader<T> implements AutoCloseable {
  private final FileChannel input;
  protected final int dim;
  protected final int bytesPerVector;

  static VectorReader create(Path path, int dim, VectorEncoding vectorEncoding) throws IOException {
    return switch (vectorEncoding) {
      case BYTE -> new VectorReader.Byte(path, dim);
      case FLOAT32 -> new VectorReader.Float32(path, dim);
    };
  }

  protected VectorReader(Path path, int dim, int bytesPerDim) throws IOException {
    this.input = FileChannel.open(path, StandardOpenOption.READ);
    this.dim = dim;
    this.bytesPerVector = dim * bytesPerDim;
  }

  public T read(long index) throws IOException {
    final long size = input.size();
    final long realIndex = index % getVectorCount();
    final long pos = realIndex * bytesPerVector;

    final ByteBuffer buf = ByteBuffer.allocate(bytesPerVector);
    final int bytesRead = input.read(buf, pos);
    if (bytesRead != bytesPerVector) {
      throw new IOException("Incompletely read " + bytesRead + "/" + bytesPerVector + " bytes");
    }

    buf.flip();
    return interpret(buf);
  }

  public long getVectorCount() throws IOException {
    return input.size() / bytesPerVector;
  }

  protected abstract T interpret(ByteBuffer buf);

  public void close() throws IOException {
    input.close();
  }

  public static final class Byte extends VectorReader<byte[]> {
    private Byte(Path path, int dim) throws IOException {
      super(path, dim, VectorEncoding.BYTE.byteSize);
    }


    @Override
    protected byte[] interpret(ByteBuffer buf) {
      final byte[] target = new byte[dim];
      buf.get(target);
      return target;
    }
  }

  public static final class Float32 extends VectorReader<float[]> {
    private Float32(Path path, int dim) throws IOException {
      super(path, dim, VectorEncoding.FLOAT32.byteSize);
    }

    @Override
    protected float[] interpret(ByteBuffer buf) {
      final float[] target = new float[dim];
      buf.order(ByteOrder.LITTLE_ENDIAN).asFloatBuffer().get(target);
      return target;
    }
  }
}

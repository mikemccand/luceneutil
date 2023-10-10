package knn;

import org.apache.lucene.index.VectorEncoding;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;

public abstract class VectorReader {
    final float[] target;
    final ByteBuffer bytes;
    final FileChannel input;

    static VectorReader create(FileChannel input, int dim, VectorEncoding vectorEncoding) {
        int bufferSize = dim * vectorEncoding.byteSize;
        return switch (vectorEncoding) {
            case BYTE -> new VectorReaderByte(input, dim, bufferSize);
            case FLOAT32 -> new VectorReaderFloat32(input, dim, bufferSize);
        };
    }

    VectorReader(FileChannel input, int dim, int bufferSize) {
        this.bytes = ByteBuffer.wrap(new byte[bufferSize]).order(ByteOrder.LITTLE_ENDIAN);
        this.input = input;
        target = new float[dim];
    }

    void reset() throws IOException {
        input.position(0);
    }

    protected final void readNext() throws IOException {
        this.input.read(bytes);
        bytes.position(0);
    }

    abstract float[] next() throws IOException;
}

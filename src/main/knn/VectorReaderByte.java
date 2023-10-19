package knn;

import java.io.IOException;
import java.nio.channels.FileChannel;

public class VectorReaderByte extends VectorReader {
    private final byte[] scratch;

    VectorReaderByte(FileChannel input, int dim, int bufferSize) {
        super(input, dim, bufferSize);
        scratch = new byte[dim];
    }

    @Override
    float[] next() throws IOException {
        readNext();
        bytes.get(scratch);
        for (int i = 0; i < scratch.length; i++) {
            target[i] = scratch[i];
        }
        return target;
    }

    byte[] nextBytes() throws IOException {
        readNext();
        bytes.get(scratch);
        return scratch;
    }
}

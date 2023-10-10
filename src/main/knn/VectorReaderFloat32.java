package knn;

import java.io.IOException;
import java.nio.channels.FileChannel;

public class VectorReaderFloat32 extends VectorReader {
    VectorReaderFloat32(FileChannel input, int dim, int bufferSize) {
        super(input, dim, bufferSize);
    }

    @Override
    float[] next() throws IOException {
        readNext();
        bytes.asFloatBuffer().get(target);
        return target;
    }
}

import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import java.nio.file.Path;
import java.util.Random;

/**
 * Minimal example of calling the natively compiled dot product (native/dot_product.c) from Java via
 * the Foreign Function &amp; Memory API (final in Java 22+).
 *
 * <p>Build the library with {@code make native}, then run:
 *
 * <pre>
 *   java --enable-native-access=ALL-UNNAMED native/NativeDotProduct.java build/native/libdot_product.so
 * </pre>
 *
 * <p>This is the same pattern an app would use to plug native code in under its own Lucene
 * VectorSimilarityFunction / scorer: bind the symbol once into a static final MethodHandle, keep
 * vectors in off-heap MemorySegments (e.g. mmap'd index files), and call invokeExact per
 * comparison.
 */
public class NativeDotProduct {

  private final MethodHandle dotProduct;

  public NativeDotProduct(Path library) {
    // global arena: the library stays loaded for the life of the jvm
    SymbolLookup lookup = SymbolLookup.libraryLookup(library, Arena.global());
    this.dotProduct =
        Linker.nativeLinker()
            .downcallHandle(
                lookup.find("dot_product").orElseThrow(),
                FunctionDescriptor.of(
                    ValueLayout.JAVA_FLOAT,
                    ValueLayout.ADDRESS,
                    ValueLayout.ADDRESS,
                    ValueLayout.JAVA_INT));
  }

  /** Dot product of two off-heap float vectors of {@code n} elements. */
  public float dotProduct(MemorySegment a, MemorySegment b, int n) {
    try {
      return (float) dotProduct.invokeExact(a, b, n);
    } catch (Throwable t) {
      throw new RuntimeException(t);
    }
  }

  /** Reference scalar java impl, used to sanity check the native result. */
  static float javaDotProduct(float[] a, float[] b) {
    float sum = 0f;
    for (int i = 0; i < a.length; i++) {
      sum += a[i] * b[i];
    }
    return sum;
  }

  public static void main(String[] args) {
    if (args.length != 1) {
      System.err.println("usage: java native/NativeDotProduct.java <path to libdot_product.so/.dylib/.dll>");
      System.exit(1);
    }
    int dims = 1024;
    NativeDotProduct nativeImpl = new NativeDotProduct(Path.of(args[0]).toAbsolutePath());

    Random random = new Random(42);
    float[] a = new float[dims];
    float[] b = new float[dims];
    for (int i = 0; i < dims; i++) {
      a[i] = random.nextFloat() - 0.5f;
      b[i] = random.nextFloat() - 0.5f;
    }

    try (Arena arena = Arena.ofConfined()) {
      MemorySegment segA = arena.allocateFrom(ValueLayout.JAVA_FLOAT, a);
      MemorySegment segB = arena.allocateFrom(ValueLayout.JAVA_FLOAT, b);
      float nativeResult = nativeImpl.dotProduct(segA, segB, dims);
      float javaResult = javaDotProduct(a, b);
      System.out.println("native=" + nativeResult + " java=" + javaResult);
      // fast-math reorders the sum, so allow small float rounding differences
      if (Math.abs(nativeResult - javaResult) > 1e-3f) {
        System.err.println("MISMATCH between native and java dot product");
        System.exit(2);
      }
      System.out.println("OK");
    }
  }
}

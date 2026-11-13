### Simple native (C) dot product for Lucene vector search

Lucene's default vector scoring is Panama (when available) or hotspot-compiled Java. An app can often go
faster by compiling a trivial C loop with `gcc -O3 -march=native` for the exact CPU it runs on, and calling it
from Java through the Foreign Function & Memory API (final in Java 22+). This directory is a minimal starting
point for that.

Files:
- `dot_product.c`: the float32 dot product, plus a stderr message the first time it runs
- `NativeDotProduct.java`: loads the library, binds `dot_product` as a `MethodHandle`, calls it, and checks the
  result against a plain Java loop

Usage (from the repo root; needs `gcc` or `clang` via `CC=clang`, `make`, and Java 22+):

```
make native       # builds build/native/libdot_product.{so,dylib,dll} for THIS cpu
make native-run   # builds, then runs the Java example
```

You should see `luceneutil native dot_product: RUNNING native vectorized code` on stderr, then `OK`.
`tests/test_native_dot_product.py` runs exactly this and checks for that message.

To use it in your own app: copy `dot_product.c`, build it as above, and bind it like `NativeDotProduct` does,
ideally holding vectors in off-heap `MemorySegment`s (e.g. mmap'd index files) so no copy is needed per call.

Notes and limitations:
- `-march=native` means the binary only runs on CPUs like the build machine. Build on the machine that runs it.
- `-ffast-math` is what lets gcc vectorize the float reduction; results may differ from Java in the last bits.
- No cross compilation, no per-CPU variants / `ifunc` dispatch, no packaging. Iterate from here.

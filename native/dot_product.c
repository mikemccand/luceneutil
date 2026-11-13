// simple native float32 dot product, meant to be compiled by gcc/clang for the cpu running "make native".
//
// the loop is deliberately trivial: with -O3 -march=native -ffast-math the compiler turns it into
// simd code (avx2/avx512/neon/...) on its own.  -ffast-math (really -fassociative-math) is required,
// otherwise the compiler must keep the strict left-to-right summation order and cannot vectorize the
// reduction.  results can therefore differ from lucene's java impl in the last few bits.

#include <stdio.h>

#if defined(_WIN32)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT __attribute__((visibility("default")))
#endif

static int announced = 0;

// called by the java side (or any ffi caller): returns sum(a[i] * b[i]) for i in [0, n).
EXPORT float dot_product(const float *restrict a, const float *restrict b, int n) {
  if (!announced) {
    // clear marker so users (and tests) can see the native code is really the one running
    announced = 1;
    fprintf(stderr, "luceneutil native dot_product: RUNNING native vectorized code (n=%d)\n", n);
    fflush(stderr);
  }
  float sum = 0.0f;
  for (int i = 0; i < n; i++) {
    sum += a[i] * b[i];
  }
  return sum;
}

// lets callers (and tests) check which build they got.
EXPORT const char *dot_product_build_info(void) {
#if defined(__AVX512F__)
  return "x86-64 avx512";
#elif defined(__AVX2__)
  return "x86-64 avx2";
#elif defined(__ARM_NEON)
  return "arm neon";
#else
  return "generic (no simd extensions detected at compile time)";
#endif
}

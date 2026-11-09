"""Temporary CI diagnostic: is float64 X.T @ X correct in this numpy build? (luceneutil PR #641 failures)"""

import os
import platform
import subprocess
import sys

import numpy as np


def check(label, got, ref, rtol):
  ok = np.allclose(got, ref, rtol=rtol, atol=rtol * float(np.abs(ref).max()))
  err = float(np.abs(got - ref).max())
  print(f"  {'ok  ' if ok else 'BAD '} {label:<44} max_abs_err={err:.3e}  got[0,0]={float(got[0, 0]):.6e} ref[0,0]={float(ref[0, 0]):.6e}")
  return ok


def run_checks():
  rng = np.random.default_rng(0)
  all_ok = True
  for n, d in ((10, 4), (2000, 100), (2000, 384)):
    print(f" shape ({n}, {d})")
    for dtype, rtol in ((np.float64, 1e-9), (np.float32, 1e-3)):
      x = rng.standard_normal((n, d)).astype(dtype)
      y = rng.standard_normal((n, d)).astype(dtype)
      ref_xx = np.einsum("ij,ik->jk", x.astype(np.float64), x.astype(np.float64))  # no BLAS
      ref_xy = np.einsum("ij,ik->jk", x.astype(np.float64), y.astype(np.float64))
      name = np.dtype(dtype).name
      all_ok &= check(f"{name} x.T @ x        (syrk path)", x.T @ x, ref_xx, rtol)
      all_ok &= check(f"{name} x.T.copy() @ x (gemm path)", x.T.copy() @ x, ref_xx, rtol)
      all_ok &= check(f"{name} x.T @ y        (gemm, distinct)", x.T @ y, ref_xy, rtol)
      all_ok &= check(f"{name} np.dot(x.T, x)", np.dot(x.T, x), ref_xx, rtol)

  # the exact smell_vectors pattern: float32 data minus float64 mean, then centered.T @ centered
  v = np.fromfile("tasks/vector-task-minilm.vec", dtype="<f4").reshape(-1, 384)[:2000]
  centered = v - v.mean(axis=0, dtype=np.float64)
  m2 = (centered * centered).sum(axis=0)
  cov = centered.T @ centered
  err = float(np.abs(np.diag(cov) - m2).max())
  ok = err < 1e-9
  all_ok &= ok
  print(f" smell_vectors pattern on minilm: {'ok' if ok else 'BAD'}  max|diag(cov) - m2|={err:.3e}  diag[:3]={np.diag(cov)[:3]}  m2[:3]={m2[:3]}")
  return all_ok


def main():
  if "--child" in sys.argv:
    sys.exit(0 if run_checks() else 1)

  print("python", sys.version)
  print("numpy", np.__version__, np.__file__)
  print("platform", platform.platform())
  print(subprocess.run("lscpu | grep -E 'Model name|Flags' | cut -c1-400", shell=True, capture_output=True, text=True).stdout)
  np.show_config()
  so_dir = os.path.join(os.path.dirname(np.__file__), "_core")
  for f in sorted(os.listdir(so_dir)):
    if f.startswith("_multiarray_umath") and f.endswith(".so"):
      print(subprocess.run(["ldd", os.path.join(so_dir, f)], capture_output=True, text=True).stdout)
  print(subprocess.run("ls /usr/lib/x86_64-linux-gnu | grep -iE 'blas|lapack|blis|mkl' ; dpkg -l | grep -iE 'blas|lapack|blis' ", shell=True, capture_output=True, text=True).stdout)

  variants = [
    ("default", {}),
    ("OPENBLAS_VERBOSE=2", {"OPENBLAS_VERBOSE": "2"}),
    ("OPENBLAS_NUM_THREADS=1", {"OPENBLAS_NUM_THREADS": "1"}),
    ("OPENBLAS_CORETYPE=Haswell", {"OPENBLAS_CORETYPE": "Haswell"}),
    ("OPENBLAS_CORETYPE=Prescott", {"OPENBLAS_CORETYPE": "Prescott"}),
  ]
  for label, extra in variants:
    print(f"\n===== variant: {label}", flush=True)
    r = subprocess.run([sys.executable, __file__, "--child"], env={**os.environ, **extra}, check=False)
    print(f"===== variant {label}: {'PASS' if r.returncode == 0 else 'FAIL'}", flush=True)


if __name__ == "__main__":
  main()

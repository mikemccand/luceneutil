"""Temporary CI diagnostic: is float64 X.T @ X correct in this numpy build? (luceneutil PR #641 failures)"""

import os
import platform
import subprocess
import sys

import numpy as np


def check(label, got, ref, rtol):
  # measure error element by element in pure Python: large float64 ufuncs themselves were
  # seen returning garbage in CI, so np.abs(got - ref).max() can't be trusted
  g, r = got.tolist(), ref.tolist()
  scale = max(abs(v) for row in r for v in row)
  err = max(abs(a - b) for gr, rr in zip(g, r, strict=True) for a, b in zip(gr, rr, strict=True))
  ok = err <= rtol * scale
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
  err = max(abs(a - b) for a, b in zip(np.diag(cov).tolist(), m2.tolist(), strict=True))
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

  try:
    from numpy.lib.introspect import opt_func_info  # noqa: PLC0415

    info = opt_func_info(func_name="matmul|subtract|absolute|maximum|add|multiply", signature="float64.*")
    for func, sigs in info.items():
      print(" dispatch", func, {k: v.get("current") for k, v in sigs.items()})
  except Exception as e:  # noqa: BLE001
    print(" opt_func_info unavailable:", e)

  variants = [
    ("default", {}),
    ("NPY_DISABLE_CPU_FEATURES=AVX2 FMA3", {"NPY_DISABLE_CPU_FEATURES": "AVX2 FMA3"}),
    ("NPY_DISABLE_CPU_FEATURES=FMA3", {"NPY_DISABLE_CPU_FEATURES": "FMA3"}),
    ("NPY_DISABLE_CPU_FEATURES=AVX2", {"NPY_DISABLE_CPU_FEATURES": "AVX2"}),
  ]
  for label, extra in variants:
    print(f"\n===== variant: {label}", flush=True)
    env = {**os.environ, **extra}
    r = subprocess.run([sys.executable, __file__, "--child"], env=env, check=False)
    print(f"===== variant {label}: matmul checks {'PASS' if r.returncode == 0 else 'FAIL'}", flush=True)
    t = subprocess.run([sys.executable, "-m", "unittest", "discover", "-s", "tests", "-q"], env=env, check=False, capture_output=True, text=True)
    print(f"===== variant {label}: unit tests {'PASS' if t.returncode == 0 else 'FAIL'}  ({t.stderr.strip().splitlines()[-1] if t.stderr.strip() else ''})", flush=True)


if __name__ == "__main__":
  main()

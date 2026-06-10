#!/usr/bin/env python3
"""Hadamard rotation — Python implementation matching the structure of
org.apache.lucene.util.quantization.HadamardRotation.

The rotation pipeline is:
  1. random sign flips (per dim)
  2. random permutation (Fisher-Yates)
  3. block-diagonal Fast Walsh-Hadamard Transform (FWHT), normalized by 1/sqrt(n)
     so the transform is orthogonal

For non-power-of-2 dims, the FWHT is applied to a sequence of power-of-2 blocks
whose sizes sum to dim (binary decomposition: 768 = 512 + 256, 100 = 64 + 32 + 4).

The composed transform is orthogonal, so it preserves L2 norms and inner products
between vectors. Thus rotation is safe to apply to both index and query vectors
before scalar quantization (OSQ) — distances are preserved exactly, while per-dim
distributions are pushed toward Gaussian (CLT) with balanced variance, which is
what OSQ wants.

This is a Python implementation of the same logic; it is NOT bit-exact with the
Java version because it uses NumPy's PRNG instead of Java's Random LCG. The
resulting rotation is still orthogonal and statistically equivalent — same gains
on per-dim distributions, same KNN recall, just a different specific permutation /
sign sequence.

Usage:
- library:  `r = HadamardRotation.for_dimension(dim); r.rotate(in_arr, out_arr)`
            `out_batch = r.rotate_batch(in_batch)`            # (N, dim) -> (N, dim)
- CLI:      `python3 hadamard_rotation.py <in.vec> <out.vec> <dim>`
            (or `-i <in.vec> -o <out.vec> <dim>`)
"""

from __future__ import annotations

import argparse
import os
import sys
import time

import numpy as np


def _power_of_two_blocks(dim: int) -> list[int]:
  """Decompose dim into power-of-2 blocks, largest-first (binary decomposition).

  Examples: 768 -> [512, 256], 100 -> [64, 32, 4], 1024 -> [1024].
  """
  if dim < 1:
    raise ValueError(f"dim must be >= 1, got {dim}")
  return [1 << bit for bit in range(dim.bit_length() - 1, -1, -1) if dim & (1 << bit)]


def _fwht_inplace(a: np.ndarray, offset: int, n: int) -> None:
  """In-place Fast Walsh-Hadamard Transform on `a[offset:offset+n]`, normalized
  by 1/sqrt(n) so the transform is orthogonal. n must be a power of 2.
  """
  end = offset + n
  length = 1
  while length < n:
    step = length << 1
    for i in range(offset, end, step):
      u = a[i : i + length].copy()
      v = a[i + length : i + step].copy()
      a[i : i + length] = u + v
      a[i + length : i + step] = u - v
    length = step
  a[offset:end] *= np.float32(1.0 / np.sqrt(n))


class HadamardRotation:
  """Orthogonal rotation = sign flips ∘ permutation ∘ block-diagonal FWHT.

  Deterministic for a given (dim, seed). Instances are immutable and thread-safe;
  callers may pass a scratch buffer to `rotate` / `inverse_rotate` to avoid
  per-call allocation.
  """

  __slots__ = ("block_sizes", "dim", "permutation", "sign_flips")

  _BY_DIMENSION: dict[int, HadamardRotation] = {}

  def __init__(self, dim: int, block_sizes: list[int], permutation: np.ndarray, sign_flips: np.ndarray) -> None:
    self.dim = dim
    self.block_sizes = block_sizes
    self.permutation = permutation
    self.sign_flips = sign_flips

  @classmethod
  def create(cls, dim: int, seed: int) -> HadamardRotation:
    """Build a rotation from a deterministic permutation + sign-flip sequence
    seeded by `seed`. Two runs with the same seed produce the same rotation.
    """
    if dim < 1:
      raise ValueError(f"dim must be >= 1, got {dim}")
    rng = np.random.default_rng(seed)
    permutation = np.arange(dim, dtype=np.int64)
    rng.shuffle(permutation)
    sign_flips = rng.integers(0, 2, size=dim, dtype=np.int8).astype(bool)
    return cls(dim, _power_of_two_blocks(dim), permutation, sign_flips)

  @classmethod
  def for_dimension(cls, dim: int) -> HadamardRotation:
    """Cached rotation for the given dim. Seed is just `dim` itself, so the same
    dim always maps to the same rotation across runs and processes.
    """
    cached = cls._BY_DIMENSION.get(dim)
    if cached is not None:
      return cached
    rotation = cls.create(dim, seed=dim)
    cls._BY_DIMENSION[dim] = rotation
    return rotation

  def dimension(self) -> int:
    return self.dim

  def rotate(self, in_vec: np.ndarray, out_vec: np.ndarray, scratch: np.ndarray | None = None) -> None:
    """Apply rotation: out = R · in. Both arrays must be float32 of length self.dim.
    `in_vec` and `out_vec` may alias.
    """
    scratch = self._prepare(in_vec, out_vec, scratch)

    # Step 1: sign flips into scratch.
    np.copyto(scratch, in_vec)
    np.negative(scratch, where=self.sign_flips, out=scratch)

    # Step 2: permutation: out[perm[i]] = scratch[i].
    out_vec[self.permutation] = scratch

    # Step 3: block-diagonal FWHT in-place on out.
    self._fwht_blocks(out_vec)

  def inverse_rotate(self, in_vec: np.ndarray, out_vec: np.ndarray, scratch: np.ndarray | None = None) -> None:
    """Apply inverse rotation: out = Rᵀ · in. Since R is orthogonal, the inverse
    is the transpose; FWHT (normalized) and the sign-flip / permutation steps
    are each self-inverse, so we apply them in reverse order.
    """
    scratch = self._prepare(in_vec, out_vec, scratch)

    # FWHT each block (self-inverse with the 1/sqrt(n) normalization).
    np.copyto(scratch, in_vec)
    self._fwht_blocks(scratch)

    # Inverse permutation: result[i] = scratch[perm[i]].
    out_vec[:] = scratch[self.permutation]

    # Inverse sign flip (self-inverse).
    np.negative(out_vec, where=self.sign_flips, out=out_vec)

  def rotate_batch(self, in_arr: np.ndarray) -> np.ndarray:
    """Rotate every row of an (N, dim) float32 array. Returns a new (N, dim) array."""
    if in_arr.ndim != 2 or in_arr.shape[1] != self.dim:
      raise ValueError(f"in_arr must be (N, {self.dim}); got shape {in_arr.shape}")
    out = np.empty_like(in_arr)
    scratch = np.empty(self.dim, dtype=np.float32)
    for i in range(in_arr.shape[0]):
      self.rotate(in_arr[i], out[i], scratch)
    return out

  def _prepare(self, in_vec: np.ndarray, out_vec: np.ndarray, scratch: np.ndarray | None) -> np.ndarray:
    """Validate lengths and return a usable scratch buffer (allocating if needed)."""
    if in_vec.shape[0] != self.dim:
      raise ValueError(f"in length {in_vec.shape[0]} != dim {self.dim}")
    if out_vec.shape[0] != self.dim:
      raise ValueError(f"out length {out_vec.shape[0]} != dim {self.dim}")
    if scratch is None:
      return np.empty(self.dim, dtype=np.float32)
    if scratch.shape[0] != self.dim:
      raise ValueError(f"scratch length {scratch.shape[0]} != dim {self.dim}")
    return scratch

  def _fwht_blocks(self, arr: np.ndarray) -> None:
    """Apply the block-diagonal FWHT in place across all blocks of self.block_sizes."""
    offset = 0
    for block_size in self.block_sizes:
      _fwht_inplace(arr, offset, block_size)
      offset += block_size


# --- CLI: rotate an entire .vec file -----------------------------------------------


def _rotate_vec_file(in_path: str, out_path: str, dim: int, batch_vectors: int = 4096) -> None:
  """Read raw float32 little-endian .vec at in_path (no header), rotate every
  vector with HadamardRotation.for_dimension(dim), write to out_path.
  """
  vec_size_bytes = dim * 4
  in_size = os.path.getsize(in_path)
  num_vectors, leftover = divmod(in_size, vec_size_bytes)
  if leftover != 0:
    raise RuntimeError(f'file "{in_path}" size {in_size} is not a multiple of dim*4 ({vec_size_bytes}); wrong dim?')

  rotation = HadamardRotation.for_dimension(dim)
  print(f"hadamard: rotating {num_vectors} x {dim}-dim vectors  {in_path} -> {out_path}")
  t0 = time.monotonic()
  written = 0
  with open(in_path, "rb") as fin, open(out_path, "wb") as fout:
    while written < num_vectors:
      batch = min(batch_vectors, num_vectors - written)
      buf = fin.read(batch * vec_size_bytes)
      if len(buf) != batch * vec_size_bytes:
        raise RuntimeError(f"short read: wanted {batch * vec_size_bytes}, got {len(buf)}")
      arr = np.frombuffer(buf, dtype="<f4").reshape(batch, dim).copy()
      fout.write(rotation.rotate_batch(arr).tobytes())
      written += batch
      if written % max(1, num_vectors // 20) < batch or written == num_vectors:
        elapsed = time.monotonic() - t0
        print(f"\r  {written}/{num_vectors} ({100 * written / num_vectors:3.0f}%) {elapsed:.1f}s", end="", flush=True)
  print()
  print(f"hadamard: done in {time.monotonic() - t0:.1f}s; wrote {os.path.getsize(out_path)} bytes")


def _main():
  ap = argparse.ArgumentParser(
    description="Apply Hadamard rotation to every vector in a .vec file.",
    epilog="Either positional (in_vec out_vec dim) or flag form (-i ... -o ... <dim>) works.",
  )
  ap.add_argument("-i", "--in", dest="in_vec_flag", help="input .vec file")
  ap.add_argument("-o", "--out", dest="out_vec_flag", help="output .vec file (will be overwritten)")
  ap.add_argument("in_vec", nargs="?", help="input .vec file (positional alternative to -i)")
  ap.add_argument("out_vec", nargs="?", help="output .vec file (positional alternative to -o)")
  ap.add_argument("dim", type=int, help="dimension of each vector")
  args = ap.parse_args()

  in_path = args.in_vec_flag or args.in_vec
  out_path = args.out_vec_flag or args.out_vec
  if not in_path or not out_path:
    ap.error("both input and output paths are required (use -i/-o or positional in_vec out_vec)")
  if os.path.abspath(in_path) == os.path.abspath(out_path):
    print("ERROR: in and out must differ", file=sys.stderr)
    raise SystemExit(2)

  _rotate_vec_file(in_path, out_path, args.dim)


if __name__ == "__main__":
  _main()

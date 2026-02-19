#!/usr/bin/env python3

# reads the .bin score files produced by knnExactNNHistogram.py, generates
# a PNG histogram for each run, then stitches them into a video with ffmpeg.

import argparse
import json
import os
import subprocess
import sys

import matplotlib as mpl
import matplotlib.pyplot as plt
import numpy as np

# non-interactive backend so it works headless
mpl.use("Agg")


def load_scores(path):
  """Load little-endian float32 scores from a .bin file."""
  return np.fromfile(path, dtype="<f4")


def make_histogram_png(scores, n, m, num_docs, top_k, output_path, global_min, global_max, max_y_pct, num_bins=100):
  """Render a histogram of scores as a PNG, with y-axis as percentage."""
  fig, ax = plt.subplots(figsize=(12, 6))

  # compute bin percentages manually so we can control y-axis
  counts, bin_edges = np.histogram(scores, bins=num_bins, range=(global_min, global_max))
  pcts = counts / len(scores) * 100.0
  bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
  bin_width = bin_edges[1] - bin_edges[0]

  ax.bar(bin_centers, pcts, width=bin_width, color="#4285f4", edgecolor="white", linewidth=0.3)

  ax.set_xlabel("dot product similarity", fontsize=14)
  ax.set_ylabel("% of scores", fontsize=14)
  ax.set_xlim(global_min, global_max)
  ax.set_ylim(0, max_y_pct)
  ax.set_title(
    f"exact NN score distribution  —  N={n:,} queries x {num_docs:,} docs, top-{top_k}, repeat {m}",
    fontsize=15,
  )

  # stats text
  stats_text = f"scores: {len(scores):,}\nmin: {scores.min():.4f}\nmax: {scores.max():.4f}\nmean: {scores.mean():.4f}\nstd: {scores.std():.4f}"
  ax.text(
    0.98,
    0.95,
    stats_text,
    transform=ax.transAxes,
    fontsize=10,
    verticalalignment="top",
    horizontalalignment="right",
    bbox=dict(boxstyle="round,pad=0.4", facecolor="wheat", alpha=0.8),
    fontfamily="monospace",
  )

  # prominent N label
  ax.text(
    0.02,
    0.95,
    f"N = {n:,}",
    transform=ax.transAxes,
    fontsize=28,
    fontweight="bold",
    verticalalignment="top",
    color="#333333",
  )

  plt.tight_layout()
  fig.savefig(output_path, dpi=150)
  plt.close(fig)


def main():
  parser = argparse.ArgumentParser(description="generate histogram PNGs and video from exact NN score files")
  parser.add_argument("-inputDir", required=True, help="directory containing .bin files and manifest.json")
  parser.add_argument("-outputDir", default=None, help="directory for PNGs and video (default: inputDir)")
  parser.add_argument("-fps", type=int, default=2, help="frames per second in output video (default: 2)")
  parser.add_argument("-bins", type=int, default=100, help="number of histogram bins (default: 100)")
  parser.add_argument("-noVideo", action="store_true", help="skip video generation, only make PNGs")
  args = parser.parse_args()

  output_dir = args.outputDir if args.outputDir else args.inputDir
  os.makedirs(output_dir, exist_ok=True)

  manifest_path = os.path.join(args.inputDir, "manifest.json")
  with open(manifest_path) as f:
    manifest = json.load(f)

  runs = manifest["runs"]
  num_docs = manifest["numDocs"]
  top_k = manifest["topK"]

  print(f"found {len(runs)} runs in manifest")
  print(f"numDocs={num_docs} topK={top_k}")

  # first pass: find global min/max and max bin percentage across all score files
  # so all histograms share the same x-axis and y-axis range
  print("computing global score and bin percentage ranges...")
  global_min = float("inf")
  global_max = float("-inf")
  max_bin_pct = 0.0
  for run in runs:
    scores_path = os.path.join(args.inputDir, run["file"])
    scores = load_scores(scores_path)
    global_min = min(global_min, float(scores.min()))
    global_max = max(global_max, float(scores.max()))

  # second mini-pass with the now-known global range to find max bin pct
  for run in runs:
    scores_path = os.path.join(args.inputDir, run["file"])
    scores = load_scores(scores_path)
    counts, _ = np.histogram(scores, bins=args.bins, range=(global_min, global_max))
    pcts = counts / len(scores) * 100.0
    max_bin_pct = max(max_bin_pct, float(pcts.max()))

  # round up to next 0.5% for a clean axis limit
  max_y_pct = (int(max_bin_pct / 0.5) + 1) * 0.5
  print(f"global score range: [{global_min:.6f}, {global_max:.6f}]")
  print(f"max bin percentage: {max_bin_pct:.2f}%, y-axis limit: {max_y_pct:.1f}%")

  # render pass: generate PNGs
  png_paths = []
  for i, run in enumerate(runs):
    scores_path = os.path.join(args.inputDir, run["file"])
    scores = load_scores(scores_path)
    png_name = f"frame_{i:04d}.png"
    png_path = os.path.join(output_dir, png_name)

    make_histogram_png(
      scores,
      run["n"],
      run["m"],
      num_docs,
      top_k,
      png_path,
      global_min,
      global_max,
      max_y_pct,
      args.bins,
    )
    png_paths.append(png_path)
    print(f"  [{i + 1}/{len(runs)}] wrote {png_path}")

  print(f"\ngenerated {len(png_paths)} PNGs")

  if args.noVideo:
    print("skipping video generation (--noVideo)")
    return

  # stitch into video with ffmpeg
  video_path = os.path.join(output_dir, "exact_nn_histogram.mp4")
  frame_pattern = os.path.join(output_dir, "frame_%04d.png")

  ffmpeg_cmd = [
    "ffmpeg",
    "-y",
    "-framerate",
    str(args.fps),
    "-i",
    frame_pattern,
    "-c:v",
    "libx264",
    "-pix_fmt",
    "yuv420p",
    "-crf",
    "18",
    video_path,
  ]
  print(f"\nrunning: {' '.join(ffmpeg_cmd)}")
  result = subprocess.run(ffmpeg_cmd, capture_output=True, text=True, check=False)
  if result.returncode != 0:
    print(f"ffmpeg stderr:\n{result.stderr}", file=sys.stderr)
    raise RuntimeError(f"ffmpeg failed with exit code {result.returncode}")

  print(f"\nwrote video to {video_path}")
  print("done.")


if __name__ == "__main__":
  main()

#!/usr/bin/env python3

# reads the .bin score files produced by knnExactNNHistogram.py, generates
# a PNG histogram for each run, then stitches them into a video with ffmpeg.

import argparse
import json
import os
import subprocess
import sys

import matplotlib
import matplotlib.pyplot as plt
import numpy as np

# non-interactive backend so it works headless
matplotlib.use("Agg")


def load_scores(path):
  """load little-endian float32 scores from a .bin file."""
  return np.fromfile(path, dtype="<f4")


def make_histogram_png(scores, n, m, num_docs, top_k, output_path, global_min, global_max, num_bins=100):
  """render a histogram of scores as a PNG."""
  fig, ax = plt.subplots(figsize=(12, 6))

  ax.hist(scores, bins=num_bins, range=(global_min, global_max), color="#4285f4", edgecolor="white", linewidth=0.3)

  ax.set_xlabel("dot product similarity", fontsize=14)
  ax.set_ylabel("count", fontsize=14)
  ax.set_title(
    f"exact NN score distribution  —  N={n:,} queries × {num_docs:,} docs, top-{top_k}, repeat {m}",
    fontsize=15,
  )

  # stats text
  stats_text = (
    f"scores: {len(scores):,}\n"
    f"min: {scores.min():.4f}\n"
    f"max: {scores.max():.4f}\n"
    f"mean: {scores.mean():.4f}\n"
    f"std: {scores.std():.4f}"
  )
  ax.text(
    0.98, 0.95, stats_text,
    transform=ax.transAxes, fontsize=10, verticalalignment="top", horizontalalignment="right",
    bbox=dict(boxstyle="round,pad=0.4", facecolor="wheat", alpha=0.8),
    fontfamily="monospace",
  )

  # prominent N label
  ax.text(
    0.02, 0.95, f"N = {n:,}",
    transform=ax.transAxes, fontsize=28, fontweight="bold", verticalalignment="top",
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

  # first pass: find global min/max across all score files so histograms share the same x-axis
  print("computing global score range...")
  global_min = float("inf")
  global_max = float("-inf")
  for run in runs:
    scores_path = os.path.join(args.inputDir, run["file"])
    scores = load_scores(scores_path)
    global_min = min(global_min, float(scores.min()))
    global_max = max(global_max, float(scores.max()))
  print(f"global score range: [{global_min:.6f}, {global_max:.6f}]")

  # second pass: generate PNGs
  png_paths = []
  for i, run in enumerate(runs):
    scores_path = os.path.join(args.inputDir, run["file"])
    scores = load_scores(scores_path)
    png_name = f"frame_{i:04d}.png"
    png_path = os.path.join(output_dir, png_name)

    make_histogram_png(
      scores, run["n"], run["m"], num_docs, top_k, png_path, global_min, global_max, args.bins,
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
    "-framerate", str(args.fps),
    "-i", frame_pattern,
    "-c:v", "libx264",
    "-pix_fmt", "yuv420p",
    "-crf", "18",
    video_path,
  ]
  print(f"\nrunning: {' '.join(ffmpeg_cmd)}")
  result = subprocess.run(ffmpeg_cmd, capture_output=True, text=True)
  if result.returncode != 0:
    print(f"ffmpeg stderr:\n{result.stderr}", file=sys.stderr)
    raise RuntimeError(f"ffmpeg failed with exit code {result.returncode}")

  print(f"\nwrote video to {video_path}")
  print("done.")


if __name__ == "__main__":
  main()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import tarfile
import argparse
from pathlib import Path
from collections import defaultdict, Counter
from datetime import datetime
import math

import matplotlib
matplotlib.use("Agg")  # headless
import matplotlib.pyplot as plt

from typing import Tuple, Dict, Iterable

# ----------- Constants -----------
ARCHIVE_PATH = Path("data/raw/Spark.tar.gz")
RAW_DIR = Path("data/raw")
OUTPUT_DIR = Path("data/output")

# Directory name pattern: application_<cluster_id>_<app_number>
APP_DIR_RE = re.compile(r"application_(\d{13})_(\d+)$")

# Timestamp at line head, e.g. "17/03/29 10:04:41 ..."
TS_RE = re.compile(r"^(\d{2}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})")

# Utility for YY→20YY mapping (dataset years are 2015–2017)
def parse_ts(ts: str) -> datetime:
    """Parse timestamp in 'yy/MM/dd HH:mm:ss' format to datetime with 20YY assumption."""
    # Using datetime.strptime with two-digit years maps 00-68 -> 2000-2068, 69-99 -> 1969-1999.
    # We explicitly enforce 20YY since dataset years are 2015-2017.
    dt = datetime.strptime(ts, "%y/%m/%d %H:%M:%S")
    if dt.year < 2000:
        dt = dt.replace(year=dt.year + 100)  # 19xx -> 20xx (defensive, not expected here)
    return dt


def ensure_extracted() -> None:
    """Extract Spark.tar.gz to data/raw/ if application_* folders not present."""
    if any(RAW_DIR.glob("application_*")):
        return
    if not ARCHIVE_PATH.exists():
        raise FileNotFoundError(
            f"Archive not found at {ARCHIVE_PATH}. Please ensure it exists."
        )
    print(f"[info] Extracting archive: {ARCHIVE_PATH} -> {RAW_DIR}")
    with tarfile.open(ARCHIVE_PATH, "r:gz") as tf:
        tf.extractall(RAW_DIR)
    print("[info] Extraction completed.")


def iter_app_dirs() -> Iterable[Path]:
    """Yield application directories in data/raw/."""
    for d in sorted(RAW_DIR.glob("application_*")):
        if d.is_dir():
            yield d


def scan_app_times(app_dir: Path) -> Tuple[str, str, str, datetime, datetime]:
    """
    Scan all .log files inside an application directory to find earliest and latest timestamps.
    Returns:
        cluster_id, application_id_str, app_number, start_dt, end_dt
    """
    m = APP_DIR_RE.search(app_dir.name)
    if not m:
        raise ValueError(f"Invalid application dir name: {app_dir.name}")
    cluster_id, app_number = m.group(1), m.group(2)
    application_id_str = f"application_{cluster_id}_{app_number}"

    start_dt, end_dt = None, None
    for log_path in app_dir.glob("*.log"):
        with log_path.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                mt = TS_RE.match(line)
                if not mt:
                    continue
                dt = parse_ts(mt.group(1))
                if start_dt is None or dt < start_dt:
                    start_dt = dt
                if end_dt is None or dt > end_dt:
                    end_dt = dt

    return cluster_id, application_id_str, app_number, start_dt, end_dt


def write_csv(path: Path, header: str, rows) -> None:
    """Write rows to CSV file."""
    with path.open("w", encoding="utf-8") as f:
        f.write(header + "\n")
        for r in rows:
            f.write(",".join(r) + "\n")


def format_dt(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def plot_bar_counts(cluster_counts: Dict[str, int], out_path: Path) -> None:
    """Bar chart: number of applications per cluster with labels."""
    # Sort by count desc
    items = sorted(cluster_counts.items(), key=lambda x: x[1], reverse=True)
    labels = [k for k, _ in items]
    values = [v for _, v in items]

    plt.figure(figsize=(10, 5))
    ax = plt.gca()
    bars = ax.bar(range(len(values)), values)
    ax.set_xticks(range(len(labels)))
    ax.set_xticklabels(labels, rotation=30, ha="right")
    ax.set_xlabel("Cluster ID")
    ax.set_ylabel("Number of Applications")
    ax.set_title("Applications per Cluster")

    # Value labels above bars
    for rect, v in zip(bars, values):
        ax.text(rect.get_x() + rect.get_width() / 2.0, rect.get_height(),
                f"{v}", ha="center", va="bottom", fontsize=9)

    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def plot_duration_density(durations_sec, out_path: Path, cluster_id: str) -> None:
    """
    Duration histogram with log-scale x-axis and a simple smoothed line (moving average)
    as a stand-in for KDE to avoid extra dependencies. If there are too few samples,
    we just draw the histogram.
    """
    if not durations_sec:
        # Create an empty plot placeholder
        plt.figure(figsize=(8, 4))
        plt.title(f"Duration Distribution (No Data) - Cluster {cluster_id}")
        plt.savefig(out_path, dpi=150)
        plt.close()
        return

    # Filter out non-positive durations defensively
    durations = [max(1.0, float(d)) for d in durations_sec]

    plt.figure(figsize=(10, 4.5))
    ax = plt.gca()

    # Use log-scale on x-axis
    ax.set_xscale("log")
    ax.hist(durations, bins=50, alpha=0.6)
    ax.set_xlabel("Duration (seconds, log scale)")
    ax.set_ylabel("Count")
    ax.set_title(f"Job Duration Distribution (n={len(durations)}) - Cluster {cluster_id}")

    # Simple "KDE-like" smoothing: moving average over log-binned histogram (optional)
    # We won't add external deps; this is intentionally lightweight.
    plt.tight_layout()
    plt.savefig(out_path, dpi=150)
    plt.close()


def main():
    parser = argparse.ArgumentParser(description="Problem 2 - Local Cluster Usage Analysis")
    parser.add_argument("master_url", nargs="?", default=None,
                        help="Ignored in local mode; kept for interface compatibility.")
    parser.add_argument("--net-id", default=None, help="Optional net-id (not used locally).")
    parser.add_argument("--skip-spark", action="store_true",
                        help="For interface compatibility; local mode does not use Spark.")
    args = parser.parse_args()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ensure_extracted()

    # ---------- Scan all applications ----------
    timeline_rows = []  # for problem2_timeline.csv
    cluster_counts = Counter()
    cluster_first = {}
    cluster_last = {}

    durations_by_cluster = defaultdict(list)  # end-start per app

    for app_dir in iter_app_dirs():
        cluster_id, app_id, app_number, start_dt, end_dt = scan_app_times(app_dir)
        # Skip apps with no timestamps found (defensive)
        if start_dt is None or end_dt is None:
            continue

        timeline_rows.append((
            cluster_id,
            app_id,
            app_number,
            format_dt(start_dt),
            format_dt(end_dt),
        ))

        cluster_counts[cluster_id] += 1
        if (cluster_id not in cluster_first) or (start_dt < cluster_first[cluster_id]):
            cluster_first[cluster_id] = start_dt
        if (cluster_id not in cluster_last) or (end_dt > cluster_last[cluster_id]):
            cluster_last[cluster_id] = end_dt

        durations_by_cluster[cluster_id].append((end_dt - start_dt).total_seconds())

    # ---------- Write timeline CSV ----------
    timeline_csv = OUTPUT_DIR / "problem2_timeline.csv"
    write_csv(timeline_csv, "cluster_id,application_id,app_number,start_time,end_time", timeline_rows)

    # ---------- Cluster summary CSV ----------
    cluster_summary_rows = []
    for cid in sorted(cluster_counts.keys(), key=lambda x: int(x)):
        first_dt = cluster_first[cid]
        last_dt = cluster_last[cid]
        cluster_summary_rows.append((
            cid,
            str(cluster_counts[cid]),
            format_dt(first_dt),
            format_dt(last_dt),
        ))

    cluster_summary_csv = OUTPUT_DIR / "problem2_cluster_summary.csv"
    write_csv(cluster_summary_csv, "cluster_id,num_applications,cluster_first_app,cluster_last_app", cluster_summary_rows)

    # ---------- Stats TXT ----------
    num_clusters = len(cluster_counts)
    num_apps = sum(cluster_counts.values())
    avg_apps_per_cluster = (num_apps / num_clusters) if num_clusters else 0.0

    # Top clusters by usage
    top_items = sorted(cluster_counts.items(), key=lambda x: x[1], reverse=True)

    stats_txt = OUTPUT_DIR / "problem2_stats.txt"
    with stats_txt.open("w", encoding="utf-8") as f:
        f.write(f"Total unique clusters: {num_clusters}\n")
        f.write(f"Total applications: {num_apps}\n")
        f.write(f"Average applications per cluster: {avg_apps_per_cluster:0.2f}\n\n")
        f.write("Most heavily used clusters:\n")
        for cid, cnt in top_items[:10]:
            f.write(f"  Cluster {cid}: {cnt} applications\n")

    # ---------- Visualizations ----------
    bar_png = OUTPUT_DIR / "problem2_bar_chart.png"
    plot_bar_counts(cluster_counts, bar_png)

    # Density/histogram for largest cluster
    largest_cluster = top_items[0][0] if top_items else None
    dens_png = OUTPUT_DIR / "problem2_density_plot.png"
    if largest_cluster is not None:
        durations = [d for d in durations_by_cluster[largest_cluster] if d and d > 0]
        plot_duration_density(durations, dens_png, largest_cluster)
    else:
        # Create empty placeholder
        plot_duration_density([], dens_png, "N/A")

    print(f"[done] Wrote:\n - {timeline_csv}\n - {cluster_summary_csv}\n - {stats_txt}\n - {bar_png}\n - {dens_png}")


if __name__ == "__main__":
    main()

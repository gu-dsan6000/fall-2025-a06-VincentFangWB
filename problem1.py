#!/usr/bin/env python3

import os
import re
import tarfile
import random
from pathlib import Path
from collections import Counter, namedtuple
from typing import Iterable, Tuple

# ----------- Constants -----------
ARCHIVE_PATH = Path("data/raw/Spark.tar.gz")
RAW_DIR = Path("data/raw")
GLOB_PATTERN = "application_*/*.log"
OUTPUT_DIR = Path("data/output")

# Regex to extract log level from a line, being tolerant to varying prefixes.
LEVEL_RE = re.compile(r"\b(INFO|WARN|ERROR|DEBUG)\b")

# For optional timestamp extraction if you want to sanity-check lines
# TS_RE = re.compile(r"^(\d{2}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})")


def ensure_extracted() -> None:
    """Extract Spark.tar.gz to data/raw/ if application_* folders not present."""
    # Check if already extracted by looking for at least one application directory.
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


def iter_log_lines() -> Iterable[Tuple[Path, str]]:
    """Yield (path, line) pairs for all log files."""
    for log_path in RAW_DIR.glob(GLOB_PATTERN):
        # Use text mode with errors='ignore' to be resilient to odd encodings
        with log_path.open("r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                yield (log_path, line.rstrip("\n"))


def reservoir_sample(iterable: Iterable[Tuple[Path, str]], k: int, predicate) -> list:
    """Reservoir sampling for k items that satisfy predicate(line)."""
    sample = []
    n = 0
    for path, line in iterable:
        if not predicate(line):
            continue
        n += 1
        if len(sample) < k:
            sample.append((path, line))
        else:
            # Replace element with gradually decreasing probability
            j = random.randint(1, n)
            if j <= k:
                sample[j - 1] = (path, line)
    return sample


def main():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ensure_extracted()

    total_lines = 0
    level_lines = 0
    counts = Counter()

    # First pass: count levels, and do reservoir sample in one stream
    def has_level(s: str) -> bool:
        return bool(LEVEL_RE.search(s))

    # We need to stream twice: once for counting with sample; but we can do both together.
    # For sampling we must iterate once; to avoid double IO, we stream once and update counts + sample simultaneously.
    k = 10
    sample = []
    n_kept = 0

    for path, line in iter_log_lines():
        total_lines += 1
        m = LEVEL_RE.search(line)
        if m:
            level = m.group(1)
            counts[level] += 1
            level_lines += 1

            # reservoir logic (predicate satisfied)
            n_kept += 1
            if len(sample) < k:
                sample.append((path, line, level))
            else:
                j = random.randint(1, n_kept)
                if j <= k:
                    sample[j - 1] = (path, line, level)

    # ---------- Write outputs ----------
    # 1) counts CSV
    counts_csv = OUTPUT_DIR / "problem1_counts.csv"
    with counts_csv.open("w", encoding="utf-8") as f:
        f.write("log_level,count\n")
        # keep a deterministic order for readability
        for lvl in ["INFO", "WARN", "ERROR", "DEBUG"]:
            if counts[lvl] > 0:
                f.write(f"{lvl},{counts[lvl]}\n")

    # 2) sample CSV (quote original line)
    sample_csv = OUTPUT_DIR / "problem1_sample.csv"
    with sample_csv.open("w", encoding="utf-8") as f:
        f.write("log_entry,log_level\n")
        for _, line, lvl in sample:
            # Quote and escape inner quotes by doubling them
            quoted = '"' + line.replace('"', '""') + '"'
            f.write(f"{quoted},{lvl}\n")

    # 3) summary TXT
    summary_txt = OUTPUT_DIR / "problem1_summary.txt"
    def pct(x: int, denom: int) -> str:
        if denom == 0:
            return "0.00%"
        return f"{(x/denom)*100:0.2f}%"

    with summary_txt.open("w", encoding="utf-8") as f:
        f.write(f"Total log lines processed: {total_lines}\n")
        f.write(f"Total lines with log levels: {level_lines}\n")
        f.write(f"Unique log levels found: {sum(1 for c in counts.values() if c>0)}\n\n")
        f.write("Log level distribution:\n")
        for lvl in ["INFO", "WARN", "ERROR", "DEBUG"]:
            c = counts[lvl]
            f.write(f"  {lvl:<5}: {c:>10} ({pct(c, level_lines)})\n")

    print(f"[done] Wrote: {counts_csv}, {sample_csv}, {summary_txt}")


if __name__ == "__main__":
    # Make the run deterministic for unit tests if needed
    random.seed(42)
    main()

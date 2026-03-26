"""
main.py
-------
Entry point for the NGSIM Scenario Extraction Pipeline (Phase 1).

Usage
-----
Local test (first 100 k rows):
    python main.py --input data/trajectories-0750am-0805am.csv \
                   --output_csv output/scenario_samples.csv \
                   --nrows 100000

Full dataset, CSV output:
    python main.py --input data/trajectories-0750am-0805am.csv \
                   --output_csv output/scenario_samples.csv

Full dataset, BigQuery output:
    python main.py --input data/trajectories-0750am-0805am.csv \
                   --output_mode bigquery \
                   --bq_project my-project --bq_dataset ngsim --bq_table scenarios

GCS source:
    python main.py --use_gcs --gcs_bucket my-bucket \
                   --gcs_blob trajectories-0750am-0805am.csv \
                   --output_csv output/scenario_samples.csv
"""

import argparse
import logging
import sys
import time

import pandas as pd

from config import LOG_DATE_FMT, LOG_FORMAT
from data_loader import load_data
from preprocessing import preprocess
from scenario_detection import detect_scenarios
from segmentation import segment_scenarios
from storage import store_results

logger = logging.getLogger(__name__)


# ═════════════════════════════════════════════════════════════════════════════
# CLI arguments
# ═════════════════════════════════════════════════════════════════════════════

def _parse_args(argv=None):
    p = argparse.ArgumentParser(
        description="NGSIM US-101 Driving Scenario Extraction Pipeline"
    )
    # ── Input ─────────────────────────────────────────────────────────────
    p.add_argument("--input", default="data/trajectories-0750am-0805am.csv",
                   help="Path to local NGSIM CSV file.")
    p.add_argument("--use_gcs", action="store_true",
                   help="Load from Google Cloud Storage instead of local.")
    p.add_argument("--gcs_bucket", default=None)
    p.add_argument("--gcs_blob", default=None)
    p.add_argument("--nrows", type=int, default=None,
                   help="Limit number of rows to read (debug mode).")

    # ── Output ────────────────────────────────────────────────────────────
    p.add_argument("--output_csv", default="output/scenario_samples.csv",
                   help="Path for CSV output.")
    p.add_argument("--output_mode", default="csv",
                   choices=["csv", "bigquery", "both"],
                   help="Where to write results.")
    p.add_argument("--bq_project", default=None)
    p.add_argument("--bq_dataset", default=None)
    p.add_argument("--bq_table", default=None)

    # ── Metadata ──────────────────────────────────────────────────────────
    p.add_argument("--source_file_name", default=None,
                   help="Override source file label in output records.")

    return p.parse_args(argv)


# ═════════════════════════════════════════════════════════════════════════════
# Pipeline execution
# ═════════════════════════════════════════════════════════════════════════════

def run_pipeline(args):
    t0 = time.perf_counter()
    source_label = args.source_file_name or args.input

    # 1. Load ──────────────────────────────────────────────────────────────
    logger.info("═══ Step 1/5: Loading data ═══")
    source = "gcs" if args.use_gcs else "local"
    raw_df = load_data(
        source=source,
        local_path=args.input,
        gcs_bucket=args.gcs_bucket,
        gcs_blob=args.gcs_blob,
        nrows=args.nrows,
    )
    logger.info("Loaded %s rows, %d columns.", f"{len(raw_df):,}", raw_df.shape[1])

    # 2. Preprocess ────────────────────────────────────────────────────────
    logger.info("═══ Step 2/5: Preprocessing ═══")
    full_df, vehicle_groups = preprocess(raw_df)
    logger.info(
        "After preprocessing: %s rows, %d vehicles.",
        f"{len(full_df):,}", len(vehicle_groups),
    )

    # 3. Scenario Detection ────────────────────────────────────────────────
    logger.info("═══ Step 3/5: Scenario Detection ═══")
    events = detect_scenarios(full_df, vehicle_groups)
    _print_detection_summary(events)

    # 4. Segmentation ──────────────────────────────────────────────────────
    logger.info("═══ Step 4/5: Segmentation ═══")
    samples = segment_scenarios(
        events, full_df, vehicle_groups, source_file=source_label,
    )

    # 5. Storage ───────────────────────────────────────────────────────────
    logger.info("═══ Step 5/5: Storing results ═══")
    store_results(
        samples,
        output_mode=args.output_mode,
        csv_path=args.output_csv,
        bq_project=args.bq_project,
        bq_dataset=args.bq_dataset,
        bq_table=args.bq_table,
    )

    elapsed = time.perf_counter() - t0
    _print_final_summary(samples, elapsed)


# ═════════════════════════════════════════════════════════════════════════════
# Summary helpers
# ═════════════════════════════════════════════════════════════════════════════

def _print_detection_summary(events):
    from collections import Counter
    counts = Counter(e["scenario_type"] for e in events)
    logger.info("─── Detection Summary ───")
    for k, v in sorted(counts.items()):
        logger.info("  %-20s %s", k, f"{v:,}")
    logger.info("  %-20s %s", "TOTAL", f"{sum(counts.values()):,}")


def _print_final_summary(samples, elapsed):
    from collections import Counter
    counts = Counter(s["scenario_type"] for s in samples)
    logger.info("╔════════════════════════════════════════════════╗")
    logger.info("║          PIPELINE COMPLETE                    ║")
    logger.info("╠════════════════════════════════════════════════╣")
    logger.info("║  Total samples : %-8s                     ║", f"{len(samples):,}")
    for k, v in sorted(counts.items()):
        logger.info("║    %-14s : %-8s                   ║", k, f"{v:,}")
    logger.info("║  Elapsed       : %.2f s                       ║", elapsed)
    logger.info("╚════════════════════════════════════════════════╝")


# ═════════════════════════════════════════════════════════════════════════════
# Entry
# ═════════════════════════════════════════════════════════════════════════════

def main():
    logging.basicConfig(
        level=logging.INFO,
        format=LOG_FORMAT,
        datefmt=LOG_DATE_FMT,
    )
    args = _parse_args()
    logger.info("Arguments: %s", vars(args))
    run_pipeline(args)


if __name__ == "__main__":
    main()

"""
shared/storage.py
-----------------
Persist scenario samples to CSV (local testing) and / or Google BigQuery
(cloud deployment).

Nested fields (``surrounding_vehicle_ids``, ``vehicle_positions``) are
JSON-serialised so they fit into a flat table row that BigQuery can ingest
with ``autodetect=True``.
"""

import json
import logging
import os
from typing import Any, Dict, List

import pandas as pd

logger = logging.getLogger(__name__)


# =============================================================================
# Public API
# =============================================================================

def store_results(
    samples: List[Dict[str, Any]],
    output_mode: str = "csv",
    csv_path: str = "output/scenario_samples.csv",
    bq_project: str = None,
    bq_dataset: str = None,
    bq_table: str = None,
) -> None:
    """
    Write scenario samples to the requested backend(s).

    Parameters
    ----------
    samples : list[dict]
        Enriched samples from ``segmentation.segment_scenarios()``.
    output_mode : str
        ``"csv"`` | ``"bigquery"`` | ``"both"``.
    csv_path : str
        Output CSV file path (used when mode includes csv).
    bq_project, bq_dataset, bq_table : str
        BigQuery coordinates (required when mode includes bigquery).
    """
    if not samples:
        logger.warning("No samples to store — skipping.")
        return

    df = _samples_to_dataframe(samples)

    if output_mode in ("csv", "both"):
        _write_csv(df, csv_path)

    if output_mode in ("bigquery", "both"):
        _write_bigquery(df, bq_project, bq_dataset, bq_table)

    logger.info("Stored %s samples  (mode=%s).", f"{len(df):,}", output_mode)


# =============================================================================
# Private helpers
# =============================================================================

def _samples_to_dataframe(samples: List[Dict[str, Any]]) -> pd.DataFrame:
    """Flatten sample dicts into a DataFrame; serialise nested fields."""
    rows = []
    for s in samples:
        rows.append({
            "sample_id":               s["sample_id"],
            "scenario_type":           s["scenario_type"],
            "ego_vehicle_id":          s["ego_vehicle_id"],
            "surrounding_vehicle_ids": json.dumps(s.get("surrounding_vehicle_ids", [])),
            "start_time":              s["start_time"],
            "end_time":                s["end_time"],
            "ego_lane":                s["ego_lane"],
            "average_speed":           s["average_speed"],
            "min_distance_to_lead":    s.get("min_distance_to_lead"),
            "source_file":             s.get("source_file", ""),
            "vehicle_positions":       json.dumps(s.get("vehicle_positions", {})),
            "label":                   s.get("label", s["scenario_type"]),
            "confidence_score":        s.get("confidence_score"),
        })
    return pd.DataFrame(rows)


def _write_csv(df: pd.DataFrame, path: str) -> None:
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    df.to_csv(path, index=False)
    logger.info("CSV written → %s", path)


def _write_bigquery(
    df: pd.DataFrame,
    project: str,
    dataset: str,
    table: str,
) -> None:
    if not all([project, dataset, table]):
        raise ValueError(
            "bq_project, bq_dataset, and bq_table must all be set "
            "for BigQuery output."
        )

    try:
        from google.cloud import bigquery
    except ImportError:
        raise ImportError(
            "google-cloud-bigquery is not installed.  "
            "Run: pip install google-cloud-bigquery pyarrow"
        )

    client = bigquery.Client(project=project)
    table_ref = f"{project}.{dataset}.{table}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        autodetect=True,
    )
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()
    logger.info("BigQuery → %s  (%s rows written)", table_ref, job.output_rows)

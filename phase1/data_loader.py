"""
data_loader.py
--------------
Load the NGSIM vehicle trajectory dataset from a local CSV or from
Google Cloud Storage (GCS).

After loading, columns are renamed via ``config.COLUMN_RENAME_MAP``
and validated against ``config.REQUIRED_COLUMNS``.
"""

import logging
import os

import pandas as pd

from config import COLUMN_RENAME_MAP, REQUIRED_COLUMNS

logger = logging.getLogger(__name__)

# ── Optional GCS import (not needed for local testing) ────────────────────────
try:
    from google.cloud import storage as gcs_storage
except ImportError:
    gcs_storage = None


# ═════════════════════════════════════════════════════════════════════════════
# Public API
# ═════════════════════════════════════════════════════════════════════════════

def load_data(
    source: str = "local",
    local_path: str = "data/trajectories-0750am-0805am.csv",
    gcs_bucket: str = None,
    gcs_blob: str = None,
    nrows: int = None,
) -> pd.DataFrame:
    """
    Load and normalise NGSIM trajectory data.

    Parameters
    ----------
    source : str
        ``"local"`` to read from *local_path*, or ``"gcs"`` to pull
        from Cloud Storage.
    local_path : str
        Path to a local NGSIM CSV file (used when *source* is ``"local"``).
    gcs_bucket, gcs_blob : str
        GCS coordinates (only required when *source* is ``"gcs"``).
    nrows : int, optional
        Read only the first *nrows* data rows.  Useful for fast debugging.

    Returns
    -------
    pd.DataFrame
        Trajectory data with normalised column names.
    """
    if source == "gcs":
        df = _load_from_gcs(gcs_bucket, gcs_blob, nrows=nrows)
    else:
        df = _load_from_csv(local_path, nrows=nrows)

    df = _normalize_columns(df)
    _validate_columns(df)

    logger.info("Loaded %s rows × %s columns.", f"{len(df):,}", len(df.columns))
    return df


# ═════════════════════════════════════════════════════════════════════════════
# Private helpers
# ═════════════════════════════════════════════════════════════════════════════

def _load_from_csv(filepath: str, nrows: int = None) -> pd.DataFrame:
    if not os.path.isfile(filepath):
        raise FileNotFoundError(f"CSV file not found: {filepath}")
    logger.info("Reading local CSV: %s  (nrows=%s)", filepath, nrows)
    return pd.read_csv(filepath, nrows=nrows)


def _load_from_gcs(
    bucket_name: str, blob_name: str, nrows: int = None,
) -> pd.DataFrame:
    if gcs_storage is None:
        raise ImportError(
            "google-cloud-storage is not installed.  "
            "Run: pip install google-cloud-storage"
        )
    if not bucket_name or not blob_name:
        raise ValueError("--gcs_bucket and --gcs_blob are required for GCS.")

    client = gcs_storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)

    tmp = "/tmp/ngsim_data.csv"
    blob.download_to_filename(tmp)
    logger.info("Downloaded gs://%s/%s → %s", bucket_name, blob_name, tmp)
    return pd.read_csv(tmp, nrows=nrows)


def _normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Rename NGSIM headers → internal snake_case names.

    Strips whitespace first to handle minor formatting quirks.
    Columns not in the rename map are kept as-is.
    """
    df.columns = df.columns.str.strip()
    rename = {k: v for k, v in COLUMN_RENAME_MAP.items() if k in df.columns}
    df = df.rename(columns=rename)
    logger.info("Renamed %d columns → %s", len(rename), list(df.columns))
    return df


def _validate_columns(df: pd.DataFrame) -> None:
    """Raise ``ValueError`` if any pipeline-critical columns are missing."""
    missing = set(REQUIRED_COLUMNS) - set(df.columns)
    if missing:
        raise ValueError(
            f"Missing required columns after normalisation: {missing}.  "
            f"Available: {list(df.columns)}"
        )

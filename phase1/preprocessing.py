"""
preprocessing.py
----------------
Clean, sort, de-duplicate, and group the raw NGSIM trajectory data so
downstream modules receive per-vehicle trajectories ordered by time.

Key outputs
~~~~~~~~~~~
- ``vehicle_groups``:  dict of ``vehicle_id`` → sorted ``pd.DataFrame``.
- ``full_df``:  the complete sorted DataFrame.
"""

import logging

import numpy as np
import pandas as pd

from config import REQUIRED_COLUMNS

logger = logging.getLogger(__name__)


# ═════════════════════════════════════════════════════════════════════════════
# Public API
# ═════════════════════════════════════════════════════════════════════════════

def preprocess(df: pd.DataFrame):
    """
    Full preprocessing pipeline.

    Steps
    -----
    1. Cast critical columns to correct dtypes.
    2. Drop exact duplicate rows.
    3. Handle / drop rows with NaN in required columns.
    4. Sort globally by ``(vehicle_id, global_time)``.
    5. Group rows by ``vehicle_id``.

    Returns
    -------
    full_df : pd.DataFrame
        The complete sorted DataFrame.
    vehicle_groups : dict[int, pd.DataFrame]
        Per-vehicle DataFrames, each sorted by ``global_time``.
    """
    df = _cast_types(df)
    df = _drop_duplicates(df)
    df = _handle_missing(df)
    df = _sort(df)
    vehicle_groups = _group_by_vehicle(df)

    logger.info(
        "Preprocessing done: %s rows, %s unique vehicles.",
        f"{len(df):,}",
        f"{len(vehicle_groups):,}",
    )
    return df, vehicle_groups


# ═════════════════════════════════════════════════════════════════════════════
# Private helpers
# ═════════════════════════════════════════════════════════════════════════════

def _cast_types(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure numeric columns have the right dtype."""
    df = df.copy()
    df["vehicle_id"]  = df["vehicle_id"].astype(int)
    df["frame_id"]    = df["frame_id"].astype(int)
    df["global_time"] = df["global_time"].astype(np.int64)
    df["local_x"]     = df["local_x"].astype(float)
    df["local_y"]     = df["local_y"].astype(float)
    df["velocity"]    = df["velocity"].astype(float)
    df["lane_id"]     = df["lane_id"].astype(int)
    return df


def _drop_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """Remove exact duplicate rows (same vehicle + frame)."""
    before = len(df)
    df = df.drop_duplicates(subset=["vehicle_id", "frame_id"], keep="first")
    dropped = before - len(df)
    if dropped:
        logger.info("Dropped %d duplicate rows.", dropped)
    return df


def _handle_missing(df: pd.DataFrame) -> pd.DataFrame:
    """Drop rows where any required column is NaN."""
    before = len(df)
    df = df.dropna(subset=REQUIRED_COLUMNS)
    dropped = before - len(df)
    if dropped:
        logger.warning("Dropped %d rows with NaN in required columns.", dropped)
    return df


def _sort(df: pd.DataFrame) -> pd.DataFrame:
    return df.sort_values(["vehicle_id", "global_time"]).reset_index(drop=True)


def _group_by_vehicle(df: pd.DataFrame) -> dict:
    """
    Return ``{vehicle_id: DataFrame}`` where each DataFrame is already
    sorted by ``global_time``.
    """
    return {
        int(vid): vdf.reset_index(drop=True)
        for vid, vdf in df.groupby("vehicle_id")
    }

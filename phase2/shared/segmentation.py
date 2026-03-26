"""
shared/segmentation.py
-----------------------
Extract fixed 5-second (5 000 ms) windows for every detected scenario event
and enrich them with trajectory details for the ego vehicle *and* nearby
surrounding vehicles.

Each output sample is a self-contained record suitable for storage in
BigQuery or CSV.
"""

import json
import logging
import uuid
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from shared.config import (
    SEGMENT_DURATION_MS,
    SEGMENT_MIN_FRAMES,
    SURROUND_RADIUS_FT,
)

logger = logging.getLogger(__name__)


# =============================================================================
# Public API
# =============================================================================

def segment_scenarios(
    events: List[Dict[str, Any]],
    full_df: pd.DataFrame,
    vehicle_groups: dict,
    source_file: str = "unknown",
) -> List[Dict[str, Any]]:
    """
    For each raw scenario event, cut a 5-second window and build a
    structured output record.

    Parameters
    ----------
    events : list[dict]
        Raw events from ``scenario_detection.detect_scenarios()``.
    full_df : pd.DataFrame
        Complete sorted trajectory table.
    vehicle_groups : dict[int, pd.DataFrame]
        Per-vehicle trajectories.
    source_file : str
        Name of the input file (recorded in every sample).

    Returns
    -------
    list[dict]
        Enriched scenario samples.
    """
    samples: List[Dict[str, Any]] = []

    for ev in events:
        sample = _build_sample(ev, full_df, vehicle_groups, source_file)
        if sample is not None:
            samples.append(sample)

    logger.info(
        "Segmentation: %s samples from %s events.",
        f"{len(samples):,}", f"{len(events):,}",
    )
    return samples


# =============================================================================
# Private helpers
# =============================================================================

def _build_sample(
    event: Dict[str, Any],
    full_df: pd.DataFrame,
    vehicle_groups: dict,
    source_file: str,
) -> Optional[Dict[str, Any]]:
    """
    Cut a window ``[t_start, t_start + 5 000 ms]`` from the ego trajectory,
    find surrounding vehicles, and assemble the output record.
    """
    ego_vid   = event["ego_vehicle_id"]
    t_start   = event["start_time_ms"]
    t_end_win = t_start + SEGMENT_DURATION_MS

    ego_traj = vehicle_groups.get(ego_vid)
    if ego_traj is None or ego_traj.empty:
        return None

    # Slice the ego trajectory within the window
    mask = (
        (ego_traj["global_time"] >= t_start)
        & (ego_traj["global_time"] <= t_end_win)
    )
    ego_win = ego_traj[mask]

    if len(ego_win) < SEGMENT_MIN_FRAMES:
        return None  # incomplete window near file boundary

    # Ego statistics
    avg_speed  = round(float(ego_win["velocity"].mean()), 3)
    ego_lane   = int(ego_win["lane_id"].mode().iloc[0])

    ego_positions = list(zip(
        ego_win["local_x"].round(3).tolist(),
        ego_win["local_y"].round(3).tolist(),
    ))

    # Minimum distance to lead (from space_headway if available)
    min_dist_to_lead = None
    if "space_headway" in ego_win.columns:
        valid_hdwy = ego_win["space_headway"][ego_win["space_headway"] > 0]
        if not valid_hdwy.empty:
            min_dist_to_lead = round(float(valid_hdwy.min()), 3)

    # Surrounding vehicles at the midpoint of the window
    mid_idx = len(ego_win) // 2
    mid_row = ego_win.iloc[mid_idx]
    mid_time = int(mid_row["global_time"])
    mid_x, mid_y = float(mid_row["local_x"]), float(mid_row["local_y"])

    surround_ids = _find_surrounding(
        full_df, ego_vid, mid_time, mid_x, mid_y,
    )

    # Also include any vehicle IDs the detector already identified
    explicit = set(event.get("surrounding_vehicle_ids", []))
    surround_ids = sorted(set(surround_ids) | explicit)

    # Collect surrounding trajectories within the window
    surround_positions: Dict[str, list] = {}
    for svid in surround_ids:
        st = vehicle_groups.get(svid)
        if st is None:
            continue
        sm = (st["global_time"] >= t_start) & (st["global_time"] <= t_end_win)
        sw = st[sm]
        if not sw.empty:
            surround_positions[str(svid)] = list(zip(
                sw["local_x"].round(3).tolist(),
                sw["local_y"].round(3).tolist(),
            ))

    # Assemble output record
    return {
        "sample_id":               str(uuid.uuid4()),
        "scenario_type":           event["scenario_type"],
        "ego_vehicle_id":          ego_vid,
        "surrounding_vehicle_ids": surround_ids,
        "start_time":              t_start,
        "end_time":                t_end_win,
        "ego_lane":                ego_lane,
        "average_speed":           avg_speed,
        "min_distance_to_lead":    min_dist_to_lead,
        "source_file":             source_file,
        "vehicle_positions":       {
            "ego": ego_positions,
            **surround_positions,
        },
    }


def _find_surrounding(
    full_df: pd.DataFrame,
    ego_vid: int,
    time_ms: int,
    ego_x: float,
    ego_y: float,
) -> List[int]:
    """
    Find all vehicles within ``SURROUND_RADIUS_FT`` of the ego at a
    specific timestamp using Euclidean distance on (local_x, local_y).
    """
    snap = full_df[full_df["global_time"] == time_ms]
    if snap.empty:
        return []

    others = snap[snap["vehicle_id"] != ego_vid]
    dx = others["local_x"].values - ego_x
    dy = others["local_y"].values - ego_y
    dist = np.sqrt(dx ** 2 + dy ** 2)

    within = others[dist <= SURROUND_RADIUS_FT]
    return sorted(within["vehicle_id"].astype(int).unique().tolist())

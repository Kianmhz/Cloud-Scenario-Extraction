"""
scenario_detection.py
---------------------
Rule-based detection of driving scenarios from real NGSIM US-101 data.

Detected scenarios
~~~~~~~~~~~~~~~~~~
1. **Car-Following** – ego follows a lead vehicle in the same lane with a
   small gap and similar speed for ≥ 5 s.
2. **Lane Change** – ego vehicle's lane_id changes and the new lane
   persists (filters sensor noise).
3. **Overtaking** – ego starts behind a target, changes lane, is faster,
   and passes the target within a time window.

The real NGSIM dataset provides ``preceding_id`` and ``space_headway``
which we exploit directly for car-following detection (much more reliable
and faster than computing pairwise distances).

All distance / speed thresholds are in **feet / ft·s⁻¹** (native NGSIM
US-101 units).  See ``config.py`` for tuneable values.
"""

import logging
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

from config import (
    CF_MAX_DISTANCE_FT,
    CF_MAX_REL_SPEED_FT_S,
    CF_MIN_DURATION_MS,
    LC_POST_STABILITY_FRAMES,
    LC_PRE_STABILITY_FRAMES,
    OT_LOOKAHEAD_MS,
    OT_MAX_INITIAL_GAP_FT,
    OT_MIN_SPEED_ADVANTAGE,
)

logger = logging.getLogger(__name__)


# ═════════════════════════════════════════════════════════════════════════════
# Public API
# ═════════════════════════════════════════════════════════════════════════════

def detect_scenarios(
    full_df: pd.DataFrame,
    vehicle_groups: dict,
) -> List[Dict[str, Any]]:
    """
    Run all three scenario detectors on the preprocessed data.

    Parameters
    ----------
    full_df : pd.DataFrame
        Complete sorted trajectory table (with ``preceding_id``,
        ``space_headway``, etc.).
    vehicle_groups : dict[int, pd.DataFrame]
        Per-vehicle trajectories from preprocessing.

    Returns
    -------
    list[dict]
        Each dict is one raw scenario event with at minimum:
        ``ego_vehicle_id``, ``scenario_type``, ``start_time_ms``,
        ``end_time_ms``.
    """
    all_events: List[Dict[str, Any]] = []
    total = len(vehicle_groups)

    # ------------------------------------------------------------------
    # Build a fast frame-level lookup (numpy arrays) used by overtaking
    # ------------------------------------------------------------------
    frame_index = _build_frame_index(full_df)

    for idx, (vid, traj) in enumerate(vehicle_groups.items(), 1):
        if idx % max(1, total // 10) == 0 or idx == total:
            logger.info("Scenario detection: vehicle %d / %d …", idx, total)

        all_events.extend(_detect_car_following(vid, traj, vehicle_groups))
        lc_events = _detect_lane_changes(vid, traj)
        all_events.extend(lc_events)
        all_events.extend(
            _detect_overtaking(vid, traj, lc_events, frame_index)
        )

    logger.info(
        "Scenario detection complete — %s raw events "
        "(car_following=%d, lane_change=%d, overtaking=%d).",
        f"{len(all_events):,}",
        sum(1 for e in all_events if e["scenario_type"] == "car_following"),
        sum(1 for e in all_events if e["scenario_type"] == "lane_change"),
        sum(1 for e in all_events if e["scenario_type"] == "overtaking"),
    )
    return all_events


# ═════════════════════════════════════════════════════════════════════════════
# Frame index (numpy) for fast inter-vehicle lookups
# ═════════════════════════════════════════════════════════════════════════════

def _build_frame_index(df: pd.DataFrame) -> Dict[int, Dict[int, Any]]:
    """
    frame_id → lane_id → dict(vehicle_ids, local_ys, velocities)
    sorted ascending by local_y.
    """
    index: Dict[int, Dict[int, Any]] = {}
    for fid, fdf in df.groupby("frame_id"):
        lane_dict: Dict[int, Any] = {}
        for lid, ldf in fdf.groupby("lane_id"):
            s = ldf.sort_values("local_y")
            lane_dict[int(lid)] = {
                "vehicle_ids": s["vehicle_id"].values,
                "local_ys":    s["local_y"].values,
                "velocities":  s["velocity"].values,
            }
        index[int(fid)] = lane_dict
    return index


# ═════════════════════════════════════════════════════════════════════════════
# 1.  Car-Following
# ═════════════════════════════════════════════════════════════════════════════

def _detect_car_following(
    vid: int,
    traj: pd.DataFrame,
    vehicle_groups: dict,
) -> List[Dict[str, Any]]:
    """
    Exploit NGSIM's ``preceding_id`` and ``space_headway`` columns when
    available.  Fall back to computing from local_y if they are missing.

    A car-following *episode* starts when all conditions are met and ends
    when any condition breaks.  Only episodes lasting ≥ ``CF_MIN_DURATION_MS``
    are emitted.
    """
    events: List[Dict[str, Any]] = []

    has_preceding = "preceding_id" in traj.columns
    has_headway   = "space_headway" in traj.columns

    # Pre-extract numpy arrays
    times     = traj["global_time"].values.astype(np.int64)
    lanes     = traj["lane_id"].values.astype(int)
    ys        = traj["local_y"].values.astype(float)
    vs        = traj["velocity"].values.astype(float)
    prec_ids  = traj["preceding_id"].values.astype(int) if has_preceding else None
    hdwys     = traj["space_headway"].values.astype(float) if has_headway else None

    # State machine for tracking episodes per leader
    # leader_vid → (start_time_ms, last_time_ms)
    active: Dict[int, List[int]] = {}

    for k in range(len(times)):
        t      = int(times[k])
        lane   = int(lanes[k])
        ego_y  = ys[k]
        ego_v  = vs[k]

        leader_vid: Optional[int] = None
        gap: Optional[float] = None
        leader_v: Optional[float] = None

        # ── Identify leader and gap ──────────────────────────────────────
        if has_preceding and prec_ids[k] != 0:
            leader_vid = int(prec_ids[k])
            gap = float(hdwys[k]) if has_headway else None

            # Look up leader's velocity in vehicle_groups at same time
            leader_traj = vehicle_groups.get(leader_vid)
            if leader_traj is not None:
                lt_row = leader_traj[leader_traj["global_time"] == t]
                if not lt_row.empty:
                    leader_v = float(lt_row.iloc[0]["velocity"])
                    if gap is None or gap <= 0:
                        leader_y = float(lt_row.iloc[0]["local_y"])
                        gap = leader_y - ego_y

        # ── Evaluate car-following conditions ────────────────────────────
        following = False
        if (
            leader_vid is not None
            and gap is not None
            and leader_v is not None
            and 0 < gap < CF_MAX_DISTANCE_FT
            and abs(ego_v - leader_v) < CF_MAX_REL_SPEED_FT_S
        ):
            following = True

        if following:
            if leader_vid not in active:
                active[leader_vid] = [t, t]
            else:
                active[leader_vid][1] = t
        else:
            # Flush completed episodes
            events.extend(_flush_cf(vid, active))
            active.clear()

    # End-of-trajectory flush
    events.extend(_flush_cf(vid, active))
    return events


def _flush_cf(
    ego_vid: int, active: Dict[int, List[int]]
) -> List[Dict[str, Any]]:
    """Emit car-following events that meet the minimum duration."""
    out = []
    for leader_vid, (t_start, t_end) in active.items():
        if (t_end - t_start) >= CF_MIN_DURATION_MS:
            out.append({
                "ego_vehicle_id":         int(ego_vid),
                "scenario_type":          "car_following",
                "start_time_ms":          int(t_start),
                "end_time_ms":            int(t_end),
                "surrounding_vehicle_ids": [int(leader_vid)],
                "detail": {
                    "leader_id": int(leader_vid),
                },
            })
    return out


# ═════════════════════════════════════════════════════════════════════════════
# 2.  Lane Change
# ═════════════════════════════════════════════════════════════════════════════

def _detect_lane_changes(
    vid: int, traj: pd.DataFrame,
) -> List[Dict[str, Any]]:
    """
    Detect lane changes with *stability filtering*:

    A transition at index *i* (``lane[i-1] != lane[i]``) is accepted only
    when the old lane was constant for ``LC_PRE_STABILITY_FRAMES`` before *i*
    **and** the new lane is constant for ``LC_POST_STABILITY_FRAMES`` after *i*.

    This removes flicker caused by vehicles straddling lane markings.
    """
    events: List[Dict[str, Any]] = []

    lanes  = traj["lane_id"].values.astype(int)
    times  = traj["global_time"].values.astype(np.int64)
    n      = len(lanes)

    pre  = LC_PRE_STABILITY_FRAMES
    post = LC_POST_STABILITY_FRAMES

    for i in range(pre, n - post):
        if lanes[i] == lanes[i - 1]:
            continue  # no transition here

        old_lane = int(lanes[i - 1])
        new_lane = int(lanes[i])

        # Check pre-stability: lanes[i-pre : i] all equal old_lane
        if not np.all(lanes[i - pre: i] == old_lane):
            continue

        # Check post-stability: lanes[i : i+post] all equal new_lane
        if not np.all(lanes[i: i + post] == new_lane):
            continue

        events.append({
            "ego_vehicle_id":         int(vid),
            "scenario_type":          "lane_change",
            "start_time_ms":          int(times[i - 1]),
            "end_time_ms":            int(times[i]),
            "surrounding_vehicle_ids": [],
            "detail": {
                "lane_from": old_lane,
                "lane_to":   new_lane,
                "frame_idx":  int(i),   # index into the traj df
            },
        })

    return events


# ═════════════════════════════════════════════════════════════════════════════
# 3.  Overtaking
# ═════════════════════════════════════════════════════════════════════════════

def _detect_overtaking(
    vid: int,
    traj: pd.DataFrame,
    lane_change_events: List[Dict[str, Any]],
    frame_index: Dict[int, Dict[int, Any]],
) -> List[Dict[str, Any]]:
    """
    Overtaking = lane-change + ego was behind a target vehicle in the
    original lane + ego is faster + ego passes that target.

    For each confirmed lane-change event we:
    1. Find the nearest vehicle **ahead** of ego in the *original* lane
       just before the change (the "target").
    2. Check that ego speed > target speed by at least
       ``OT_MIN_SPEED_ADVANTAGE``.
    3. Scan frames after the lane change (up to ``OT_LOOKAHEAD_MS``) to
       see if ego's local_y exceeds the target's.
    """
    events: List[Dict[str, Any]] = []

    times     = traj["global_time"].values.astype(np.int64)
    frame_ids = traj["frame_id"].values.astype(int)
    local_ys  = traj["local_y"].values.astype(float)
    vels      = traj["velocity"].values.astype(float)

    for lc in lane_change_events:
        idx_lc = lc["detail"]["frame_idx"]  # index in traj
        orig_lane = lc["detail"]["lane_from"]
        t_lc = int(times[idx_lc])
        fid_before = int(frame_ids[idx_lc - 1])
        ego_y_before = local_ys[idx_lc - 1]
        ego_v_after  = vels[idx_lc]

        # Find nearest vehicle ahead in the original lane
        target_vid, target_y = _find_leader_in_frame(
            fid_before, orig_lane, vid, ego_y_before, frame_index,
        )
        if target_vid is None:
            continue
        if (target_y - ego_y_before) > OT_MAX_INITIAL_GAP_FT:
            continue  # too far away

        # Target speed at same frame
        target_v = _get_speed_in_frame(
            fid_before, orig_lane, target_vid, frame_index,
        )
        if target_v is None:
            continue

        # Speed advantage check
        if ego_v_after - target_v < OT_MIN_SPEED_ADVANTAGE:
            continue

        # Scan ahead to see if ego passes target
        t_max = t_lc + OT_LOOKAHEAD_MS
        passed = False
        t_pass = t_lc

        for j in range(idx_lc, len(times)):
            if times[j] > t_max:
                break
            fid_j = int(frame_ids[j])
            ty = _get_y_in_frame(fid_j, orig_lane, target_vid, frame_index)
            if ty is not None and local_ys[j] > ty:
                passed = True
                t_pass = int(times[j])
                break

        if passed:
            events.append({
                "ego_vehicle_id":         int(vid),
                "scenario_type":          "overtaking",
                "start_time_ms":          int(times[idx_lc - 1]),
                "end_time_ms":            int(t_pass),
                "surrounding_vehicle_ids": [int(target_vid)],
                "detail": {
                    "target_id": int(target_vid),
                    "lane_from": orig_lane,
                    "lane_to":   lc["detail"]["lane_to"],
                },
            })

    return events


# ═════════════════════════════════════════════════════════════════════════════
# Fast numpy look-ups on the frame index
# ═════════════════════════════════════════════════════════════════════════════

def _find_leader_in_frame(
    frame_id: int, lane_id: int, ego_vid: int, ego_y: float,
    frame_index: Dict[int, Dict[int, Any]],
):
    """Nearest vehicle ahead (higher local_y) in the same lane & frame."""
    ld = frame_index.get(frame_id, {}).get(lane_id)
    if ld is None:
        return None, None
    vids, ys = ld["vehicle_ids"], ld["local_ys"]
    mask = (vids != ego_vid) & (ys > ego_y)
    if not np.any(mask):
        return None, None
    first = np.argmax(mask)       # sorted ascending → first True is closest
    return int(vids[first]), float(ys[first])


def _get_speed_in_frame(
    frame_id: int, lane_id: int, target_vid: int,
    frame_index: Dict[int, Dict[int, Any]],
):
    ld = frame_index.get(frame_id, {}).get(lane_id)
    if ld is None:
        return None
    mask = ld["vehicle_ids"] == target_vid
    if not np.any(mask):
        return None
    return float(ld["velocities"][mask][0])


def _get_y_in_frame(
    frame_id: int, lane_id: int, target_vid: int,
    frame_index: Dict[int, Dict[int, Any]],
):
    ld = frame_index.get(frame_id, {}).get(lane_id)
    if ld is None:
        return None
    mask = ld["vehicle_ids"] == target_vid
    if not np.any(mask):
        return None
    return float(ld["local_ys"][mask][0])

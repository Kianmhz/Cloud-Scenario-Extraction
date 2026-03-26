"""
shared/config.py
----------------
Centralised configuration for the NGSIM Scenario Extraction Pipeline.

All tuneable thresholds, column mappings, and constants live here so that
no magic numbers appear in the processing modules.

Units (matching NGSIM US-101 dataset)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
- Distance:  **feet (ft)**
- Speed:     **feet per second (ft/s)**
- Time:      **milliseconds (ms)** for ``global_time``.
- Recording: 10 Hz (100 ms between frames).
"""

# =============================================================================
# Column Mapping – NGSIM raw headers → internal snake_case names
# =============================================================================

COLUMN_RENAME_MAP = {
    "Vehicle_ID":   "vehicle_id",
    "Frame_ID":     "frame_id",
    "Global_Time":  "global_time",
    "Local_X":      "local_x",
    "Local_Y":      "local_y",
    "v_Vel":        "velocity",
    "Lane_ID":      "lane_id",
    "Preceeding":   "preceding_id",      # NGSIM typo preserved
    "Space_Hdwy":   "space_headway",
}

# Columns that **must** exist after normalisation for the pipeline to run.
REQUIRED_COLUMNS = [
    "vehicle_id",
    "frame_id",
    "global_time",
    "local_x",
    "local_y",
    "velocity",
    "lane_id",
]

# =============================================================================
# 1.  Car-Following thresholds
# =============================================================================
CF_MAX_DISTANCE_FT    = 150.0   # max bumper-to-bumper gap (ft)
CF_MAX_REL_SPEED_FT_S = 15.0    # max |ego_v − leader_v| (ft/s)
CF_MIN_DURATION_MS    = 5000     # minimum episode length (ms) → 5 s

# =============================================================================
# 2.  Lane-Change stability filter
# =============================================================================
LC_PRE_STABILITY_FRAMES  = 10   # constant-lane frames required BEFORE change
LC_POST_STABILITY_FRAMES = 10   # constant-lane frames required AFTER  change

# =============================================================================
# 3.  Overtaking
# =============================================================================
OT_LOOKAHEAD_MS        = 10000  # scan window after lane change (ms) → 10 s
OT_MIN_SPEED_ADVANTAGE = 3.0    # ego speed − target speed (ft/s)
OT_MAX_INITIAL_GAP_FT  = 200.0  # max gap to target at start (ft)

# =============================================================================
# 4.  Segmentation
# =============================================================================
SEGMENT_DURATION_MS = 5000      # output window length (ms) → 5 s
SEGMENT_MIN_FRAMES  = 40        # minimum ego frames inside window (≈ 4 s)
SURROUND_RADIUS_FT  = 300.0     # radius for surrounding-vehicle search (ft)

# =============================================================================
# Logging
# =============================================================================
LOG_FORMAT   = "%(asctime)s  %(levelname)-8s  %(name)s  %(message)s"
LOG_DATE_FMT = "%Y-%m-%d %H:%M:%S"

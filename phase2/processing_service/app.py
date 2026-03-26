"""
processing_service/app.py
--------------------------
Loads NGSIM trajectory data, runs the full preprocessing + scenario detection
+ segmentation pipeline, and returns raw (unlabeled) scenario samples.

BONUS: Supports parallel vehicle processing via ``parallel=true`` query param,
       using a ThreadPoolExecutor to speed up detection across many vehicles.

Routes
------
POST /process    - Run the detection pipeline; returns {raw_samples, stats}
GET  /health     - Health check
GET  /metrics    - Service metrics (bonus)
"""

import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Dict, List, Optional

# Make 'shared' importable when running inside Docker (PYTHONPATH=/app)
sys.path.insert(0, "/app")

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from shared.data_loader import load_data
from shared.preprocessing import preprocess
from shared.scenario_detection import (
    _build_frame_index,
    _detect_car_following,
    _detect_lane_changes,
    _detect_overtaking,
    detect_scenarios,
)
from shared.segmentation import segment_scenarios

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="NGSIM Processing Service",
    description=(
        "Loads NGSIM trajectory data and runs preprocessing, scenario "
        "detection, and 5-second segmentation.  Supports parallel "
        "processing for improved throughput (bonus feature)."
    ),
    version="2.0.0",
)

# ── Configuration ─────────────────────────────────────────────────────────────
SERVICE_NAME = "processing-service"
MAX_WORKERS  = int(os.getenv("MAX_WORKERS", "4"))   # parallel worker count

# ── Metrics (bonus: monitoring) ───────────────────────────────────────────────
_metrics = {
    "requests_total":  0,
    "files_processed": 0,
    "samples_produced": 0,
    "errors":          0,
    "start_time":      time.time(),
}


# =============================================================================
# Request schema
# =============================================================================

class ProcessRequest(BaseModel):
    file_path:   str
    source_name: Optional[str] = None
    nrows:       Optional[int] = None
    use_gcs:     bool = False
    gcs_bucket:  Optional[str] = None
    gcs_blob:    Optional[str] = None
    parallel:    bool = False       # BONUS: enable parallel vehicle processing


# =============================================================================
# Routes
# =============================================================================

@app.post("/process")
def process_data(req: ProcessRequest):
    """
    Load NGSIM data from *file_path*, run the full detection pipeline,
    and return raw (unlabeled) scenario samples.

    Set ``parallel=true`` to enable the bonus parallel processing mode,
    which distributes vehicle-level detection across multiple threads.
    """
    _metrics["requests_total"] += 1
    t0 = time.perf_counter()

    try:
        # ── Step 1: Load ──────────────────────────────────────────────────
        logger.info("Step 1/4: Loading data from %s", req.file_path)
        if req.use_gcs:
            raw_df = load_data(
                source="gcs",
                gcs_bucket=req.gcs_bucket,
                gcs_blob=req.gcs_blob,
                nrows=req.nrows,
            )
        else:
            raw_df = load_data(
                source="local",
                local_path=req.file_path,
                nrows=req.nrows,
            )

        # ── Step 2: Preprocess ────────────────────────────────────────────
        logger.info("Step 2/4: Preprocessing (%s rows)", f"{len(raw_df):,}")
        full_df, vehicle_groups = preprocess(raw_df)

        # ── Step 3: Scenario Detection ────────────────────────────────────
        logger.info(
            "Step 3/4: Scenario detection (%d vehicles, parallel=%s)",
            len(vehicle_groups), req.parallel,
        )
        if req.parallel:
            events = _detect_parallel(full_df, vehicle_groups, MAX_WORKERS)
        else:
            events = detect_scenarios(full_df, vehicle_groups)

        # ── Step 4: Segmentation ──────────────────────────────────────────
        logger.info("Step 4/4: Segmentation (%d events)", len(events))
        source_name = req.source_name or req.file_path
        samples = segment_scenarios(
            events, full_df, vehicle_groups, source_file=source_name,
        )

        elapsed = time.perf_counter() - t0

        # Build per-type counts
        counts: Dict[str, int] = {}
        for s in samples:
            counts[s["scenario_type"]] = counts.get(s["scenario_type"], 0) + 1

        _metrics["files_processed"] += 1
        _metrics["samples_produced"] += len(samples)

        logger.info(
            "Processing complete: %d samples in %.2f s  (parallel=%s)",
            len(samples), elapsed, req.parallel,
        )

        return {
            "status": "success",
            "raw_samples": samples,
            "stats": {
                "total_rows":          len(raw_df),
                "total_vehicles":      len(vehicle_groups),
                "total_events":        len(events),
                "total_samples":       len(samples),
                "scenario_counts":     counts,
                "elapsed_seconds":     round(elapsed, 3),
                "parallel_processing": req.parallel,
                "max_workers":         MAX_WORKERS if req.parallel else 1,
            },
        }

    except FileNotFoundError as exc:
        _metrics["errors"] += 1
        raise HTTPException(status_code=404, detail=str(exc))
    except Exception as exc:
        _metrics["errors"] += 1
        logger.error("Processing failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Processing failed: {exc}")


@app.get("/health")
def health():
    return {
        "status":         "healthy",
        "service":        SERVICE_NAME,
        "max_workers":    MAX_WORKERS,
        "uptime_seconds": round(time.time() - _metrics["start_time"], 2),
    }


@app.get("/metrics")
def metrics():
    """Per-service metrics for the aggregate /metrics endpoint in the gateway."""
    return {
        "service": SERVICE_NAME,
        **_metrics,
        "uptime_seconds": round(time.time() - _metrics["start_time"], 2),
    }


# =============================================================================
# BONUS: Parallel detection
# =============================================================================

def _detect_parallel(
    full_df,
    vehicle_groups: dict,
    max_workers: int = 4,
) -> List[Dict[str, Any]]:
    """
    BONUS – Parallel scenario detection.

    Builds the shared frame_index once (read-only), then fans out
    per-vehicle detection across a thread pool.  Errors in individual
    vehicle threads are logged and skipped rather than aborting the run.

    Note: Python's GIL limits true parallelism for CPU-bound code, but
    the NumPy operations inside the detectors release the GIL, so we
    observe real speedups on multi-core machines.
    """
    frame_index = _build_frame_index(full_df)
    all_events: List[Dict[str, Any]] = []

    def _process_one_vehicle(item):
        vid, traj = item
        evts: List[Dict[str, Any]] = []
        evts.extend(_detect_car_following(vid, traj, vehicle_groups))
        lc_evts = _detect_lane_changes(vid, traj)
        evts.extend(lc_evts)
        evts.extend(_detect_overtaking(vid, traj, lc_evts, frame_index))
        return evts

    logger.info(
        "Parallel detection: %d vehicles across %d workers",
        len(vehicle_groups), max_workers,
    )

    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {
            pool.submit(_process_one_vehicle, item): item[0]
            for item in vehicle_groups.items()
        }
        for future in as_completed(futures):
            vid = futures[future]
            try:
                all_events.extend(future.result())
            except Exception as exc:
                logger.warning("Vehicle %d detection error (skipped): %s", vid, exc)

    cf = sum(1 for e in all_events if e["scenario_type"] == "car_following")
    lc = sum(1 for e in all_events if e["scenario_type"] == "lane_change")
    ot = sum(1 for e in all_events if e["scenario_type"] == "overtaking")
    logger.info(
        "Parallel detection done: %d events  (cf=%d, lc=%d, ot=%d)",
        len(all_events), cf, lc, ot,
    )
    return all_events

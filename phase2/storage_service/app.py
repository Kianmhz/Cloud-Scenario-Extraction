"""
storage_service/app.py
-----------------------
Persists labeled scenario samples to CSV (local) and/or Google BigQuery
(cloud).  Also provides a query API for retrieving stored scenarios.

Routes
------
POST /store                 - Persist labeled samples
GET  /scenarios             - Query stored scenarios (with filtering & pagination)
GET  /scenarios/{sample_id} - Retrieve a specific scenario by ID
GET  /health                - Health check
GET  /metrics               - Service metrics (bonus)
"""

import json
import logging
import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional

# Make 'shared' importable inside Docker (PYTHONPATH=/app)
sys.path.insert(0, "/app")

from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from shared.storage import store_results

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="NGSIM Storage Service",
    description=(
        "Persists labeled scenario samples to CSV and/or Google BigQuery. "
        "Provides a query API for retrieving and filtering stored scenarios."
    ),
    version="2.0.0",
)

# ── Configuration ─────────────────────────────────────────────────────────────
SERVICE_NAME  = "storage-service"
OUTPUT_DIR    = Path(os.getenv("OUTPUT_DIR",   "/data/outputs"))
BQ_PROJECT    = os.getenv("BQ_PROJECT",   "")
BQ_DATASET    = os.getenv("BQ_DATASET",   "ngsim_scenarios")
BQ_TABLE      = os.getenv("BQ_TABLE",     "scenario_samples")
STORAGE_MODE  = os.getenv("STORAGE_MODE", "csv")   # csv | bigquery | both

# ── In-memory scenario cache (for the query API) ──────────────────────────────
_scenario_cache: Dict[str, Dict[str, Any]] = {}

# ── Metrics (bonus: monitoring) ───────────────────────────────────────────────
_metrics = {
    "requests_total":  0,
    "samples_stored":  0,
    "queries_served":  0,
    "errors":          0,
    "start_time":      time.time(),
}


# =============================================================================
# Request schemas
# =============================================================================

class StoreRequest(BaseModel):
    labeled_samples: List[Dict[str, Any]]
    output_mode:     Optional[str] = None   # overrides STORAGE_MODE env var


# =============================================================================
# Routes
# =============================================================================

@app.post("/store")
def store_samples(req: StoreRequest):
    """
    Persist a list of labeled scenario samples.

    Writes to CSV (default), BigQuery, or both depending on the
    ``STORAGE_MODE`` environment variable or the request's ``output_mode``.
    """
    _metrics["requests_total"] += 1

    if not req.labeled_samples:
        raise HTTPException(status_code=400, detail="No samples provided.")

    try:
        mode = req.output_mode or STORAGE_MODE
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        csv_path = str(OUTPUT_DIR / "scenario_samples.csv")

        store_results(
            samples     = req.labeled_samples,
            output_mode = mode,
            csv_path    = csv_path,
            bq_project  = BQ_PROJECT  or None,
            bq_dataset  = BQ_DATASET  or None,
            bq_table    = BQ_TABLE    or None,
        )

        # Update the in-memory cache for the query API
        for s in req.labeled_samples:
            _scenario_cache[s["sample_id"]] = s

        _metrics["samples_stored"] += len(req.labeled_samples)

        logger.info(
            "Stored %d samples (mode=%s, csv=%s)",
            len(req.labeled_samples), mode, csv_path,
        )

        return {
            "status":       "success",
            "stored_count": len(req.labeled_samples),
            "sample_ids":   [s["sample_id"] for s in req.labeled_samples],
            "storage_mode": mode,
            "csv_path":     csv_path if "csv" in mode else None,
            "bigquery_table": (
                f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
                if "bigquery" in mode else None
            ),
        }

    except Exception as exc:
        _metrics["errors"] += 1
        logger.error("Storage failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Storage failed: {exc}")


@app.get("/scenarios")
def get_scenarios(
    scenario_type:  Optional[str] = Query(None, description="Filter by scenario type"),
    ego_vehicle_id: Optional[int] = Query(None, description="Filter by ego vehicle ID"),
    min_confidence: Optional[float] = Query(None, description="Minimum confidence score"),
    limit:  int = Query(50, le=1000, ge=1, description="Max results to return"),
    offset: int = Query(0,  ge=0,         description="Pagination offset"),
):
    """
    Query stored scenarios with optional filtering and pagination.

    Supports filtering by ``scenario_type``, ``ego_vehicle_id``, and
    ``min_confidence`` (bonus: confidence-based filtering).
    """
    _metrics["requests_total"] += 1
    _metrics["queries_served"] += 1

    results = list(_scenario_cache.values())

    if scenario_type:
        results = [r for r in results if r.get("scenario_type") == scenario_type]
    if ego_vehicle_id is not None:
        results = [r for r in results if r.get("ego_vehicle_id") == ego_vehicle_id]
    if min_confidence is not None:
        results = [
            r for r in results
            if float(r.get("confidence_score", 0.0)) >= min_confidence
        ]

    total   = len(results)
    results = results[offset: offset + limit]

    return {
        "total":   total,
        "limit":   limit,
        "offset":  offset,
        "results": results,
    }


@app.get("/scenarios/{sample_id}")
def get_scenario(sample_id: str):
    """Retrieve a single scenario sample by its UUID."""
    _metrics["requests_total"] += 1

    if sample_id not in _scenario_cache:
        raise HTTPException(
            status_code=404, detail=f"Scenario '{sample_id}' not found."
        )
    return _scenario_cache[sample_id]


@app.get("/health")
def health():
    return {
        "status":             "healthy",
        "service":            SERVICE_NAME,
        "storage_mode":       STORAGE_MODE,
        "cached_scenarios":   len(_scenario_cache),
        "uptime_seconds":     round(time.time() - _metrics["start_time"], 2),
    }


@app.get("/metrics")
def metrics():
    return {
        "service": SERVICE_NAME,
        **_metrics,
        "cached_scenarios_count": len(_scenario_cache),
        "uptime_seconds":         round(time.time() - _metrics["start_time"], 2),
    }

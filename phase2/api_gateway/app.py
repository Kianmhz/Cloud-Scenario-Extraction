"""
api_gateway/app.py
-------------------
Main entry point for the NGSIM Phase 2 microservices system.

Orchestrates the full 4-stage pipeline:
  Ingestion → Processing → Labeling → Storage

Also provides:
  - Scenario query API (proxied to Storage Service)
  - Aggregate health check across all services
  - Aggregate metrics endpoint (bonus: monitoring)

Routes
------
POST /pipeline/run           - Upload file and run the full pipeline
GET  /pipeline/status/{id}   - Check a pipeline job's status
GET  /scenarios              - Query stored scenarios
GET  /scenarios/{id}         - Retrieve a specific scenario
GET  /services/health        - Health check across all downstream services
GET  /health                 - Gateway health check
GET  /metrics                - Aggregate metrics from all services (bonus)
"""

import logging
import os
import time
import uuid
from typing import Any, Dict, List, Optional

import httpx
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import JSONResponse
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="NGSIM Scenario Extraction – API Gateway",
    description=(
        "Microservices-based NGSIM driving scenario extraction system (Phase 2). "
        "Orchestrates Ingestion → Processing → Labeling → Storage services. "
        "Includes parallel processing and confidence scoring (bonus features)."
    ),
    version="2.0.0",
)

# ── Service URLs (injected via environment variables / Kubernetes ConfigMap) ───
INGESTION_URL  = os.getenv("INGESTION_SERVICE_URL",  "http://ingestion:8001")
PROCESSING_URL = os.getenv("PROCESSING_SERVICE_URL", "http://processing:8002")
LABELING_URL   = os.getenv("LABELING_SERVICE_URL",   "http://labeling:8003")
STORAGE_URL    = os.getenv("STORAGE_SERVICE_URL",    "http://storage:8004")

SERVICE_NAME     = "api-gateway"
REQUEST_TIMEOUT  = float(os.getenv("REQUEST_TIMEOUT",  "600"))   # 10 min max
USE_PARALLEL     = os.getenv("USE_PARALLEL_PROCESSING", "false").lower() == "true"

# ── In-memory pipeline job tracker ────────────────────────────────────────────
_pipeline_jobs: Dict[str, Dict[str, Any]] = {}

# ── Gateway metrics (bonus: monitoring) ───────────────────────────────────────
_metrics = {
    "pipeline_runs":    0,
    "successful_runs":  0,
    "failed_runs":      0,
    "start_time":       time.time(),
}


# =============================================================================
# Pipeline endpoint
# =============================================================================

@app.post("/pipeline/run")
async def run_pipeline(
    file:     UploadFile = File(...),
    parallel: bool = Query(False, description="Enable parallel vehicle processing (bonus)"),
):
    """
    Run the full NGSIM scenario extraction pipeline.

    **Pipeline stages**
    1. **Ingestion** – upload the CSV file to shared storage
    2. **Processing** – load data, detect scenarios, produce 5-second samples
    3. **Labeling** – validate labels and compute confidence scores
    4. **Storage** – persist results to CSV (and optionally BigQuery)

    Set ``parallel=true`` to enable the bonus parallel processing mode.

    Returns a complete summary including per-stage stats and overall metrics.
    """
    _metrics["pipeline_runs"] += 1
    job_id = str(uuid.uuid4())
    _pipeline_jobs[job_id] = {
        "status":     "running",
        "started_at": time.time(),
        "stage":      "initialising",
    }

    t0 = time.perf_counter()

    try:
        async with httpx.AsyncClient(timeout=REQUEST_TIMEOUT) as client:

            # ── Stage 1: Ingestion ────────────────────────────────────────
            _pipeline_jobs[job_id]["stage"] = "ingestion"
            logger.info("[%s] Stage 1/4: Ingesting '%s'", job_id, file.filename)

            content = await file.read()
            ingest_resp = await client.post(
                f"{INGESTION_URL}/ingest",
                files={"file": (file.filename, content, "text/csv")},
            )
            _assert_ok(ingest_resp, "Ingestion", job_id)
            ingest_data = ingest_resp.json()
            file_path   = ingest_data["file_path"]

            logger.info("[%s] Ingestion done → %s", job_id, file_path)

            # ── Stage 2: Processing ───────────────────────────────────────
            _pipeline_jobs[job_id]["stage"] = "processing"
            logger.info("[%s] Stage 2/4: Processing (parallel=%s)", job_id, parallel)

            process_resp = await client.post(
                f"{PROCESSING_URL}/process",
                json={
                    "file_path":   file_path,
                    "source_name": file.filename,
                    "parallel":    parallel or USE_PARALLEL,
                },
            )
            _assert_ok(process_resp, "Processing", job_id)
            process_data    = process_resp.json()
            raw_samples     = process_data["raw_samples"]
            processing_stats = process_data["stats"]

            logger.info("[%s] Processing done: %d samples", job_id, len(raw_samples))

            # ── Stage 3: Labeling ─────────────────────────────────────────
            _pipeline_jobs[job_id]["stage"] = "labeling"
            logger.info("[%s] Stage 3/4: Labeling %d samples", job_id, len(raw_samples))

            label_resp = await client.post(
                f"{LABELING_URL}/label",
                json={"raw_samples": raw_samples},
            )
            _assert_ok(label_resp, "Labeling", job_id)
            label_data      = label_resp.json()
            labeled_samples = label_data["labeled_samples"]
            labeling_stats  = label_data["stats"]

            logger.info("[%s] Labeling done: %d labeled", job_id, len(labeled_samples))

            # ── Stage 4: Storage ──────────────────────────────────────────
            _pipeline_jobs[job_id]["stage"] = "storage"
            logger.info("[%s] Stage 4/4: Storing %d samples", job_id, len(labeled_samples))

            store_resp = await client.post(
                f"{STORAGE_URL}/store",
                json={"labeled_samples": labeled_samples},
            )
            _assert_ok(store_resp, "Storage", job_id)
            store_data = store_resp.json()

            logger.info("[%s] Storage done: %d persisted", job_id, store_data["stored_count"])

        # ── Finalise ──────────────────────────────────────────────────────
        elapsed = time.perf_counter() - t0
        _metrics["successful_runs"] += 1

        avg_confidence = (
            sum(s.get("confidence_score", 0.0) for s in labeled_samples)
            / len(labeled_samples)
            if labeled_samples else 0.0
        )

        result = {
            "job_id":  job_id,
            "status":  "success",
            "elapsed_seconds": round(elapsed, 3),
            "pipeline_stages": {
                "ingestion": {
                    "file_path":  file_path,
                    "size_bytes": ingest_data["size_bytes"],
                    "storage":    ingest_data.get("storage", "local"),
                },
                "processing": processing_stats,
                "labeling":   labeling_stats,
                "storage": {
                    "stored_count":  store_data["stored_count"],
                    "storage_mode":  store_data["storage_mode"],
                    "csv_path":      store_data.get("csv_path"),
                    "bigquery_table": store_data.get("bigquery_table"),
                },
            },
            "summary": {
                "total_samples":          store_data["stored_count"],
                "scenario_counts":        processing_stats.get("scenario_counts", {}),
                "average_confidence":     round(avg_confidence, 3),
                "parallel_processing":    parallel or USE_PARALLEL,
            },
        }

        _pipeline_jobs[job_id].update({
            "status":          "completed",
            "elapsed_seconds": round(elapsed, 3),
            "result":          result["summary"],
        })

        return result

    except HTTPException:
        raise
    except Exception as exc:
        _metrics["failed_runs"] += 1
        _pipeline_jobs[job_id].update({"status": "failed", "error": str(exc)})
        logger.error("[%s] Pipeline failed: %s", job_id, exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Pipeline error: {exc}")


# =============================================================================
# Job status
# =============================================================================

@app.get("/pipeline/status/{job_id}")
def get_pipeline_status(job_id: str):
    """Return the current status of a pipeline job."""
    if job_id not in _pipeline_jobs:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
    return _pipeline_jobs[job_id]


# =============================================================================
# Scenario query (proxied to Storage Service)
# =============================================================================

@app.get("/scenarios")
async def get_scenarios(
    scenario_type:  Optional[str]   = Query(None),
    ego_vehicle_id: Optional[int]   = Query(None),
    min_confidence: Optional[float] = Query(None, description="Minimum confidence (bonus)"),
    limit:  int = Query(50,  le=1000),
    offset: int = Query(0,   ge=0),
):
    """Query stored scenarios — proxied to the Storage Service."""
    params: Dict[str, Any] = {"limit": limit, "offset": offset}
    if scenario_type  is not None: params["scenario_type"]  = scenario_type
    if ego_vehicle_id is not None: params["ego_vehicle_id"] = ego_vehicle_id
    if min_confidence is not None: params["min_confidence"] = min_confidence

    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(f"{STORAGE_URL}/scenarios", params=params)
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail="Storage service error.")
    return resp.json()


@app.get("/scenarios/{sample_id}")
async def get_scenario(sample_id: str):
    """Retrieve a specific scenario — proxied to the Storage Service."""
    async with httpx.AsyncClient(timeout=30) as client:
        resp = await client.get(f"{STORAGE_URL}/scenarios/{sample_id}")
    if resp.status_code == 404:
        raise HTTPException(status_code=404, detail="Scenario not found.")
    if resp.status_code != 200:
        raise HTTPException(status_code=502, detail="Storage service error.")
    return resp.json()


# =============================================================================
# Health & Metrics
# =============================================================================

@app.get("/services/health")
async def services_health():
    """
    Aggregate health check — pings every downstream service in parallel
    and returns a combined status report.
    """
    services = {
        "ingestion":  INGESTION_URL,
        "processing": PROCESSING_URL,
        "labeling":   LABELING_URL,
        "storage":    STORAGE_URL,
    }
    statuses: Dict[str, Any] = {}
    all_healthy = True

    async with httpx.AsyncClient(timeout=5) as client:
        for name, url in services.items():
            try:
                resp = await client.get(f"{url}/health")
                if resp.status_code == 200:
                    statuses[name] = {"status": "healthy", **resp.json()}
                else:
                    statuses[name] = {"status": "unhealthy", "http_code": resp.status_code}
                    all_healthy = False
            except Exception as exc:
                statuses[name] = {"status": "unreachable", "error": str(exc)}
                all_healthy = False

    return {
        "gateway":              "healthy",
        "all_services_healthy": all_healthy,
        "services":             statuses,
    }


@app.get("/health")
def health():
    return {
        "status":         "healthy",
        "service":        SERVICE_NAME,
        "upstream_urls": {
            "ingestion":  INGESTION_URL,
            "processing": PROCESSING_URL,
            "labeling":   LABELING_URL,
            "storage":    STORAGE_URL,
        },
        "uptime_seconds": round(time.time() - _metrics["start_time"], 2),
    }


@app.get("/metrics")
async def aggregate_metrics():
    """
    BONUS – Aggregate metrics collected from all services.

    Useful for monitoring dashboards (e.g., Grafana + Prometheus).
    """
    all_metrics: Dict[str, Any] = {
        "gateway": {
            **_metrics,
            "uptime_seconds": round(time.time() - _metrics["start_time"], 2),
        }
    }

    services = {
        "ingestion":  INGESTION_URL,
        "processing": PROCESSING_URL,
        "labeling":   LABELING_URL,
        "storage":    STORAGE_URL,
    }

    async with httpx.AsyncClient(timeout=5) as client:
        for name, url in services.items():
            try:
                resp = await client.get(f"{url}/metrics")
                all_metrics[name] = resp.json() if resp.status_code == 200 else {"status": "error"}
            except Exception:
                all_metrics[name] = {"status": "unreachable"}

    return all_metrics


# =============================================================================
# Private helpers
# =============================================================================

def _assert_ok(resp: httpx.Response, stage: str, job_id: str) -> None:
    """Raise an HTTPException with a clear message if a service call fails."""
    if resp.status_code != 200:
        _metrics["failed_runs"] += 1
        _pipeline_jobs[job_id].update({"status": "failed", "stage": stage.lower()})
        raise HTTPException(
            status_code=502,
            detail=(
                f"{stage} service returned HTTP {resp.status_code}: "
                f"{resp.text[:300]}"
            ),
        )

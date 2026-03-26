"""
ingestion_service/app.py
-------------------------
Accepts NGSIM CSV file uploads, saves to shared storage (local volume or GCS),
and returns a file reference for downstream processing.

Routes
------
POST /ingest        - Upload a CSV file; returns {job_id, file_path, ...}
GET  /status/{id}   - Check ingestion job status
GET  /health        - Health check
GET  /metrics       - Service metrics (bonus: monitoring)
"""

import logging
import os
import time
import uuid
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, File, HTTPException, UploadFile
from fastapi.responses import JSONResponse

# Optional GCS support
try:
    from google.cloud import storage as gcs_storage
    GCS_AVAILABLE = True
except ImportError:
    gcs_storage = None
    GCS_AVAILABLE = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="NGSIM Ingestion Service",
    description=(
        "Handles NGSIM CSV file uploads and stores them to local shared "
        "storage or Google Cloud Storage for downstream processing."
    ),
    version="2.0.0",
)

# ── Configuration from environment variables ──────────────────────────────────
DATA_DIR    = Path(os.getenv("DATA_DIR", "/data/uploads"))
GCS_BUCKET  = os.getenv("GCS_BUCKET", "")
USE_GCS     = os.getenv("USE_GCS", "false").lower() == "true" and GCS_AVAILABLE
SERVICE_NAME = "ingestion-service"

# ── In-memory job store (use Redis in production) ─────────────────────────────
_jobs: dict = {}

# ── Metrics counters (bonus: monitoring) ──────────────────────────────────────
_metrics = {
    "requests_total":  0,
    "files_ingested":  0,
    "bytes_received":  0,
    "errors":          0,
    "start_time":      time.time(),
}


# =============================================================================
# Routes
# =============================================================================

@app.post("/ingest")
async def ingest_file(file: UploadFile = File(...)):
    """
    Accept a NGSIM CSV file upload.

    Saves the file to local shared storage (or GCS if configured).
    Returns a ``file_path`` that the Processing Service uses to load the data.
    """
    _metrics["requests_total"] += 1

    if not file.filename.endswith(".csv"):
        _metrics["errors"] += 1
        raise HTTPException(status_code=400, detail="Only CSV files are supported.")

    job_id = str(uuid.uuid4())

    try:
        content = await file.read()
        _metrics["bytes_received"] += len(content)

        if USE_GCS:
            file_path = await _save_to_gcs(job_id, file.filename, content)
        else:
            file_path = _save_to_local(job_id, file.filename, content)

        _jobs[job_id] = {
            "status":     "completed",
            "file_path":  str(file_path),
            "filename":   file.filename,
            "size_bytes": len(content),
            "timestamp":  time.time(),
        }

        _metrics["files_ingested"] += 1
        logger.info(
            "Ingested '%s' → %s  (job_id=%s, %.1f KB)",
            file.filename, file_path, job_id, len(content) / 1024,
        )

        return {
            "job_id":     job_id,
            "file_path":  str(file_path),
            "filename":   file.filename,
            "size_bytes": len(content),
            "status":     "completed",
            "storage":    "gcs" if USE_GCS else "local",
        }

    except Exception as exc:
        _metrics["errors"] += 1
        _jobs[job_id] = {"status": "failed", "error": str(exc)}
        logger.error("Ingestion failed for '%s': %s", file.filename, exc)
        raise HTTPException(status_code=500, detail=f"Ingestion failed: {exc}")


@app.get("/status/{job_id}")
def get_status(job_id: str):
    """Check the status of a previously submitted ingestion job."""
    if job_id not in _jobs:
        raise HTTPException(status_code=404, detail=f"Job '{job_id}' not found.")
    return _jobs[job_id]


@app.get("/health")
def health():
    """Health check endpoint — used by Kubernetes liveness/readiness probes."""
    return {
        "status":         "healthy",
        "service":        SERVICE_NAME,
        "storage_mode":   "gcs" if USE_GCS else "local",
        "data_dir":       str(DATA_DIR),
        "uptime_seconds": round(time.time() - _metrics["start_time"], 2),
    }


@app.get("/metrics")
def metrics():
    """Prometheus-style metrics for monitoring (bonus)."""
    return {
        "service": SERVICE_NAME,
        **_metrics,
        "uptime_seconds": round(time.time() - _metrics["start_time"], 2),
    }


# =============================================================================
# Private helpers
# =============================================================================

def _save_to_local(job_id: str, filename: str, content: bytes) -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    dest = DATA_DIR / f"{job_id}_{filename}"
    dest.write_bytes(content)
    return dest


async def _save_to_gcs(job_id: str, filename: str, content: bytes) -> str:
    if not GCS_AVAILABLE:
        raise RuntimeError("google-cloud-storage package is not installed.")
    client = gcs_storage.Client()
    bucket = client.bucket(GCS_BUCKET)
    blob_name = f"uploads/{job_id}_{filename}"
    blob = bucket.blob(blob_name)
    blob.upload_from_string(content, content_type="text/csv")
    return f"gs://{GCS_BUCKET}/{blob_name}"

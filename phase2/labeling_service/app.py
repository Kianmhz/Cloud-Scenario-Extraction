"""
labeling_service/app.py
------------------------
Validates raw scenario samples and enriches them with:
  - Human-readable labels (e.g., "Car-Following")
  - Confidence scores (rule-based quality metric, 0.0–1.0)
  - Label version metadata

BONUS: Confidence scoring evaluates multiple quality dimensions per
       scenario type, providing a measure of detection reliability.

Routes
------
POST /label    - Enrich raw samples with labels and confidence scores
GET  /health   - Health check
GET  /metrics  - Service metrics (bonus)
"""

import json
import logging
import time
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="NGSIM Labeling Service",
    description=(
        "Validates and enriches raw scenario samples with human-readable "
        "labels and rule-based confidence scores.  Confidence scoring is a "
        "bonus feature that quantifies detection quality on a 0–1 scale."
    ),
    version="2.0.0",
)

# ── Configuration ─────────────────────────────────────────────────────────────
SERVICE_NAME = "labeling-service"
LABEL_VERSION = "v1.0"

VALID_TYPES = {"car_following", "lane_change", "overtaking"}

HUMAN_LABELS = {
    "car_following": "Car-Following",
    "lane_change":   "Lane Change",
    "overtaking":    "Overtaking",
}

# ── Metrics (bonus: monitoring) ───────────────────────────────────────────────
_metrics = {
    "requests_total":  0,
    "samples_labeled": 0,
    "samples_skipped": 0,
    "errors":          0,
    "start_time":      time.time(),
}


# =============================================================================
# Request / Response schemas
# =============================================================================

class LabelRequest(BaseModel):
    raw_samples: List[Dict[str, Any]]


# =============================================================================
# Routes
# =============================================================================

@app.post("/label")
def label_samples(req: LabelRequest):
    """
    Validate and enrich scenario samples.

    Each sample receives:
    - ``label``           : Human-readable scenario name
    - ``confidence_score``: Float in [0.0, 1.0] reflecting detection quality
    - ``label_version``   : Version tag for reproducibility
    """
    _metrics["requests_total"] += 1

    if not req.raw_samples:
        raise HTTPException(status_code=400, detail="No samples provided.")

    try:
        labeled:  List[Dict[str, Any]] = []
        skipped:  List[str]            = []

        for sample in req.raw_samples:
            enriched = _enrich(sample)
            if enriched is not None:
                labeled.append(enriched)
            else:
                skipped.append(sample.get("sample_id", "?"))

        _metrics["samples_labeled"] += len(labeled)
        _metrics["samples_skipped"] += len(skipped)

        # Per-type label distribution
        distribution: Dict[str, int] = {}
        for s in labeled:
            t = s["scenario_type"]
            distribution[t] = distribution.get(t, 0) + 1

        # Average confidence
        avg_conf = (
            sum(s["confidence_score"] for s in labeled) / len(labeled)
            if labeled else 0.0
        )

        logger.info(
            "Labeled %d samples (skipped %d).  Avg confidence=%.3f",
            len(labeled), len(skipped), avg_conf,
        )

        return {
            "status": "success",
            "labeled_samples": labeled,
            "stats": {
                "total_input":         len(req.raw_samples),
                "labeled":             len(labeled),
                "skipped":             len(skipped),
                "label_distribution":  distribution,
                "average_confidence":  round(avg_conf, 3),
                "label_version":       LABEL_VERSION,
            },
        }

    except Exception as exc:
        _metrics["errors"] += 1
        logger.error("Labeling failed: %s", exc, exc_info=True)
        raise HTTPException(status_code=500, detail=f"Labeling failed: {exc}")


@app.get("/health")
def health():
    return {
        "status":         "healthy",
        "service":        SERVICE_NAME,
        "label_version":  LABEL_VERSION,
        "uptime_seconds": round(time.time() - _metrics["start_time"], 2),
    }


@app.get("/metrics")
def metrics():
    return {
        "service": SERVICE_NAME,
        **_metrics,
        "uptime_seconds": round(time.time() - _metrics["start_time"], 2),
    }


# =============================================================================
# Private helpers
# =============================================================================

def _enrich(sample: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Validate a single sample and attach label + confidence score.
    Returns ``None`` for samples with an unrecognised scenario_type.
    """
    scenario_type = sample.get("scenario_type", "")

    if scenario_type not in VALID_TYPES:
        logger.warning("Unknown scenario_type '%s' — skipping.", scenario_type)
        return None

    confidence = _compute_confidence(sample)

    return {
        **sample,
        "label":            HUMAN_LABELS[scenario_type],
        "confidence_score": confidence,
        "label_version":    LABEL_VERSION,
    }


def _compute_confidence(sample: Dict[str, Any]) -> float:
    """
    BONUS – Rule-based confidence scoring.

    Produces a float in [0.0, 1.0] that reflects how strongly the sample
    exhibits the characteristics of its labelled scenario type.

    Scoring dimensions
    ~~~~~~~~~~~~~~~~~~
    - Base score:        0.40  (always granted for passing basic validation)
    - Surrounding cars:  +0.10  (interaction evidence)
    - Realistic speed:   +0.10  (10–120 ft/s ≈ 7–82 mph)
    - Scenario-specific: up to +0.40  (type-dependent quality signal)

    Maximum possible: 1.00
    """
    score = 0.40   # base

    # ── Dimension 1: Has at least one surrounding vehicle ─────────────────
    surrounding = sample.get("surrounding_vehicle_ids", [])
    if isinstance(surrounding, str):
        try:
            surrounding = json.loads(surrounding)
        except Exception:
            surrounding = []
    if len(surrounding) > 0:
        score += 0.10

    # ── Dimension 2: Ego speed in a realistic highway range ───────────────
    avg_speed = sample.get("average_speed", 0.0)
    if 10.0 <= float(avg_speed) <= 120.0:
        score += 0.10

    # ── Dimension 3: Scenario-specific quality metrics ────────────────────
    scenario_type = sample.get("scenario_type", "")

    if scenario_type == "car_following":
        # Reward close following gaps (stronger signal than loose following)
        min_dist = sample.get("min_distance_to_lead")
        if min_dist is not None:
            if 0 < float(min_dist) < 50:
                score += 0.40   # very close following — high confidence
            elif 0 < float(min_dist) < 100:
                score += 0.25
            elif 0 < float(min_dist) < 150:
                score += 0.10
        else:
            score += 0.10       # no headway data, partial credit

    elif scenario_type == "lane_change":
        # Reward longer observation windows
        duration_ms = float(sample.get("end_time", 0)) - float(sample.get("start_time", 0))
        if duration_ms >= 5000:
            score += 0.40
        elif duration_ms >= 3000:
            score += 0.20
        elif duration_ms >= 1000:
            score += 0.10

    elif scenario_type == "overtaking":
        # Overtaking requires three independent conditions satisfied
        # simultaneously — inherently higher confidence when detected
        score += 0.40

    return round(min(score, 1.0), 3)

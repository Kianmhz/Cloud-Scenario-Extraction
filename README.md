# NGSIM Cloud-Based Scenario Extraction

**SOFE4630U – Cloud Computing | Group Project | Winter 2026**
**Group 2** — Bralyn Loach-Perry · Hanum Magaji · Kianmehr Haddad Zahmatkesh · Owais AbdurRahman

---

## Overview

Cloud-based system for ingesting, processing, and extracting labeled driving scenarios from the [NGSIM US-101 highway dataset](https://ops.fhwa.dot.gov/trafficanalysistools/ngsim.htm).

The system identifies three scenario types from real vehicle trajectory data:

| Scenario | Description |
|---|---|
| **Car-Following** | Ego vehicle follows a lead vehicle in the same lane with a small gap for ≥ 5 seconds |
| **Lane Change** | Ego vehicle transitions from one lane to another (noise-filtered) |
| **Overtaking** | Ego vehicle changes lanes, accelerates past a target vehicle, and completes the pass |

---

## Project Structure

```
Cloud/
├── phase1/          Phase 1 – Modular Monolithic System
│   ├── main.py
│   ├── config.py
│   ├── data_loader.py
│   ├── preprocessing.py
│   ├── scenario_detection.py
│   ├── segmentation.py
│   ├── storage.py
│   ├── visualize.py
│   ├── requirements.txt
│   ├── Dockerfile
│   ├── data/
│   └── output/
│
└── phase2/          Phase 2 – Microservices-Based System
    ├── shared/               Shared processing library
    ├── ingestion_service/    File upload & cloud storage
    ├── processing_service/   Scenario detection & segmentation
    ├── labeling_service/     Label validation & confidence scoring
    ├── storage_service/      Persistence & query API
    ├── api_gateway/          Pipeline orchestration entry point
    ├── docker-compose.yml    Local development environment
    └── k8s/                  Kubernetes manifests for GKE
```

---

## Phase 1 – Modular Monolith

A single Python application deployed on **Google Compute Engine**.  All pipeline stages (load → preprocess → detect → segment → store) run sequentially in one process.

See [phase1/README.md](phase1/README.md) for full usage and deployment instructions.

**Quick start:**
```bash
cd phase1
pip install -r requirements.txt
python main.py --input data/trajectories-0750am-0805am.csv \
               --output_csv output/scenario_samples.csv
```

---

## Phase 2 – Microservices System

Five independent services deployed on **Google Kubernetes Engine (GKE)**, each in its own Docker container, communicating over HTTP.

See [phase2/README.md](phase2/README.md) for full usage and deployment instructions.

**Quick start:**
```bash
cd phase2
docker compose up --build

# Run the full pipeline
curl -X POST http://localhost:8000/pipeline/run \
     -F "file=@../phase1/data/trajectories-0750am-0805am.csv"
```

---

## Dataset

| Property | Value |
|---|---|
| Source | NGSIM US-101, Los Angeles, CA |
| Segment | Lankershim Blvd on-ramp area |
| Time window | 7:50:39 AM – 8:04:29 AM (~15 min) |
| Rows | 1,048,575 trajectory frames |
| Unique vehicles | 1,993 |
| Sampling rate | 10 Hz (100 ms per frame) |
| Units | Distance: ft · Speed: ft/s · Time: ms |

---

## Cloud Infrastructure

| Component | Phase 1 | Phase 2 |
|---|---|---|
| Compute | Google Compute Engine (VM) | Google Kubernetes Engine |
| Raw storage | Google Cloud Storage (GCS) | Google Cloud Storage (GCS) |
| Processed storage | Google BigQuery | Google BigQuery |
| Containerisation | Docker | Docker + Kubernetes |

---

## Results (Phase 1 baseline)

| Scenario | Samples |
|---|---|
| Car-Following | 184 |
| Lane Change | 54 |
| Overtaking | 9 |
| **Total** | **247** |

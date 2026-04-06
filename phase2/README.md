# Phase 2 – Microservices-Based System

**SOFE4630U – Cloud Computing | Group 2 | Winter 2026**

The Phase 1 monolithic pipeline decomposed into five independent microservices, each in its own Docker container, communicating over HTTP REST. Deployed on **Google Kubernetes Engine (GKE)**.

---

## Architecture

```
                         ┌─────────────────────────────────────────┐
  Client                 │           API Gateway  :8000             │
  POST /pipeline/run ───►│  Orchestrates all stages                 │
                         │  GET /scenarios  GET /services/health    │
                         └──┬──────────┬──────────┬──────────┬──────┘
                            │          │          │          │
                      HTTP  │    HTTP  │    HTTP  │    HTTP  │
                            ▼          ▼          ▼          ▼
                    ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐
                    │Ingestion │ │Processing│ │ Labeling │ │ Storage  │
                    │  :8001   │ │  :8002   │ │  :8003   │ │  :8004   │
                    │          │ │          │ │          │ │          │
                    │Upload CSV│ │Detect    │ │Validate  │ │Persist   │
                    │→ GCS /   │ │scenarios │ │+ score   │ │CSV /     │
                    │  local   │ │segment   │ │confidence│ │BigQuery  │
                    └──────────┘ └──────────┘ └──────────┘ └──────────┘
```

**Pipeline stages:**
```
File Upload
  → Ingestion Service   saves CSV to shared storage, returns file_path
  → Processing Service  loads data, detects scenarios, produces raw samples
  → Labeling Service    validates labels, adds confidence scores (bonus)
  → Storage Service     persists to CSV / BigQuery, exposes query API
  → Client              receives full summary with per-stage stats
```

---

## Services

| Service | Port | Responsibility |
|---|---|---|
| **API Gateway** | 8000 | Orchestrates the pipeline; proxies scenario queries to Storage |
| **Ingestion** | 8001 | Accepts CSV file upload, saves to shared volume or GCS |
| **Processing** | 8002 | Loads data, runs preprocessing + scenario detection + segmentation |
| **Labeling** | 8003 | Validates scenario types, assigns human-readable labels and confidence scores |
| **Storage** | 8004 | Persists labeled samples to CSV/BigQuery; query API for retrieval |

Each service exposes:
- `GET /health` — liveness check (used by Kubernetes probes)
- `GET /metrics` — request counts, error rates, uptime (bonus: monitoring)

---

## Bonus Features

### Parallel Processing
The Processing Service can distribute vehicle-level detection across multiple threads:
```bash
curl -X POST "http://localhost:8000/pipeline/run?parallel=true" \
     -F "file=@data/trajectories.csv"
```
Controlled by `MAX_WORKERS` environment variable (default: 4).

### Confidence Scoring
The Labeling Service assigns each scenario sample a `confidence_score` (0.0–1.0) based on:
- **Car-Following:** space headway distance (closer gap = higher confidence)
- **Lane Change:** observation window duration
- **Overtaking:** all three conditions satisfied simultaneously (inherently high confidence)

Filter by confidence via the query API:
```bash
curl "http://localhost:8000/scenarios?min_confidence=0.8"
```

### Aggregate Monitoring
The API Gateway's `/metrics` endpoint polls all five services and returns a combined metrics report — suitable for integration with Grafana or Cloud Monitoring.

---

## Quick Start (local with Docker Compose)

```bash
# Start all 5 services
docker compose up --build

# Run the full pipeline
curl -X POST http://localhost:8000/pipeline/run \
     -F "file=@../phase1/data/trajectories-0750am-0805am.csv"

# Run with parallel processing (bonus)
curl -X POST "http://localhost:8000/pipeline/run?parallel=true" \
     -F "file=@../phase1/data/trajectories-0750am-0805am.csv"

# Check all services are healthy
curl http://localhost:8000/services/health

# Query extracted scenarios
curl "http://localhost:8000/scenarios?limit=10"
curl "http://localhost:8000/scenarios?scenario_type=car_following"
curl "http://localhost:8000/scenarios?min_confidence=0.8"

# Aggregate metrics (bonus)
curl http://localhost:8000/metrics

# Stop all services
docker compose down
```

---

## API Reference

### `POST /pipeline/run`
Upload a NGSIM CSV and run the full pipeline.

| Parameter | Type | Description |
|---|---|---|
| `file` | multipart file | NGSIM CSV file |
| `parallel` | bool (query) | Enable parallel processing (default: false) |

**Response:**
```json
{
  "job_id": "...",
  "status": "success",
  "elapsed_seconds": 45.2,
  "pipeline_stages": {
    "ingestion":   { "file_path": "...", "size_bytes": 123456 },
    "processing":  { "total_samples": 247, "scenario_counts": {...}, "elapsed_seconds": 44.1 },
    "labeling":    { "labeled": 247, "average_confidence": 0.73 },
    "storage":     { "stored_count": 247, "storage_mode": "csv" }
  },
  "summary": {
    "total_samples": 247,
    "scenario_counts": { "car_following": 184, "lane_change": 54, "overtaking": 9 },
    "average_confidence": 0.73,
    "parallel_processing": false
  }
}
```

### `GET /scenarios`
Query stored scenarios.

| Parameter | Description |
|---|---|
| `scenario_type` | Filter by `car_following`, `lane_change`, or `overtaking` |
| `ego_vehicle_id` | Filter by vehicle ID |
| `min_confidence` | Minimum confidence score (0.0–1.0) |
| `limit` | Max results (default 50, max 1000) |
| `offset` | Pagination offset |

### `GET /pipeline/status/{job_id}`
Check the status of a running or completed pipeline job.

### `GET /services/health`
Aggregate health check across all downstream services.

### `GET /metrics`
Aggregated metrics from all five services.

---

## Cloud Deployment (Google Kubernetes Engine)

### Prerequisites
- GCP project with GKE and Artifact Registry APIs enabled
- `gcloud` CLI authenticated
- `kubectl` configured for your cluster
- Docker available (Cloud Shell recommended)

### Steps

**1. Create a GKE cluster**
```bash
export PROJECT_ID=your-gcp-project-id
export REGION=your-region   # e.g. northamerica-northeast2

gcloud container clusters create ngsim-cluster \
  --region=$REGION \
  --machine-type=e2-small \
  --num-nodes=1

gcloud container clusters get-credentials ngsim-cluster --region=$REGION
```

**2. Create Artifact Registry repository and push images**
```bash
# Enable Artifact Registry API
gcloud services enable artifactregistry.googleapis.com

# Create Docker repository
gcloud artifacts repositories create ngsim \
  --repository-format=docker \
  --location=$REGION

# Configure Docker auth
gcloud auth configure-docker ${REGION}-docker.pkg.dev

# Set image registry prefix
export AR=${REGION}-docker.pkg.dev/$PROJECT_ID/ngsim

# Build and push all images
docker build -t $AR/ngsim-ingestion:latest  -f ingestion_service/Dockerfile  .
docker build -t $AR/ngsim-processing:latest -f processing_service/Dockerfile .
docker build -t $AR/ngsim-labeling:latest   -f labeling_service/Dockerfile   .
docker build -t $AR/ngsim-storage:latest    -f storage_service/Dockerfile    .
docker build -t $AR/ngsim-gateway:latest    -f api_gateway/Dockerfile        .

docker push $AR/ngsim-ingestion:latest
docker push $AR/ngsim-processing:latest
docker push $AR/ngsim-labeling:latest
docker push $AR/ngsim-storage:latest
docker push $AR/ngsim-gateway:latest
```

**3. Update image references**

Replace `REGION` and `YOUR_PROJECT_ID` in all `k8s/*-deployment.yaml` files with your actual region and project ID:
```bash
sed -i "s|REGION|${REGION}|g" k8s/*-deployment.yaml
sed -i "s|YOUR_PROJECT_ID|${PROJECT_ID}|g" k8s/*-deployment.yaml
```

**4. Grant Artifact Registry access to GKE nodes**
```bash
PROJECT_NUMBER=$(gcloud projects describe $PROJECT_ID --format="value(projectNumber)")
gcloud projects add-iam-policy-binding $PROJECT_ID \
  --member="serviceAccount:${PROJECT_NUMBER}-compute@developer.gserviceaccount.com" \
  --role="roles/artifactregistry.reader"
```

**5. Deploy to GKE**
```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/pvc.yaml
kubectl create serviceaccount ngsim-workload-sa -n ngsim
kubectl apply -f k8s/ingestion-deployment.yaml
kubectl apply -f k8s/processing-deployment.yaml
kubectl apply -f k8s/labeling-deployment.yaml
kubectl apply -f k8s/storage-deployment.yaml
kubectl apply -f k8s/api-gateway-deployment.yaml

# Get the external IP of the gateway
kubectl get service api-gateway-service -n ngsim
```

**6. Run the pipeline**
```bash
# Replace EXTERNAL_IP with the LoadBalancer IP from step 5
curl -X POST http://EXTERNAL_IP/pipeline/run \
     -F "file=@trajectories-0750am-0805am.csv"
```

> **Note:** Resource requests are tuned for `e2-small` nodes (2 vCPU, 2GB RAM).
> For larger CSV files, use `e2-medium` or higher machine types to avoid OOM errors during processing.

---

## Environment Variables

| Variable | Default | Used by |
|---|---|---|
| `DATA_DIR` | `/data/uploads` | Ingestion |
| `USE_GCS` | `false` | Ingestion |
| `GCS_BUCKET` | — | Ingestion |
| `MAX_WORKERS` | `4` | Processing |
| `OUTPUT_DIR` | `/data/outputs` | Storage |
| `STORAGE_MODE` | `csv` | Storage |
| `BQ_PROJECT` | — | Storage |
| `BQ_DATASET` | `ngsim_scenarios` | Storage |
| `BQ_TABLE` | `scenario_samples` | Storage |
| `INGESTION_SERVICE_URL` | `http://ingestion:8001` | Gateway |
| `PROCESSING_SERVICE_URL` | `http://processing:8002` | Gateway |
| `LABELING_SERVICE_URL` | `http://labeling:8003` | Gateway |
| `STORAGE_SERVICE_URL` | `http://storage:8004` | Gateway |
| `USE_PARALLEL_PROCESSING` | `false` | Gateway |
| `REQUEST_TIMEOUT` | `600` | Gateway |

---

## Phase 1 vs Phase 2 Comparison

| Aspect | Phase 1 (Monolith) | Phase 2 (Microservices) |
|---|---|---|
| **Deployment** | Single VM (GCE) | Kubernetes cluster (GKE) |
| **Scaling** | Entire app scales together | Each service scales independently |
| **Fault tolerance** | Single point of failure | Service-level fault isolation |
| **Communication** | In-process function calls | HTTP REST between containers |
| **Development** | Simpler — one codebase | More complex — 5 codebases |
| **Debugging** | Easier — single log stream | Harder — distributed logs |
| **Deployment overhead** | Low | Higher (Docker, k8s manifests) |
| **Processing speed** | Single-threaded | Parallel processing option |
| **Observability** | One log file | Per-service `/health` + `/metrics` |
| **BigQuery/GCS** | Direct SDK calls | Shared volume + Storage Service |

**Conclusion:** The monolithic system is simpler to develop, test, and debug. The microservices architecture adds operational complexity but enables independent scaling, fault isolation, and parallel processing — advantages that become significant at larger data volumes or higher request rates.

---

## Project Structure

```
phase2/
├── shared/                     Shared processing library (adapted from Phase 1)
│   ├── config.py
│   ├── data_loader.py
│   ├── preprocessing.py
│   ├── scenario_detection.py
│   ├── segmentation.py
│   └── storage.py
├── ingestion_service/
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── processing_service/
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── labeling_service/
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── storage_service/
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── api_gateway/
│   ├── app.py
│   ├── Dockerfile
│   └── requirements.txt
├── docker-compose.yml
└── k8s/
    ├── namespace.yaml
    ├── configmap.yaml
    ├── pvc.yaml
    ├── ingestion-deployment.yaml
    ├── processing-deployment.yaml
    ├── labeling-deployment.yaml
    ├── storage-deployment.yaml
    └── api-gateway-deployment.yaml
```

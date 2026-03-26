# Phase 1 – Modular Monolithic System

**SOFE4630U – Cloud Computing | Group 2 | Winter 2026**

A single cloud-based Python application that ingests raw NGSIM trajectory data, detects driving scenarios, and produces labeled 5-second samples. All pipeline stages are internally modular but deployed as one unit on **Google Compute Engine**.

---

## Architecture

```
main.py                     Entry point / CLI orchestrator
├── data_loader.py           Load CSV from local disk or GCS
├── preprocessing.py         Sort, cast, deduplicate, group by vehicle
├── scenario_detection.py    Rule-based detectors (car-following, lane change, overtaking)
├── segmentation.py          5-second window extraction + surrounding vehicle enrichment
├── storage.py               Write results to CSV and/or BigQuery
└── visualize.py             Trajectory & summary plots for validation
config.py                   All thresholds and constants (single source of truth)
```

**Data flow:**
```
Raw CSV → Load → Preprocess → Detect Scenarios → Segment (5 s windows) → Store
```

---

## Dataset

| Property | Value |
|---|---|
| Source | NGSIM US-101, Los Angeles, CA |
| Segment | Lankershim Blvd on-ramp |
| Time window | 7:50:39 AM – 8:04:29 AM (~15 min) |
| Rows | 1,048,575 trajectory frames |
| Unique vehicles | 1,993 |
| Sampling rate | 10 Hz (100 ms/frame) |
| Units | ft (distance) · ft/s (speed) · ms (time) |

---

## Scenario Detection Rules

| Scenario | Detection Logic |
|---|---|
| **Car-Following** | Same lane · gap < 150 ft · \|ego_v − lead_v\| < 15 ft/s · episode ≥ 5 s |
| **Lane Change** | `lane_id` transition with 10 stable frames before **and** after (noise filter) |
| **Overtaking** | Lane change + speed advantage ≥ 3 ft/s + ego passes target within 10 s |

---

## Output Schema

Each extracted sample contains:

| Field | Type | Description |
|---|---|---|
| `sample_id` | string | UUID |
| `scenario_type` | string | `car_following`, `lane_change`, or `overtaking` |
| `ego_vehicle_id` | int | Ego vehicle ID |
| `surrounding_vehicle_ids` | JSON list | IDs of vehicles within 300 ft |
| `start_time` | int | Window start (epoch ms) |
| `end_time` | int | Window end (epoch ms) |
| `ego_lane` | int | Primary lane during window |
| `average_speed` | float | Mean ego speed (ft/s) |
| `min_distance_to_lead` | float | Minimum space headway in window (ft) |
| `source_file` | string | Input filename |
| `vehicle_positions` | JSON dict | Ego + surrounding `(x, y)` traces |

---

## Results

| Scenario | Samples |
|---|---|
| Car-Following | 184 |
| Lane Change | 54 |
| Overtaking | 9 |
| **Total** | **247** |

---

## Quick Start (local)

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Run full pipeline — CSV output
python main.py --input data/trajectories-0750am-0805am.csv \
               --output_csv output/scenario_samples.csv

# 3. Debug run (first 100k rows)
python main.py --input data/trajectories-0750am-0805am.csv \
               --output_csv output/scenario_samples.csv \
               --nrows 100000

# 4. Visualise and validate results
python visualize.py --csv output/scenario_samples.csv \
                    --output_dir output/plots
```

Output plots written to `output/plots/`:
- `summary_counts.png` — bar chart of scenario counts
- `car_following_01_ego<id>.png` — trajectory plot
- `lane_change_01_ego<id>.png` — trajectory plot
- `overtaking_01_ego<id>.png` — trajectory plot

---

## Cloud Deployment (Google Compute Engine)

### Option A — Direct Python on VM

```bash
# On the GCE VM, install deps then run:
pip install -r requirements.txt

python main.py \
    --use_gcs \
    --gcs_bucket YOUR_BUCKET \
    --gcs_blob trajectories/us101.csv \
    --output_mode bigquery \
    --bq_project YOUR_PROJECT \
    --bq_dataset ngsim \
    --bq_table scenarios
```

Required VM service account roles:
- `roles/storage.objectViewer` on the GCS bucket
- `roles/bigquery.dataEditor` on the BigQuery dataset

### Option B — Docker on VM

```bash
# Build image
docker build -t ngsim-monolith .

# Run with local data
docker run \
    -v $(pwd)/data:/app/data \
    -v $(pwd)/output:/app/output \
    ngsim-monolith \
    python main.py \
        --input data/trajectories-0750am-0805am.csv \
        --output_csv output/scenario_samples.csv

# Run with GCS + BigQuery
docker run \
    -e GOOGLE_APPLICATION_CREDENTIALS=/app/key.json \
    -v /path/to/key.json:/app/key.json \
    ngsim-monolith \
    python main.py \
        --use_gcs --gcs_bucket my-bucket \
        --gcs_blob trajectories-0750am-0805am.csv \
        --output_mode bigquery \
        --bq_project my-project --bq_dataset ngsim --bq_table scenarios
```

---

## CLI Reference

| Argument | Default | Description |
|---|---|---|
| `--input` | `data/trajectories-0750am-0805am.csv` | Local CSV path |
| `--use_gcs` | false | Load from GCS instead of local |
| `--gcs_bucket` | — | GCS bucket name |
| `--gcs_blob` | — | GCS object path |
| `--nrows` | — | Limit rows read (debug mode) |
| `--output_csv` | `output/scenario_samples.csv` | Output CSV path |
| `--output_mode` | `csv` | `csv`, `bigquery`, or `both` |
| `--bq_project` | — | GCP project ID |
| `--bq_dataset` | — | BigQuery dataset name |
| `--bq_table` | — | BigQuery table name |
| `--source_file_name` | — | Override source label in output |

---

## Configuration (`config.py`)

All detection thresholds live in one place — no magic numbers in the pipeline:

| Parameter | Value | Meaning |
|---|---|---|
| `CF_MAX_DISTANCE_FT` | 150.0 ft | Max bumper-to-bumper gap for car-following |
| `CF_MAX_REL_SPEED_FT_S` | 15.0 ft/s | Max speed difference for car-following |
| `CF_MIN_DURATION_MS` | 5000 ms | Minimum car-following episode length |
| `LC_PRE_STABILITY_FRAMES` | 10 | Stable frames required before lane change |
| `LC_POST_STABILITY_FRAMES` | 10 | Stable frames required after lane change |
| `OT_MIN_SPEED_ADVANTAGE` | 3.0 ft/s | Speed advantage needed to count as overtaking |
| `OT_MAX_INITIAL_GAP_FT` | 200.0 ft | Max initial gap to target for overtaking |
| `OT_LOOKAHEAD_MS` | 10000 ms | Scan window to detect the pass |
| `SEGMENT_DURATION_MS` | 5000 ms | Output window length |
| `SURROUND_RADIUS_FT` | 300.0 ft | Radius for surrounding vehicle search |

##############################################################################
# Dockerfile – Phase 1 (Monolithic System)
# -----------------------------------------
# Containerises the Phase 1 monolithic pipeline for cloud deployment on
# Google Compute Engine or Cloud Run.
#
# Build & run locally
# --------------------
#   docker build -t ngsim-monolith .
#   docker run -v $(pwd)/data:/app/data -v $(pwd)/output:/app/output \
#              ngsim-monolith \
#              python main.py --input data/trajectories-0750am-0805am.csv \
#                             --output_csv output/scenario_samples.csv
#
# GCS source + BigQuery output
# ----------------------------
#   docker run -e GOOGLE_APPLICATION_CREDENTIALS=/app/key.json \
#              -v /path/to/key.json:/app/key.json \
#              ngsim-monolith \
#              python main.py --use_gcs \
#                --gcs_bucket my-bucket \
#                --gcs_blob trajectories-0750am-0805am.csv \
#                --output_mode bigquery \
#                --bq_project my-project \
#                --bq_dataset ngsim \
#                --bq_table scenarios
##############################################################################

FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY config.py           .
COPY data_loader.py      .
COPY preprocessing.py    .
COPY scenario_detection.py .
COPY segmentation.py     .
COPY storage.py          .
COPY visualize.py        .
COPY main.py             .

# Create directories for data and output
RUN mkdir -p /app/data /app/output/plots

# Default entrypoint — override CMD at runtime
ENTRYPOINT ["python", "main.py"]
CMD ["--help"]

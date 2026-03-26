FROM python:3.11-slim

WORKDIR /app

# Copy shared library first (leverages Docker layer caching)
COPY shared/ /app/shared/

# Install dependencies
COPY ingestion_service/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Copy service code
COPY ingestion_service/app.py /app/app.py

# Set Python path so 'shared' package is importable
ENV PYTHONPATH=/app

EXPOSE 8001

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8001"]

FROM python:3.11-slim

WORKDIR /app

COPY shared/ /app/shared/

COPY processing_service/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY processing_service/app.py /app/app.py

ENV PYTHONPATH=/app

EXPOSE 8002

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8002"]

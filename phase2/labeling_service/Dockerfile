FROM python:3.11-slim

WORKDIR /app

COPY labeling_service/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

COPY labeling_service/app.py /app/app.py

EXPOSE 8003

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8003"]

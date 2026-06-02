FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY app.py anomaly_explanations.py incident_commands.py pipeline_status.py rate_limit.py ./
COPY static ./static

CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8000"]

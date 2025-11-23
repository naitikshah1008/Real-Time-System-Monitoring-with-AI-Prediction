FROM python:3.11-slim

WORKDIR /app

# Install system deps if needed (none for now)
RUN apt-get update && apt-get install -y --no-install-recommends \
    && rm -rf /var/lib/apt/lists/*

# Copy code + requirements
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY metrics_producer.py .

CMD ["python", "metrics_producer.py"]

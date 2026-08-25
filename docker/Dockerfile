FROM python:3.11-slim

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1
ENV PROJECT_ROOT=/app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY config ./config
COPY src ./src

RUN mkdir -p /app/data/raw

CMD ["python", "src/ingestion/ingestion.py"]
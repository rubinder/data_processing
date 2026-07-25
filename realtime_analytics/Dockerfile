# Producer / consumer / API image.
FROM python:3.10-slim

WORKDIR /app

COPY pyproject.toml ./
COPY realtime_analytics ./realtime_analytics

RUN pip install --no-cache-dir \
        "clickhouse-connect>=0.7.0" \
        "fastapi>=0.110.0" \
        "uvicorn[standard]>=0.27.0" \
        "kafka-python>=2.0.2" \
        "requests>=2.31.0"

ENV PYTHONUNBUFFERED=1 \
    PYTHONPATH=/app \
    ANALYTICS_BACKEND=http

EXPOSE 8500

CMD ["uvicorn", "realtime_analytics.api:app", "--host", "0.0.0.0", "--port", "8500"]

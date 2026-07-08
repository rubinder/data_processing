FROM python:3.10-slim

WORKDIR /app

# Install runtime dependencies (embedded DuckDB + FastAPI service).
RUN pip install --no-cache-dir \
    "duckdb>=1.0.0" \
    "fastapi>=0.110.0" \
    "uvicorn>=0.29.0" \
    "requests>=2.31.0"

COPY app/ /app/app/

ENV DUCKDB_PATH=/data/impressions.duckdb
ENV API_BASE_URL=http://localhost:8000

EXPOSE 8100

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8100"]

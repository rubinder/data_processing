FROM python:3.10-slim

WORKDIR /app

# Runtime dependencies. duckdb is only used by the benchmark's comparison
# variant; it is small enough to keep the image single-purpose.
RUN pip install --no-cache-dir \
    "polars>=1.44,<2" \
    "requests>=2.31.0" \
    "duckdb>=1.0.0" \
    "pyarrow>=15.0.0"

COPY polars_deployment/ /app/polars_deployment/
COPY benchmarks/ /app/benchmarks/

ENV POLARS_DATA_DIR=/data/impressions
ENV API_BASE_URL=http://localhost:8000

# Run-to-completion: every invocation is one CLI command.
ENTRYPOINT ["python", "-m", "polars_deployment.cli"]
CMD ["--help"]

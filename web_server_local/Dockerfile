FROM python:3.10-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv

WORKDIR /app

COPY pyproject.toml ./
COPY web_server_code ./web_server_code

RUN uv pip install --system .

EXPOSE 8000

CMD ["uvicorn", "web_server_code.main:app", "--host", "0.0.0.0", "--port", "8000"]

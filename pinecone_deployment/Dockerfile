FROM python:3.10-slim
WORKDIR /app
COPY pyproject.toml README.md ./
COPY pinecone_deployment ./pinecone_deployment
RUN pip install --no-cache-dir .
EXPOSE 8090
CMD ["uvicorn", "pinecone_deployment.api:app", "--host", "0.0.0.0", "--port", "8090"]

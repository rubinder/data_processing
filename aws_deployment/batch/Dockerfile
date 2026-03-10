FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY check_encoding.py .

ENTRYPOINT ["python", "check_encoding.py"]

# Dockerfile
FROM python:3.11-slim-buster # Or your preferred Python base image

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . . # Copy your application code

CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]

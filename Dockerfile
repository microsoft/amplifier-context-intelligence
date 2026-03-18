FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
COPY context_intelligence_server/ context_intelligence_server/

RUN pip install --no-cache-dir .

EXPOSE 8000

CMD ["gunicorn", "context_intelligence_server.main:app", "--worker-class", "uvicorn.workers.UvicornWorker", "--workers", "1", "--bind", "0.0.0.0:8000", "--timeout", "30", "--graceful-timeout", "10"]

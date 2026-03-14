FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
COPY context_intelligence_server/ context_intelligence_server/

RUN pip install --no-cache-dir .

EXPOSE 8000

CMD ["uvicorn", "context_intelligence_server.main:app", "--host", "0.0.0.0", "--port", "8000"]

FROM python:3.11-slim

WORKDIR /app

COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml .
COPY context_intelligence_server/ context_intelligence_server/
COPY docker-entrypoint.sh .
RUN chmod +x docker-entrypoint.sh

RUN uv pip install --system --no-cache .

EXPOSE 8000

ENTRYPOINT ["./docker-entrypoint.sh"]
CMD ["context-intelligence-server"]

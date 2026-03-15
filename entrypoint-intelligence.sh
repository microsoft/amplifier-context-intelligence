#!/bin/bash
set -e

mkdir -p /data

# Copy config files to /data without overwriting existing files
cp -rn /config/* /data/ 2>/dev/null || true

exec uvicorn intelligence_service.app:app --host 0.0.0.0 --port 8100

#!/bin/sh
set -e

mkdir -p /data

# Copy config files to /data without overwriting existing files
cp -rn /config/* /data/ 2>/dev/null || true

# Configure git to use GH_TOKEN for private GitHub repos (if available)
if [ -n "$GH_TOKEN" ]; then
    git config --global url."https://${GH_TOKEN}@github.com/".insteadOf "https://github.com/"
fi

# Execute the main command
exec "$@"

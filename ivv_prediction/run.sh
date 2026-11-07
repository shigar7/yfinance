#!/usr/bin/env bash
# Start the app on http://127.0.0.1:9003
cd "$(dirname "$0")"
exec .venv/bin/uvicorn app:app --host "${HOST:-127.0.0.1}" --port "${PORT:-9003}" "$@"

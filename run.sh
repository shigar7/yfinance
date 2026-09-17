#!/usr/bin/env bash
# Start the tracker on http://localhost:7777
cd "$(dirname "$0")"
exec .venv/bin/uvicorn app:app --host "${HOST:-0.0.0.0}" --port "${PORT:-7777}" "$@"

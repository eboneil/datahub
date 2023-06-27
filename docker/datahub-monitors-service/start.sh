#!/bin/bash

echo "Starting datahub monitors service..."
poetry run uvicorn datahub_monitors.app.server:app --host 0.0.0.0 --port 9004 ${EXTRA_UVICORN_ARGS:-}

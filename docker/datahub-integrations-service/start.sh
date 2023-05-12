#!/bin/bash

echo "Starting datahub integrations service..."
poetry run uvicorn datahub_integrations.server:app --host 0.0.0.0 --port 9003 ${EXTRA_UVICORN_ARGS:-}

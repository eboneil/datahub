#!/bin/bash

source .env

poetry run uvicorn datahub_monitors.app.server:app --reload --port 9004

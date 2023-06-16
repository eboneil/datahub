from datahub_monitors.app.app import app
from datahub_monitors.app.health import health_router
from datahub_monitors.app.logging import configure_logging
from datahub_monitors.app.monitors import start_async_monitors
from datahub_monitors.app.routes import internal_router

# Configure global logging.
configure_logging()

# Start the async monitors
start_async_monitors()

# from fastapi import HTTPException, status
# from pydantic import BaseModel
app.include_router(health_router)
app.include_router(internal_router, prefix="/assertions")

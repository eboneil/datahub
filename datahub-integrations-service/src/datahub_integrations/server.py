import os

from fastapi import HTTPException, status
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

from datahub_integrations.app import STATIC_ASSETS_DIR, app
from datahub_integrations.slack.slack import (
    SlackLinkPreview,
    external_router,
    get_slack_link_preview,
    internal_router,
    reload_slack_credentials,
)


@internal_router.post("/reload_credentials")
def reload_credentials() -> None:
    """Reloads all integration credentials from GMS."""

    reload_slack_credentials()


class GetLinkPreviewInput(BaseModel):
    type: str
    url: str


@internal_router.post("/get_link_preview", response_model=SlackLinkPreview)
def get_link_preview(input: GetLinkPreviewInput) -> SlackLinkPreview:
    """Get a link preview."""

    if input.type == "SLACK_MESSAGE":
        return get_slack_link_preview(input.url)
    else:
        raise HTTPException(
            status.HTTP_400_BAD_REQUEST, f"Unknown link type: {input.type}"
        )


app.include_router(internal_router, prefix="/private")

# Using .mount() on ApiRouter doesn't work. See https://github.com/tiangolo/fastapi/issues/1469.
# As such, we have to mount it directly on the app.
app.mount(
    "/public/static",
    StaticFiles(directory=STATIC_ASSETS_DIR),
    name="integrations-static-dir",
)
app.include_router(external_router, prefix="/public")

if os.environ.get("DEV_MODE_BYPASS_FRONTEND", False):
    # This is only for development purposes.
    # When running this directly instead of through the frontend server's proxy, we
    # need to mount the external router on /integrations because that's what the frontend
    # route is.
    app.include_router(external_router, prefix="/integrations")

import fastapi

internal_router = fastapi.APIRouter(
    dependencies=[
        # TODO: Add middleware for requiring system auth here.
    ]
)


@internal_router.post("/evaluate")
def evaluate_assertion() -> None:
    """Evaluates an assertion with a particular urn"""
    pass


if __name__ == "__main__":
    # For development only
    pass

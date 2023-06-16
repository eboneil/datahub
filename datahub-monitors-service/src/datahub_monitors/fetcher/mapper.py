import logging
from typing import Dict, List

from datahub_monitors.types import Monitor

logger = logging.getLogger(__name__)


def graphql_to_monitors(graphql_monitors: List[Dict]) -> List[Monitor]:
    logger.debug(f"Converting GraphQL monitors to Engine Monitors {graphql_monitors}")
    monitors = []
    for graphql_monitor in graphql_monitors:
        try:
            # Simply parse to our Pydantic models using the raw GraphQL Response.
            monitors.append(Monitor.parse_obj(graphql_monitor))
        except Exception:
            logger.exception(
                f"Failed to convert GraphQL Monitor object to Python object. {graphql_monitor}"
            )
    logger.debug(f"Finished converting GraphQL monitors to Engine monitors {monitors}")
    return monitors

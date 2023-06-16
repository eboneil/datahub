import logging
from typing import Any, Optional

from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.bigquery_v2.bigquery_config import BigQueryV2Config
from datahub.ingestion.source.bigquery_v2.common import get_bigquery_client

from datahub_monitors.connection.connection import Connection
from datahub_monitors.constants import BIGQUERY_PLATFORM_URN

logger = logging.getLogger(__name__)


class BigQueryConnection(Connection):
    """A connection to BigQuery"""

    def __init__(self, urn: str, config: BigQueryV2Config, graph: DataHubGraph):
        super().__init__(urn, BIGQUERY_PLATFORM_URN)
        self.config = config
        self.graph = graph
        self.connection: Optional[Any] = None

    def get_client(self) -> Any:
        if self.connection is None:
            self.connection = get_bigquery_client(self.config)
        return self.connection

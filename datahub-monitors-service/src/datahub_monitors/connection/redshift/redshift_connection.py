import logging
from typing import Any, Optional

import redshift_connector
from datahub.ingestion.graph.client import DataHubGraph
from datahub.ingestion.source.redshift.config import RedshiftConfig

from datahub_monitors.connection.connection import Connection
from datahub_monitors.constants import REDSHIFT_PLATFORM_URN

logger = logging.getLogger(__name__)


class RedshiftConnection(Connection):
    """A connection to Redshift"""

    def __init__(self, urn: str, config: RedshiftConfig, graph: DataHubGraph):
        super().__init__(urn, REDSHIFT_PLATFORM_URN)
        self.config = config
        self.graph = graph
        self.connection: Optional[Any] = None

    def get_client(self) -> Any:
        # TODO: Add try
        # TODO: Filter out unsupported auth types.
        if self.connection is None:
            client_options = self.config.extra_client_options
            host, port = self.config.host_port.split(":")
            self.connection = redshift_connector.connect(
                host=host,
                port=int(port),
                user=self.config.username,
                database=self.config.database,
                password=self.config.password.get_secret_value()
                if self.config.password
                else None,
                **client_options,
            )
        return self.connection

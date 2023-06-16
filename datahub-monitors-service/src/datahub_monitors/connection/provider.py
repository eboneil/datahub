from abc import ABC, abstractmethod
from typing import Optional

from datahub_monitors.connection.connection import Connection


class ConnectionProvider(ABC):
    """Base class for a provider of connection details."""

    @abstractmethod
    def get_connection(self, urn: str) -> Optional[Connection]:
        raise NotImplementedError()

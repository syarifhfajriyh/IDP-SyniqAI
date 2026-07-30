from abc import ABC, abstractmethod
from typing import Dict, Any, Iterator

class BaseConnector(ABC):
    source_type = "generic"
    connector_version = "v1.0.0"

    def __init__(self, connection_config: Dict[str, Any]):
        self.connection_config = connection_config

    @abstractmethod
    def connect(self) -> None:
        """Establish connection to the source."""
        pass

    @abstractmethod
    def validate_credentials(self) -> None:
        """Check if connection works."""
        pass

    @abstractmethod
    def extract(self, extraction_plan: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        """
        Yields standard payload:
        {
            "data": DataFrame (Chunk),
            "metadata": Dict
        }
        """
        pass

    @abstractmethod
    def close(self) -> None:
        """Clean up resources."""
        pass
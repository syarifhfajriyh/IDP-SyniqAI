"""
Base Connector - Abstract Interface for All Data Sources
=========================================================
Defines the contract that all connectors must implement.

All connectors must:
1. Accept config dict from ConfigLoader
2. Implement connect(), extract(), close()
3. Return standardized metadata format
4. Handle errors gracefully
"""

from abc import ABC, abstractmethod
from typing import Dict, Any, Iterator
import logging

logger = logging.getLogger(__name__)


class BaseConnector(ABC):
    """
    Abstract base class for all data source connectors.
    
    Attributes:
        source_type (str): Type of connector (postgres, mariadb, mongodb, etc.)
        connection_config (dict): Connection configuration from ConfigLoader
    """
    
    source_type: str = "base"  # Override in subclasses
    
    def __init__(self, connection_config: Dict[str, Any]):
        """
        Initialize connector with configuration.
        
        Args:
            connection_config: Configuration dict from ConfigLoader
                Example for PostgreSQL:
                {
                    "host": "localhost",
                    "port": 5432,
                    "database": "mydb",
                    "user": "user",
                    "password": "pass",
                    "pool_size": 5,
                    ...
                }
        """
        self.connection_config = connection_config
        self.engine = None
        
        logger.info(f"Initialized {self.__class__.__name__}")
    
    @abstractmethod
    def connect(self) -> None:
        """
        Establish connection to data source.
        
        Must:
        - Create connection/engine
        - Validate connectivity
        - Set up connection pooling
        - Handle SSL/TLS if required
        
        Raises:
            ConnectionError: If connection fails
        """
        pass
    
    @abstractmethod
    def validate_credentials(self) -> None:
        """
        Validate that connector has required permissions.
        
        Must check:
        - Can connect to database
        - Can list tables/collections
        - Has SELECT/READ permissions
        
        Raises:
            PermissionError: If credentials are invalid or insufficient
        """
        pass
    
    @abstractmethod
    def extract(self, extraction_plan: Dict[str, Any]) -> Iterator[Dict[str, Any]]:
        """
        Extract data from source based on extraction plan.
        
        Args:
            extraction_plan: Extraction configuration
                Required fields:
                - entity: Table/collection name
                - mode: "full" or "incremental"
                
                Optional fields:
                - chunk_size: Rows per chunk
                - partition_column: Column for parallel extraction
                - num_workers: Number of parallel workers
                - watermark_column: Column for incremental (if mode=incremental)
                - initial_value: Starting value for incremental
        
        Yields:
            Dict containing:
            {
                "data": pd.DataFrame,  # Extracted data chunk
                "metadata": {
                    "source_type": str,
                    "entity": str,
                    "chunk_index": int,
                    "row_count": int,
                    "schema_snapshot": List[Dict],
                    "extracted_at": str (ISO timestamp)
                }
            }
        
        Example:
            for batch in connector.extract({"entity": "customers", "mode": "full"}):
                df = batch["data"]
                meta = batch["metadata"]
                print(f"Got {len(df)} rows from {meta['entity']}")
        """
        pass
    
    @abstractmethod
    def close(self) -> None:
        """
        Clean up connection resources.
        
        Must:
        - Close connection pools
        - Release file handles
        - Clean up temporary resources
        """
        pass
    
    def __enter__(self):
        """Context manager entry - calls connect()"""
        self.connect()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit - calls close()"""
        self.close()
        return False
    
    def get_connection_string(self) -> str:
        """
        Generate connection string for logging (with password masked).
        
        Returns:
            str: Safe connection string for logging
        """
        cfg = self.connection_config
        password_masked = "***" if cfg.get("password") else "None"
        
        return (
            f"{self.source_type}://{cfg.get('user', 'unknown')}:{password_masked}"
            f"@{cfg.get('host', 'unknown')}:{cfg.get('port', 'unknown')}"
            f"/{cfg.get('database', 'unknown')}"
        )
    
    def __repr__(self):
        return f"<{self.__class__.__name__}({self.get_connection_string()})>"
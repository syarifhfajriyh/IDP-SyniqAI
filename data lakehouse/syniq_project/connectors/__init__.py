"""
Database and data source connectors for Syniq Lakehouse.

Supported connectors:
- PostgreSQL
- MariaDB (on-premise)
- MariaDB Cloud (SkySQL)
- MongoDB
- AWS S3

Usage:
    from connectors import PostgresConnector
    connector = PostgresConnector(config)
    data = connector.extract("customers")
"""

from .base_connector import BaseConnector

# Import specific connectors as they're built
from .postgres_connector import PostgresConnector
from .mariadb_connector import MariaDBConnector
from .mariadb_cloud_connector import MariaDBCloudConnector
from .mongodb_connector import MongoDBConnector
# from .s3_connector import S3Connector
# from .connector_factory import ConnectorFactory

__all__ = [
    "BaseConnector",
    "PostgresConnector",
    "MariaDBConnector",
    "MariaDBCloudConnector",
    "MongoDBConnector",
    # "S3Connector",
    # "ConnectorFactory",
]

__version__ = "0.1.0"

# Optional: Connector registry for auto-discovery
CONNECTOR_REGISTRY = {
    "postgres": PostgresConnector,
    "mariadb": MariaDBConnector,
    "mariadb_cloud": MariaDBCloudConnector,
    "mongodb": MongoDBConnector,
    # "s3": S3Connector,
}


def get_connector(connector_type: str):
    """
    Get connector class by type.
    
    Args:
        connector_type: Type of connector (e.g., 'postgres', 'mariadb')
        
    Returns:
        Connector class
        
    Raises:
        ValueError: If connector type not found
    """
    if connector_type not in CONNECTOR_REGISTRY:
        raise ValueError(
            f"Unknown connector type: {connector_type}. "
            f"Available: {list(CONNECTOR_REGISTRY.keys())}"
        )
    return CONNECTOR_REGISTRY[connector_type]
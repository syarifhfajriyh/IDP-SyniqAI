"""
Bronze Table Initialization Script
===================================
Creates Bronze Iceberg tables for all data domains:
- Finance (transactions, accounts, credit_cards)
- Healthcare (patients, appointments, prescriptions)
- Retail (products, orders, customers)

This script:
1. Connects to Iceberg catalog (PostgreSQL)
2. Creates Bronze schemas if they don't exist
3. Defines table schemas for all domains
4. Creates Iceberg tables with proper partitioning
5. Sets up table properties for CDC optimization

Usage:
    # Initialize all domains
    python bronze/initialize_bronze_tables.py
    
    # Initialize specific domain
    python bronze/initialize_bronze_tables.py --domain finance
    
    # Dry run (show what would be created)
    python bronze/initialize_bronze_tables.py --dry-run

Requirements:
    - PostgreSQL running (Iceberg catalog)
    - MinIO running (Iceberg warehouse)
    - pyiceberg[pyarrow,sql-postgres] installed
"""

import sys
import argparse
import logging
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

# Add parent directory to path
sys.path.append(str(Path(__file__).parent.parent))

from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.schema import Schema
from pyiceberg.types import (
    NestedField, StringType, IntegerType, LongType, 
    DoubleType, TimestampType, BooleanType, DecimalType,
    DateType
)
from pyiceberg.partitioning import PartitionSpec, PartitionField
from pyiceberg.transforms import DayTransform, IdentityTransform

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================================================
# Domain Schemas
# ============================================================================

def get_finance_schemas() -> Dict[str, Schema]:
    """Define schemas for Finance domain tables."""
    return {
        "transactions": Schema(
            NestedField(1, "transaction_id", StringType(), required=True),
            NestedField(2, "user_id", StringType(), required=True),
            NestedField(3, "transaction_date", TimestampType(), required=True),
            NestedField(4, "amount", DecimalType(15, 2), required=True),
            NestedField(5, "currency", StringType(), required=False),
            NestedField(6, "merchant_name", StringType(), required=False),
            NestedField(7, "merchant_category", StringType(), required=False),
            NestedField(8, "card_type", StringType(), required=False),
            NestedField(9, "transaction_type", StringType(), required=False),
            NestedField(10, "status", StringType(), required=False),
            NestedField(11, "is_fraud", BooleanType(), required=False),
            # CDC metadata
            NestedField(20, "__op", StringType(), required=False),
            NestedField(21, "__source_ts_ms", LongType(), required=False),
            NestedField(22, "__source_db", StringType(), required=False),
            NestedField(23, "__source_table", StringType(), required=False),
            NestedField(24, "ingestion_timestamp", TimestampType(), required=True),
        ),
        
        "accounts": Schema(
            NestedField(1, "account_id", StringType(), required=True),
            NestedField(2, "user_id", StringType(), required=True),
            NestedField(3, "account_type", StringType(), required=True),
            NestedField(4, "balance", DecimalType(15, 2), required=True),
            NestedField(5, "currency", StringType(), required=False),
            NestedField(6, "opened_date", DateType(), required=False),
            NestedField(7, "status", StringType(), required=False),
            NestedField(8, "branch_code", StringType(), required=False),
            # CDC metadata
            NestedField(20, "__op", StringType(), required=False),
            NestedField(21, "__source_ts_ms", LongType(), required=False),
            NestedField(22, "__source_db", StringType(), required=False),
            NestedField(23, "__source_table", StringType(), required=False),
            NestedField(24, "ingestion_timestamp", TimestampType(), required=True),
        ),
        
        "credit_cards": Schema(
            NestedField(1, "card_id", StringType(), required=True),
            NestedField(2, "user_id", StringType(), required=True),
            NestedField(3, "card_number_masked", StringType(), required=True),
            NestedField(4, "card_type", StringType(), required=False),
            NestedField(5, "expiry_date", StringType(), required=False),
            NestedField(6, "credit_limit", DecimalType(15, 2), required=False),
            NestedField(7, "current_balance", DecimalType(15, 2), required=False),
            NestedField(8, "status", StringType(), required=False),
            # CDC metadata
            NestedField(20, "__op", StringType(), required=False),
            NestedField(21, "__source_ts_ms", LongType(), required=False),
            NestedField(22, "__source_db", StringType(), required=False),
            NestedField(23, "__source_table", StringType(), required=False),
            NestedField(24, "ingestion_timestamp", TimestampType(), required=True),
        ),
    }


def get_healthcare_schemas() -> Dict[str, Schema]:
    """Define schemas for Healthcare domain tables."""
    return {
        "patients": Schema(
            NestedField(1, "patient_id", StringType(), required=True),
            NestedField(2, "first_name", StringType(), required=True),
            NestedField(3, "last_name", StringType(), required=True),
            NestedField(4, "date_of_birth", DateType(), required=False),
            NestedField(5, "gender", StringType(), required=False),
            NestedField(6, "phone", StringType(), required=False),
            NestedField(7, "email", StringType(), required=False),
            NestedField(8, "address", StringType(), required=False),
            NestedField(9, "insurance_id", StringType(), required=False),
            NestedField(10, "blood_type", StringType(), required=False),
            # CDC metadata
            NestedField(20, "__op", StringType(), required=False),
            NestedField(21, "__source_ts_ms", LongType(), required=False),
            NestedField(22, "__source_db", StringType(), required=False),
            NestedField(23, "__source_table", StringType(), required=False),
            NestedField(24, "ingestion_timestamp", TimestampType(), required=True),
        ),
        
        "appointments": Schema(
            NestedField(1, "appointment_id", StringType(), required=True),
            NestedField(2, "patient_id", StringType(), required=True),
            NestedField(3, "doctor_id", StringType(), required=False),
            NestedField(4, "appointment_date", TimestampType(), required=True),
            NestedField(5, "department", StringType(), required=False),
            NestedField(6, "status", StringType(), required=False),
            NestedField(7, "notes", StringType(), required=False),
            # CDC metadata
            NestedField(20, "__op", StringType(), required=False),
            NestedField(21, "__source_ts_ms", LongType(), required=False),
            NestedField(22, "__source_db", StringType(), required=False),
            NestedField(23, "__source_table", StringType(), required=False),
            NestedField(24, "ingestion_timestamp", TimestampType(), required=True),
        ),
        
        "prescriptions": Schema(
            NestedField(1, "prescription_id", StringType(), required=True),
            NestedField(2, "patient_id", StringType(), required=True),
            NestedField(3, "doctor_id", StringType(), required=False),
            NestedField(4, "medication_name", StringType(), required=True),
            NestedField(5, "dosage", StringType(), required=False),
            NestedField(6, "frequency", StringType(), required=False),
            NestedField(7, "start_date", DateType(), required=False),
            NestedField(8, "end_date", DateType(), required=False),
            NestedField(9, "status", StringType(), required=False),
            # CDC metadata
            NestedField(20, "__op", StringType(), required=False),
            NestedField(21, "__source_ts_ms", LongType(), required=False),
            NestedField(22, "__source_db", StringType(), required=False),
            NestedField(23, "__source_table", StringType(), required=False),
            NestedField(24, "ingestion_timestamp", TimestampType(), required=True),
        ),
    }


def get_retail_schemas() -> Dict[str, Schema]:
    """Define schemas for Retail domain tables."""
    return {
        "products": Schema(
            NestedField(1, "product_id", StringType(), required=True),
            NestedField(2, "product_name", StringType(), required=True),
            NestedField(3, "category", StringType(), required=False),
            NestedField(4, "subcategory", StringType(), required=False),
            NestedField(5, "brand", StringType(), required=False),
            NestedField(6, "price", DecimalType(10, 2), required=True),
            NestedField(7, "cost", DecimalType(10, 2), required=False),
            NestedField(8, "stock_quantity", IntegerType(), required=False),
            NestedField(9, "sku", StringType(), required=False),
            NestedField(10, "status", StringType(), required=False),
            # CDC metadata
            NestedField(20, "__op", StringType(), required=False),
            NestedField(21, "__source_ts_ms", LongType(), required=False),
            NestedField(22, "__source_db", StringType(), required=False),
            NestedField(23, "__source_table", StringType(), required=False),
            NestedField(24, "ingestion_timestamp", TimestampType(), required=True),
        ),
        
        "orders": Schema(
            NestedField(1, "order_id", StringType(), required=True),
            NestedField(2, "customer_id", StringType(), required=True),
            NestedField(3, "order_date", TimestampType(), required=True),
            NestedField(4, "total_amount", DecimalType(10, 2), required=True),
            NestedField(5, "discount_amount", DecimalType(10, 2), required=False),
            NestedField(6, "tax_amount", DecimalType(10, 2), required=False),
            NestedField(7, "shipping_address", StringType(), required=False),
            NestedField(8, "status", StringType(), required=False),
            NestedField(9, "payment_method", StringType(), required=False),
            # CDC metadata
            NestedField(20, "__op", StringType(), required=False),
            NestedField(21, "__source_ts_ms", LongType(), required=False),
            NestedField(22, "__source_db", StringType(), required=False),
            NestedField(23, "__source_table", StringType(), required=False),
            NestedField(24, "ingestion_timestamp", TimestampType(), required=True),
        ),
        
        "customers": Schema(
            NestedField(1, "customer_id", StringType(), required=True),
            NestedField(2, "first_name", StringType(), required=True),
            NestedField(3, "last_name", StringType(), required=True),
            NestedField(4, "email", StringType(), required=False),
            NestedField(5, "phone", StringType(), required=False),
            NestedField(6, "address", StringType(), required=False),
            NestedField(7, "city", StringType(), required=False),
            NestedField(8, "country", StringType(), required=False),
            NestedField(9, "signup_date", DateType(), required=False),
            NestedField(10, "loyalty_points", IntegerType(), required=False),
            # CDC metadata
            NestedField(20, "__op", StringType(), required=False),
            NestedField(21, "__source_ts_ms", LongType(), required=False),
            NestedField(22, "__source_db", StringType(), required=False),
            NestedField(23, "__source_table", StringType(), required=False),
            NestedField(24, "ingestion_timestamp", TimestampType(), required=True),
        ),
    }


# ============================================================================
# Catalog and Table Creation
# ============================================================================

class BronzeTableInitializer:
    """Initialize Bronze Iceberg tables."""
    
    def __init__(
        self,
        catalog_uri: str = "postgresql://syniqai_user:syniqai_pass@localhost:5432/syniqai_metadata",
        warehouse: str = "s3a://bronze/iceberg",
        dry_run: bool = False
    ):
        """
        Initialize Bronze table creator.
        
        Args:
            catalog_uri: PostgreSQL catalog connection URI
            warehouse: S3 warehouse path
            dry_run: If True, only show what would be created
        """
        self.catalog_uri = catalog_uri
        self.warehouse = warehouse
        self.dry_run = dry_run
        self.catalog = None
        
        logger.info("BronzeTableInitializer initialized")
        logger.info(f"  Catalog: {catalog_uri}")
        logger.info(f"  Warehouse: {warehouse}")
        logger.info(f"  Dry Run: {dry_run}")
    
    
    def connect_catalog(self) -> bool:
        """Connect to Iceberg catalog."""
        try:
            logger.info("Connecting to Iceberg catalog...")
            
            self.catalog = SqlCatalog(
                "syniq_iceberg",
                **{
                    "uri": self.catalog_uri,
                    "warehouse": self.warehouse,
                    "s3.endpoint": "http://localhost:9000",
                    "s3.access-key-id": "minioadmin",
                    "s3.secret-access-key": "minioadmin",
                    "s3.path-style-access": "true",
                }
            )
            
            logger.info("✅ Connected to Iceberg catalog successfully")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to connect to catalog: {e}")
            return False
    
    
    def create_namespace(self, namespace: str) -> bool:
        """
        Create namespace (schema) if it doesn't exist.
        
        Args:
            namespace: Namespace name (e.g., 'bronze.postgres')
        """
        try:
            if self.dry_run:
                logger.info(f"[DRY RUN] Would create namespace: {namespace}")
                return True
            
            # Check if namespace exists
            namespaces = self.catalog.list_namespaces()
            namespace_tuple = tuple(namespace.split('.'))
            
            if namespace_tuple in namespaces:
                logger.info(f"Namespace already exists: {namespace}")
                return True
            
            # Create namespace
            self.catalog.create_namespace(namespace_tuple)
            logger.info(f"✅ Created namespace: {namespace}")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create namespace {namespace}: {e}")
            return False
    
    
    def create_table(
        self,
        namespace: str,
        table_name: str,
        schema: Schema,
        partition_by: Optional[str] = None
    ) -> bool:
        """
        Create Iceberg table.
        
        Args:
            namespace: Namespace name
            table_name: Table name
            schema: Table schema
            partition_by: Optional partition field (e.g., 'ingestion_timestamp')
        """
        try:
            full_table_name = f"{namespace}.{table_name}"
            
            if self.dry_run:
                logger.info(f"[DRY RUN] Would create table: {full_table_name}")
                logger.info(f"  Columns: {len(schema.fields)}")
                if partition_by:
                    logger.info(f"  Partitioned by: {partition_by}")
                return True
            
            # Check if table exists
            namespace_tuple = tuple(namespace.split('.'))
            tables = self.catalog.list_tables(namespace_tuple)
            
            if (namespace_tuple[0], namespace_tuple[1], table_name) in tables:
                logger.info(f"Table already exists: {full_table_name}")
                return True
            
            # Create partition spec if specified
            partition_spec = None
            if partition_by:
                # Partition by day for timestamp fields
                partition_spec = PartitionSpec(
                    PartitionField(
                        source_id=schema.find_field(partition_by).field_id,
                        field_id=1000,
                        transform=DayTransform(),
                        name=f"{partition_by}_day"
                    )
                )
            
            # Create table
            table = self.catalog.create_table(
                identifier=f"{namespace}.{table_name}",
                schema=schema,
                partition_spec=partition_spec,
                properties={
                    "write.format.default": "parquet",
                    "write.parquet.compression-codec": "snappy",
                    "commit.retry.num-retries": "3",
                    "write.metadata.delete-after-commit.enabled": "true",
                }
            )
            
            logger.info(f"✅ Created table: {full_table_name}")
            if partition_by:
                logger.info(f"   Partitioned by: {partition_by} (day)")
            return True
            
        except Exception as e:
            logger.error(f"❌ Failed to create table {full_table_name}: {e}")
            return False
    
    
    def initialize_domain(self, domain: str, source: str = "postgres") -> bool:
        """
        Initialize all tables for a domain.
        
        Args:
            domain: Domain name (finance, healthcare, retail)
            source: Source database (postgres, mariadb)
        """
        logger.info("=" * 70)
        logger.info(f" Initializing {domain.upper()} domain (source: {source})")
        logger.info("=" * 70)
        
        # Get schema definitions
        schema_funcs = {
            "finance": get_finance_schemas,
            "healthcare": get_healthcare_schemas,
            "retail": get_retail_schemas,
        }
        
        if domain not in schema_funcs:
            logger.error(f"Unknown domain: {domain}")
            return False
        
        schemas = schema_funcs[domain]()
        
        # Create namespace
        namespace = f"bronze.{source}"
        if not self.create_namespace(namespace):
            return False
        
        # Create tables
        success_count = 0
        for table_name, schema in schemas.items():
            if self.create_table(
                namespace=namespace,
                table_name=table_name,
                schema=schema,
                partition_by="ingestion_timestamp"
            ):
                success_count += 1
        
        logger.info(f"✅ Created {success_count}/{len(schemas)} tables for {domain}")
        return success_count == len(schemas)
    
    
    def initialize_all_domains(self, source: str = "postgres") -> bool:
        """Initialize all domains."""
        logger.info("=" * 70)
        logger.info(" INITIALIZING ALL BRONZE TABLES")
        logger.info("=" * 70)
        
        domains = ["finance", "healthcare", "retail"]
        
        for domain in domains:
            if not self.initialize_domain(domain, source):
                logger.warning(f"Failed to initialize {domain} domain")
        
        logger.info("=" * 70)
        logger.info(" ✅ BRONZE TABLE INITIALIZATION COMPLETE")
        logger.info("=" * 70)
        return True


# ============================================================================
# Main
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Initialize Bronze Iceberg tables for all domains"
    )
    parser.add_argument(
        "--domain",
        choices=["finance", "healthcare", "retail", "all"],
        default="all",
        help="Domain to initialize (default: all)"
    )
    parser.add_argument(
        "--source",
        choices=["postgres", "mariadb"],
        default="postgres",
        help="Source database type (default: postgres)"
    )
    parser.add_argument(
        "--catalog-uri",
        default="postgresql://syniqai_user:syniqai_pass@localhost:5432/syniqai_metadata",
        help="PostgreSQL catalog URI"
    )
    parser.add_argument(
        "--warehouse",
        default="s3a://bronze/iceberg",
        help="Iceberg warehouse path"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be created without actually creating"
    )
    
    args = parser.parse_args()
    
    # Create initializer
    initializer = BronzeTableInitializer(
        catalog_uri=args.catalog_uri,
        warehouse=args.warehouse,
        dry_run=args.dry_run
    )
    
    # Connect to catalog
    if not initializer.connect_catalog():
        logger.error("Failed to connect to catalog. Exiting.")
        return 1
    
    # Initialize tables
    try:
        if args.domain == "all":
            initializer.initialize_all_domains(source=args.source)
        else:
            initializer.initialize_domain(domain=args.domain, source=args.source)
        
        return 0
        
    except Exception as e:
        logger.error(f"Initialization failed: {e}")
        import traceback
        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())

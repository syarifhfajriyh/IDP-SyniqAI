"""
Ingestion Usage Examples
========================
Demonstrates how to use the Bronze Ingestor and Ingestion Orchestrator.
"""

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import pandas as pd
from datetime import datetime

from utils import get_logger, load_config, DataValidator, ValidationSeverity
from connectors import PostgresConnector
from ingestion import (
    BronzeIngestor,
    IngestionOrchestrator,
    run_ingestion,
    create_bronze_validator
)

logger = get_logger(__name__)


# ============================================================================
# EXAMPLE 1: Direct Bronze Ingestion (Single Batch)
# ============================================================================

def example_1_direct_bronze_ingestion():
    """Example 1: Directly ingest a DataFrame to Bronze zone"""
    print("\n" + "="*60)
    print("EXAMPLE 1: Direct Bronze Ingestion")
    print("="*60)
    
    # Sample data
    df = pd.DataFrame({
        "customer_id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "email": ["alice@email.com", "bob@email.com", "charlie@email.com", "diana@email.com", "eve@email.com"],
        "balance": [1000.00, 2500.50, 750.25, 3200.00, 1800.75],
        "is_active": [True, True, False, True, True],
        "signup_date": pd.to_datetime(["2024-01-01", "2024-01-15", "2024-02-01", "2024-02-15", "2024-03-01"])
    })
    
    print(f"Sample data: {len(df)} rows")
    print(df.head())
    
    # Load MinIO config
    minio_config = load_config("config/minio_config.yaml")
    
    # Initialize Bronze Ingestor
    ingestor = BronzeIngestor(
        minio_config=minio_config,
        bronze_bucket="bronze",
        enable_validation=True,
        enable_partitioning=True
    )
    
    # Ingest batch
    result = ingestor.ingest_batch(
        df=df,
        source_system="demo_system",
        source_entity="customers",
        ingestion_timestamp=datetime.utcnow()
    )
    
    print(f"\n✅ Ingestion result:")
    print(f"   Success: {result['success']}")
    print(f"   Rows written: {result['rows_written']}")
    print(f"   File path: {result['file_path']}")
    print(f"   File size: {result['file_size_bytes'] / 1024:.2f} KB")


# ============================================================================
# EXAMPLE 2: Bronze Ingestion with Data Validation
# ============================================================================

def example_2_ingestion_with_validation():
    """Example 2: Ingest with data quality validation"""
    print("\n" + "="*60)
    print("EXAMPLE 2: Bronze Ingestion with Validation")
    print("="*60)
    
    # Sample data with some quality issues
    df = pd.DataFrame({
        "customer_id": [1, 2, None, 4, 5],  # One null ID
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "age": [25, 30, 150, 40, 45],  # Age 150 is outlier
        "email": ["alice@email.com", "invalid-email", "charlie@email.com", "diana@email.com", "eve@email.com"]
    })
    
    print(f"Sample data: {len(df)} rows (with quality issues)")
    
    # Create validator with rules
    validator = DataValidator(entity="customers")
    validator.add_not_null_rule(
        columns=["customer_id", "name"],
        threshold=0.0,  # 0% nulls allowed
        severity=ValidationSeverity.ERROR
    )
    validator.add_range_rule(
        column="age",
        min_value=0,
        max_value=120,
        severity=ValidationSeverity.WARNING
    )
    validator.add_pattern_rule(
        column="email",
        pattern=r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
        description="Valid email format",
        severity=ValidationSeverity.WARNING
    )
    
    print(f"\n📋 Validation rules configured: {len(validator.rules)}")
    
    # Load MinIO config
    minio_config = load_config("config/minio_config.yaml")
    
    # Initialize Bronze Ingestor
    ingestor = BronzeIngestor(
        minio_config=minio_config,
        bronze_bucket="bronze",
        enable_validation=True
    )
    
    # Ingest with validation
    result = ingestor.ingest_batch(
        df=df,
        source_system="demo_system",
        source_entity="customers_validated",
        validator=validator
    )
    
    if result["success"]:
        print(f"\n✅ Validation passed and data ingested")
    else:
        print(f"\n❌ Validation failed:")
        if "validation_report" in result:
            report = result["validation_report"]
            print(f"   Error: {result['error']}")


# ============================================================================
# EXAMPLE 3: Connector-Based Ingestion
# ============================================================================

def example_3_connector_ingestion():
    """Example 3: Ingest data from a connector"""
    print("\n" + "="*60)
    print("EXAMPLE 3: Connector-Based Ingestion")
    print("="*60)
    
    try:
        # Load configs
        db_config = load_config("config/db_config.yaml")
        minio_config = load_config("config/minio_config.yaml")
        
        # Initialize connector
        connector = PostgresConnector(db_config["postgresql"])
        connector.connect()
        connector.validate_credentials()
        
        print(f"✅ Connected to PostgreSQL")
        
        # Initialize Bronze Ingestor
        ingestor = BronzeIngestor(
            minio_config=minio_config,
            bronze_bucket="bronze"
        )
        
        # Define extraction plan
        extraction_plan = {
            "source_system": "postgres_prod",
            "entity": "customers",
            "mode": "full",
            "batch_size": 1000
        }
        
        # Ingest from connector
        result = ingestor.ingest_from_connector(
            connector=connector,
            extraction_plan=extraction_plan
        )
        
        print(f"\n✅ Connector ingestion complete:")
        print(f"   Total rows: {result['total_rows']:,}")
        print(f"   Total files: {result['total_files']}")
        print(f"   Total size: {result['total_size_bytes'] / (1024*1024):.2f} MB")
        print(f"   Elapsed: {result['elapsed_seconds']:.2f}s")
        print(f"   Failed batches: {result['failed_batches']}")
        
        connector.close()
        
    except FileNotFoundError:
        print("⚠️  Config files not found (expected for demo)")
    except Exception as e:
        print(f"⚠️  Error: {e} (expected if database not configured)")


# ============================================================================
# EXAMPLE 4: Orchestrated Ingestion (Configuration-Driven)
# ============================================================================

def example_4_orchestrated_ingestion():
    """Example 4: Run orchestrated ingestion from configuration"""
    print("\n" + "="*60)
    print("EXAMPLE 4: Orchestrated Ingestion")
    print("="*60)
    
    try:
        # Run orchestrated ingestion for all configured sources
        report = run_ingestion(
            save_report=True
        )
        
        print(f"\n✅ Orchestration complete:")
        print(f"   Total jobs: {report['total_jobs']}")
        print(f"   Successful: {report['successful_jobs']}")
        print(f"   Failed: {report['failed_jobs']}")
        print(f"   Total rows: {report['total_rows']:,}")
        print(f"   Total files: {report['total_files']}")
        print(f"   Elapsed: {report['elapsed_seconds']:.2f}s")
        
    except FileNotFoundError:
        print("⚠️  Config files not found (expected for demo)")
    except Exception as e:
        print(f"⚠️  Error: {e} (expected if sources not configured)")


# ============================================================================
# EXAMPLE 5: Filtered Orchestration (Specific Sources/Entities)
# ============================================================================

def example_5_filtered_orchestration():
    """Example 5: Run orchestration with filters"""
    print("\n" + "="*60)
    print("EXAMPLE 5: Filtered Orchestration")
    print("="*60)
    
    try:
        # Run only for specific source
        report = run_ingestion(
            sources=["postgres_prod"],
            save_report=True
        )
        
        print(f"\n✅ Filtered orchestration (postgres_prod) complete:")
        print(f"   Jobs executed: {report['total_jobs']}")
        print(f"   Successful: {report['successful_jobs']}")
        
        # Run only for specific entities
        report2 = run_ingestion(
            entities=["customers", "policies"],
            save_report=True
        )
        
        print(f"\n✅ Filtered orchestration (customers, policies) complete:")
        print(f"   Jobs executed: {report2['total_jobs']}")
        
    except FileNotFoundError:
        print("⚠️  Config files not found (expected for demo)")
    except Exception as e:
        print(f"⚠️  Error: {e} (expected if sources not configured)")


# ============================================================================
# EXAMPLE 6: Custom IngestionOrchestrator
# ============================================================================

def example_6_custom_orchestrator():
    """Example 6: Use IngestionOrchestrator directly with custom logic"""
    print("\n" + "="*60)
    print("EXAMPLE 6: Custom Orchestrator")
    print("="*60)
    
    try:
        # Initialize orchestrator
        orchestrator = IngestionOrchestrator(
            db_config_path="config/db_config.yaml",
            ingestion_config_path="config/ingestion_config.yaml",
            minio_config_path="config/minio_config.yaml",
            bronze_bucket="bronze"
        )
        
        print(f"✅ Orchestrator initialized")
        
        # Create jobs from configuration
        jobs = orchestrator.create_ingestion_jobs()
        
        print(f"✅ Created {len(jobs)} ingestion jobs:")
        for job in jobs:
            print(f"   - {job.source_system}.{job.source_entity} ({job.connector_type})")
        
        # Run orchestration with filters
        report = orchestrator.run(
            source_filter=None,  # All sources
            entity_filter=None   # All entities
        )
        
        print(f"\n✅ Orchestration report:")
        print(f"   Total jobs: {report['total_jobs']}")
        print(f"   Successful: {report['successful_jobs']}")
        print(f"   Failed: {report['failed_jobs']}")
        
        # Save report
        orchestrator.save_report(report, "custom_ingestion_report.json")
        
    except FileNotFoundError:
        print("⚠️  Config files not found (expected for demo)")
    except Exception as e:
        print(f"⚠️  Error: {e} (expected if sources not configured)")


# ============================================================================
# EXAMPLE 7: Create Validator from Config
# ============================================================================

def example_7_validator_from_config():
    """Example 7: Create validator from configuration"""
    print("\n" + "="*60)
    print("EXAMPLE 7: Validator from Configuration")
    print("="*60)
    
    # Define validation rules in configuration format
    validation_rules = [
        {
            "type": "not_null",
            "columns": ["customer_id", "name"],
            "threshold": 0.0,
            "severity": "ERROR"
        },
        {
            "type": "range",
            "column": "age",
            "min": 0,
            "max": 120,
            "severity": "WARNING"
        },
        {
            "type": "pattern",
            "column": "email",
            "pattern": r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
            "description": "Valid email format",
            "severity": "WARNING"
        },
        {
            "type": "unique",
            "columns": ["customer_id"],
            "severity": "ERROR"
        }
    ]
    
    # Create validator from config
    validator = create_bronze_validator(
        entity="customers",
        validation_rules=validation_rules
    )
    
    print(f"✅ Validator created with {len(validation_rules)} rules:")
    for rule in validation_rules:
        print(f"   - {rule['type']}: {rule.get('columns', rule.get('column'))}")
    
    # Test with sample data
    df = pd.DataFrame({
        "customer_id": [1, 2, 3, 4, 5],
        "name": ["Alice", "Bob", "Charlie", "Diana", "Eve"],
        "age": [25, 30, 35, 40, 45],
        "email": ["alice@a.com", "bob@b.com", "charlie@c.com", "diana@d.com", "eve@e.com"]
    })
    
    report = validator.validate(df)
    
    print(f"\n✅ Validation complete:")
    print(f"   Total rows: {report.total_rows}")
    print(f"   Rules passed: {len([r for r in report.results if r.passed])}/{len(report.results)}")
    print(f"   Has errors: {report.has_errors()}")


# ============================================================================
# MAIN
# ============================================================================

if __name__ == "__main__":
    print("\n" + "🚀"*30)
    print("INGESTION USAGE EXAMPLES")
    print("🚀"*30)
    
    # Run examples (comment/uncomment as needed)
    try:
        example_1_direct_bronze_ingestion()
        # example_2_ingestion_with_validation()
        # example_3_connector_ingestion()
        # example_4_orchestrated_ingestion()
        # example_5_filtered_orchestration()
        # example_6_custom_orchestrator()
        # example_7_validator_from_config()
        
    except Exception as e:
        print(f"\n❌ Error running examples: {e}")
        import traceback
        traceback.print_exc()
    
    print("\n" + "🚀"*30)
    print("Examples complete! Check logs/ for detailed logs.")
    print("🚀"*30)

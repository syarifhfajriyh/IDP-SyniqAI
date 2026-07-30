#!/usr/bin/env python3
"""
Syniq Data Lakehouse - Main CLI Application

This is the main entry point for the Syniq data lakehouse system.
Provides CLI commands for data ingestion, validation, and management.

Usage:
    python main.py ingest                    # Run all ingestion jobs
    python main.py ingest --source postgres  # Run specific source
    python main.py test                      # Test all connections
    python main.py setup                     # Setup MinIO buckets
    python main.py list sources              # List all sources
    python main.py silver validate postgres hosp_raya_patient_record
    python main.py silver transform postgres hosp_raya_patient_record
    python main.py silver report postgres hosp_raya_patient_record
    python main.py gold list                 # List all tables for Gold processing
    python main.py gold process              # Process all tables with full EDA
    python main.py gold process -s postgres -e hosp_raya_patient_record
    python main.py gold report postgres hosp_raya_patient_record
    python main.py gold summary postgres hosp_raya_patient_record
"""

import argparse
import sys
import json
import traceback
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from utils import get_logger, load_config
from ingestion import run_ingestion, IngestionOrchestrator
from connectors import (
    PostgresConnector,
    MariaDBConnector,
    MariaDBCloudConnector,
    MongoDBConnector
)

# Initialize logger
logger = get_logger(__name__)


class SyniqCLI:
    """Main CLI application for Syniq Data Lakehouse."""
    
    def __init__(self):
        """Initialize CLI with configuration paths."""
        self.project_root = Path(__file__).parent
        self.config_dir = self.project_root / "config"
        self.db_config_path = self.config_dir / "db_config.yaml"
        self.ingestion_config_path = self.config_dir / "ingestion_config.yaml"
        self.minio_config_path = self.config_dir / "minio_config.yaml"
        
        # Check if config files exist
        self._verify_config_files()
    
    def _verify_config_files(self):
        """Verify that all required configuration files exist."""
        required_files = [
            self.db_config_path,
            self.ingestion_config_path,
            self.minio_config_path
        ]
        
        missing_files = [f for f in required_files if not f.exists()]
        
        if missing_files:
            logger.error("Missing configuration files:")
            for file in missing_files:
                logger.error(f"  - {file}")
            sys.exit(1)
    
    def load_configurations(self) -> Dict:
        """Load all configuration files."""
        try:
            db_config = load_config(self.db_config_path)
            ingestion_config = load_config(self.ingestion_config_path)
            minio_config = load_config(self.minio_config_path)
            
            return {
                "db": db_config,
                "ingestion": ingestion_config,
                "minio": minio_config
            }
        except Exception as e:
            logger.error(f"Failed to load configurations: {e}")
            sys.exit(1)
    
    # ========================================
    # INGEST COMMAND
    # ========================================
    
    def cmd_stream(self, args):
        """Run direct streaming ingestion to MinIO."""
        logger.info("=" * 70)
        logger.info("SYNIQ DATA LAKEHOUSE - DIRECT STREAM INGESTION")
        logger.info("=" * 70)
        
        try:
            from minio import Minio
            
            # Load config
            with open(args.config) as f:
                config = json.load(f)
            
            # Initialize MinIO from minio_config.yaml
            logger.info("🔌 Connecting to MinIO...")
            minio_config = load_config(self.minio_config_path)["minio"]
            
            minio_client = Minio(
                minio_config["endpoint"],
                access_key=minio_config["access_key"],
                secret_key=minio_config["secret_key"],
                secure=minio_config.get("secure", False)
            )
            
            # Verify MinIO connection
            try:
                minio_client.list_buckets()
                logger.info(f"✅ Connected to MinIO at {minio_config['endpoint']}")
            except Exception as e:
                logger.error(f"❌ Failed to connect to MinIO: {e}")
                sys.exit(1)
            
            # Initialize connector based on source type
            source_type = config['source_type'].lower()
            conn_config = config['connection_config']
            
            logger.info(f"🔌 Connecting to {source_type}...")
            
            if source_type == 'postgres':
                connector = PostgresConnector(conn_config)
            elif source_type == 'mariadb':
                connector = MariaDBConnector(conn_config)
            elif source_type == 'mariadb_cloud':
                connector = MariaDBCloudConnector(conn_config)
            else:
                raise ValueError(f"Unsupported source type: {source_type}")
            
            connector.connect()
            logger.info(f"✅ Connected to {source_type}")
            
            # Import streaming ingestor
            from direct_ingestor import DirectStreamIngestor
            
            # Initialize ingestor
            save_backup = (args.mode == 'hybrid')
            ingestor = DirectStreamIngestor(
                minio_client=minio_client,
                save_local_backup=save_backup
            )
            
            # Extract source and entity
            source = config.get('source_name', config['source_type'])
            entity = config['extraction_request']['entity']
            
            logger.info(f"🚀 Starting DIRECT streaming to MinIO...")
            logger.info(f"   Source: {source}")
            logger.info(f"   Entity: {entity}")
            logger.info(f"   Mode: {args.mode}")
            
            # Run streaming ingestion
            result = ingestor.ingest_streaming(
                connector=connector,
                source=source,
                entity=entity,
                extraction_config=config['extraction_request']
            )
            
            # Cleanup
            connector.close()
            
            # Print result
            print("\n" + "=" * 70)
            print("STREAMING RESULT:")
            print("=" * 70)
            print(json.dumps(result, indent=2, default=str))
            
            if result['success']:
                logger.info(f"\n✅ Success! Data streamed directly to MinIO")
                logger.info(f"   Location: {result['minio_location']}")
                logger.info(f"   Rows: {result['total_rows']:,}")
                logger.info(f"   Speed: {result['rows_per_second']:,} rows/sec")
                sys.exit(0)
            else:
                logger.error(f"\n❌ Ingestion failed")
                sys.exit(1)
                
        except Exception as e:
            logger.error(f"Stream ingestion failed: {e}")
            logger.error(traceback.format_exc())
            sys.exit(1)

    def cmd_ingest(self, args):
        """Run data ingestion."""
        logger.info("=" * 70)
        logger.info("SYNIQ DATA LAKEHOUSE - INGESTION")
        logger.info("=" * 70)
        
        try:
            # Run ingestion with filters
            sources = [args.source] if args.source else None
            entities = [args.entity] if args.entity else None
            
            logger.info(f"Starting ingestion...")
            if sources:
                logger.info(f"  Sources: {sources}")
            if entities:
                logger.info(f"  Entities: {entities}")
            
            report = run_ingestion(
                db_config_path=str(self.db_config_path),
                ingestion_config_path=str(self.ingestion_config_path),
                minio_config_path=str(self.minio_config_path),
                sources=sources,
                entities=entities
            )
            
            # Print summary
            self._print_ingestion_report(report)
            
            # Save report if requested
            if args.report:
                report_path = Path(args.report)
                report_path.parent.mkdir(parents=True, exist_ok=True)
                
                with open(report_path, 'w') as f:
                    json.dump(report, f, indent=2, default=str)
                
                logger.info(f"Report saved to: {report_path}")
            
            # Exit with appropriate code
            if report["failed_jobs"] > 0:
                logger.warning(f"Completed with {report['failed_jobs']} failed job(s)")
                sys.exit(1)
            else:
                logger.info("✅ All jobs completed successfully")
                sys.exit(0)
        
        except Exception as e:
            logger.error(f"Ingestion failed: {e}")
            logger.error(traceback.format_exc())
            sys.exit(1)
    
    def _print_ingestion_report(self, report: Dict):
        """Print ingestion report in a readable format."""
        print("\n" + "=" * 70)
        print("INGESTION REPORT")
        print("=" * 70)
        
        print(f"\n📊 Summary:")
        print(f"  Start Time:      {report['orchestration_start']}")
        print(f"  End Time:        {report['orchestration_end']}")
        print(f"  Duration:        {report['elapsed_seconds']:.2f} seconds")
        print(f"  Total Jobs:      {report['total_jobs']}")
        print(f"  ✅ Successful:   {report['successful_jobs']}")
        print(f"  ❌ Failed:       {report['failed_jobs']}")
        print(f"  Total Rows:      {report['total_rows']:,}")
        print(f"  Total Files:     {report['total_files']}")
        print(f"  Total Size:      {report['total_size_mb']:.2f} MB")
        
        print(f"\n📋 Job Details:")
        for job in report["jobs"]:
            status_icon = "✅" if job["status"] == "SUCCESS" else "❌"
            print(f"\n  {status_icon} {job['source']}/{job['entity']}")
            print(f"     Status:   {job['status']}")
            print(f"     Rows:     {job.get('rows', 0):,}")
            print(f"     Files:    {job.get('files', 0)}")
            print(f"     Duration: {job.get('duration_seconds', 0):.2f}s")
            
            if job["status"] == "FAILED" and "error" in job:
                print(f"     Error:    {job['error']}")
        
        print("\n" + "=" * 70)
    
    # ========================================
    # TEST COMMAND
    # ========================================
    
    def cmd_test(self, args):
        """Test database and MinIO connections."""
        logger.info("=" * 70)
        logger.info("SYNIQ DATA LAKEHOUSE - CONNECTION TEST")
        logger.info("=" * 70)
        
        configs = self.load_configurations()
        
        all_passed = True
        
        # Test database connections
        print("\n📡 Testing Database Connections:")
        
        db_config = configs["db"]
        connector_map = {
            "postgresql": PostgresConnector,
            "mariadb": MariaDBConnector,
            "mariadb_cloud": MariaDBCloudConnector,
            "mongodb": MongoDBConnector
        }
        
        for db_type, config in db_config.items():
            if db_type not in connector_map:
                continue
            
            print(f"\n  Testing {db_type}...")
            
            try:
                connector_class = connector_map[db_type]
                connector = connector_class(config)
                connector.connect()
                result = connector.test_connection()
                connector.disconnect()
                
                if result["connected"]:
                    print(f"    ✅ Connected successfully")
                    if "database" in result:
                        print(f"       Database: {result['database']}")
                    if "version" in result:
                        print(f"       Version: {result['version']}")
                else:
                    print(f"    ❌ Failed: {result.get('error', 'Unknown error')}")
                    all_passed = False
            
            except Exception as e:
                print(f"    ❌ Failed: {e}")
                all_passed = False
        
        # Test MinIO connection
        print(f"\n📦 Testing MinIO Connection:")
        
        try:
            from minio import Minio
            
            minio_config = configs["minio"]["minio"]
            
            client = Minio(
                minio_config["endpoint"],
                access_key=minio_config["access_key"],
                secret_key=minio_config["secret_key"],
                secure=minio_config.get("secure", False)
            )
            
            # List buckets
            buckets = client.list_buckets()
            bucket_names = [b.name for b in buckets]
            
            print(f"  ✅ Connected to MinIO")
            print(f"     Endpoint: {minio_config['endpoint']}")
            print(f"     Buckets: {', '.join(bucket_names) if bucket_names else 'None'}")
            
            # Check required buckets
            required_buckets = configs["minio"]["minio"]["buckets"]
            missing_buckets = [
                b for b in required_buckets.values()
                if b not in bucket_names
            ]
            
            if missing_buckets:
                print(f"  ⚠️  Missing buckets: {', '.join(missing_buckets)}")
                print(f"     Run 'python main.py setup' to create them")
        
        except Exception as e:
            print(f"  ❌ Failed: {e}")
            all_passed = False
        
        # Final result
        print("\n" + "=" * 70)
        if all_passed:
            print("✅ All connection tests passed")
            sys.exit(0)
        else:
            print("❌ Some connection tests failed")
            sys.exit(1)
    
    # ========================================
    # VALIDATE COMMAND
    # ========================================
    
    def cmd_validate(self, args):
        """Validate configuration files."""
        logger.info("=" * 70)
        logger.info("SYNIQ DATA LAKEHOUSE - CONFIGURATION VALIDATION")
        logger.info("=" * 70)
        
        all_valid = True
        
        # Validate each config file
        config_files = {
            "Database Config": self.db_config_path,
            "Ingestion Config": self.ingestion_config_path,
            "MinIO Config": self.minio_config_path
        }
        
        print("\n📋 Validating Configuration Files:")
        
        for name, path in config_files.items():
            print(f"\n  {name} ({path.name}):")
            
            try:
                config = load_config(path)
                
                # Basic validation
                if not config:
                    print(f"    ❌ Empty configuration")
                    all_valid = False
                else:
                    print(f"    ✅ Valid YAML structure")
                    print(f"       Keys: {', '.join(config.keys())}")
            
            except Exception as e:
                print(f"    ❌ Failed: {e}")
                all_valid = False
        
        # Validate ingestion config structure
        print(f"\n📦 Validating Ingestion Configuration:")
        
        try:
            ingestion_config = load_config(self.ingestion_config_path)
            
            if "sources" not in ingestion_config:
                print(f"  ❌ Missing 'sources' section")
                all_valid = False
            else:
                sources = ingestion_config["sources"]
                enabled_sources = [
                    s for s, cfg in sources.items()
                    if cfg.get("enabled", True)
                ]
                
                print(f"  ✅ Found {len(sources)} source(s)")
                print(f"     Enabled: {', '.join(enabled_sources)}")
                
                # Count entities
                total_entities = 0
                for source_name, source_config in sources.items():
                    if "entities" in source_config:
                        entities = source_config["entities"]
                        enabled_entities = [
                            e for e, cfg in entities.items()
                            if cfg.get("enabled", True)
                        ]
                        total_entities += len(enabled_entities)
                
                print(f"     Total Entities: {total_entities}")
        
        except Exception as e:
            print(f"  ❌ Failed: {e}")
            all_valid = False
        
        # Final result
        print("\n" + "=" * 70)
        if all_valid:
            print("✅ All configurations are valid")
            sys.exit(0)
        else:
            print("❌ Some configurations are invalid")
            sys.exit(1)
    
    # ========================================
    # LIST COMMAND
    # ========================================
    
    def cmd_list(self, args):
        """List sources or entities."""
        configs = self.load_configurations()
        ingestion_config = configs["ingestion"]
        
        if args.type == "sources":
            self._list_sources(ingestion_config)
        elif args.type == "entities":
            self._list_entities(ingestion_config, args.source)
        else:
            logger.error(f"Unknown list type: {args.type}")
            sys.exit(1)
    
    def _list_sources(self, ingestion_config: Dict):
        """List all available sources."""
        print("\n📊 Available Sources:")
        print("=" * 70)
        
        if "sources" not in ingestion_config:
            print("No sources configured")
            return
        
        sources = ingestion_config["sources"]
        
        for source_name, source_config in sources.items():
            enabled = source_config.get("enabled", True)
            status = "✅ Enabled" if enabled else "⭕ Disabled"
            connector_type = source_config.get("connector_type", "unknown")
            
            print(f"\n{status} {source_name}")
            print(f"  Connector: {connector_type}")
            
            if "entities" in source_config:
                entity_count = len(source_config["entities"])
                enabled_entities = sum(
                    1 for e in source_config["entities"].values()
                    if e.get("enabled", True)
                )
                print(f"  Entities: {enabled_entities}/{entity_count} enabled")
        
        print("\n" + "=" * 70)
    
    def _list_entities(self, ingestion_config: Dict, source_filter: Optional[str]):
        """List all entities (optionally filtered by source)."""
        print("\n📋 Available Entities:")
        print("=" * 70)
        
        if "sources" not in ingestion_config:
            print("No sources configured")
            return
        
        sources = ingestion_config["sources"]
        
        for source_name, source_config in sources.items():
            # Apply source filter
            if source_filter and source_name != source_filter:
                continue
            
            if "entities" not in source_config:
                continue
            
            print(f"\n📦 {source_name}")
            
            entities = source_config["entities"]
            for entity_name, entity_config in entities.items():
                enabled = entity_config.get("enabled", True)
                status = "✅" if enabled else "⭕"
                
                extraction = entity_config.get("extraction", {})
                mode = extraction.get("mode", "full")
                batch_size = extraction.get("batch_size", "default")
                
                print(f"  {status} {entity_name}")
                print(f"      Mode: {mode}, Batch: {batch_size}")
        
        print("\n" + "=" * 70)
    
    # ========================================
    # SETUP COMMAND
    # ========================================
    
    def cmd_setup(self, args):
        """Setup MinIO buckets."""
        logger.info("=" * 70)
        logger.info("SYNIQ DATA LAKEHOUSE - MINIO SETUP")
        logger.info("=" * 70)
        
        try:
            from minio import Minio
            
            # Only load MinIO config for setup
            minio_config = load_config(self.minio_config_path)["minio"]
            
            print(f"\n📦 Connecting to MinIO...")
            print(f"   Endpoint: {minio_config['endpoint']}")
            
            client = Minio(
                minio_config["endpoint"],
                access_key=minio_config["access_key"],
                secret_key=minio_config["secret_key"],
                secure=minio_config.get("secure", False)
            )
            
            # Get required buckets
            required_buckets = minio_config["buckets"]
            
            print(f"\n📋 Creating buckets:")
            
            for bucket_type, bucket_name in required_buckets.items():
                try:
                    if client.bucket_exists(bucket_name):
                        print(f"  ✅ {bucket_name} (already exists)")
                    else:
                        client.make_bucket(bucket_name)
                        print(f"  ✅ {bucket_name} (created)")
                except Exception as e:
                    print(f"  ❌ {bucket_name} (failed: {e})")
            
            print("\n" + "=" * 70)
            print("✅ MinIO setup completed")
            sys.exit(0)
        
        except Exception as e:
            logger.error(f"Setup failed: {e}")
            logger.error(traceback.format_exc())
            sys.exit(1)
    
    # ========================================
    # STATUS COMMAND
    # ========================================
    
    def cmd_status(self, args):
        """Show system status."""
        logger.info("=" * 70)
        logger.info("SYNIQ DATA LAKEHOUSE - SYSTEM STATUS")
        logger.info("=" * 70)
        
        print("\n📋 System Status:")
        print("=" * 70)
        
        # Check configuration files
        print(f"\n✅ Configuration Files:")
        
        for name, path in {
            "Database": self.db_config_path,
            "Ingestion": self.ingestion_config_path,
            "MinIO": self.minio_config_path
        }.items():
            status = "✅" if path.exists() else "❌"
            print(f"  {status} {name}: {path.name}")
        
        # Check database config structure
        print(f"\n📊 Database Sources:")
        try:
            configs = self.load_configurations()
            sources = configs["ingestion"].get("sources", {})
            total_entities = sum(
                len(src.get("entities", {}))
                for src in sources.values()
            )
            print(f"  Total:   {len(sources)} source(s)")
            print(f"  Entities: {total_entities}")
            
            print(f"\n📋 Entities:")
            for source_config in sources.values():
                if "entities" in source_config:
                    entities = source_config["entities"]
                    enabled_entities = sum(
                        1 for e in entities.values()
                        if e.get("enabled", True)
                    )
            print(f"  Total:   {total_entities}")
            print(f"  Enabled: {enabled_entities}")
        except Exception as e:
            print(f"\n⚠️  Could not load ingestion config: {e}")
        
        # Check MinIO connection
        print(f"\n📦 MinIO:")
        try:
            from minio import Minio
            
            minio_config = load_config(self.minio_config_path)["minio"]
            client = Minio(
                minio_config["endpoint"],
                access_key=minio_config["access_key"],
                secret_key=minio_config["secret_key"],
                secure=minio_config.get("secure", False)
            )
            
            buckets = client.list_buckets()
            print(f"  Status:  ✅ Connected")
            print(f"  Buckets: {len(buckets)}")
        except Exception as e:
            print(f"  Status:  ❌ Not connected ({e})")
        
        print("\n" + "=" * 70)
    
    # ========================================
    # SILVER LAYER COMMANDS
    # ========================================
    
    def cmd_silver_validate(self, args):
        """Run Silver layer quality gate (light EDA)."""
        logger.info("=" * 70)
        logger.info("SYNIQ DATA LAKEHOUSE - SILVER QUALITY GATE")
        logger.info("=" * 70)
        try:
            import pandas as pd
            from silver_quality_gate import SilverQualityGate
            
            # Load bronze parquet
            bronze_path = f"bronze/{args.source}/{args.entity}/"
            df = pd.read_parquet(bronze_path)
            
            # Define expected schema
            expected_schema = {
                col: 'numeric' if pd.api.types.is_numeric_dtype(df[col]) else 'object'
                for col in df.columns
            }
            min_rows = 100
            
            qg = SilverQualityGate(df)
            issues = qg.perform_quality_checks(expected_schema, min_rows)
            quality_score = 100.0 - (len(issues) * 2)
            
            quality_report = {
                "quality_passed": len(issues) == 0,
                "quality_score": quality_score,
                "issues": issues,
                "recommendation": "proceed_with_cleaning" if len(issues) == 0 else "investigate"
            }
            
            # Save report
            report_path = f"silver/quality_reports/{args.source}/{args.entity}/quality_gate.json"
            Path(report_path).parent.mkdir(parents=True, exist_ok=True)
            with open(report_path, 'w') as f:
                json.dump(quality_report, f, indent=2)
            
            logger.info(f"Quality report saved to: {report_path}")
            print(json.dumps(quality_report, indent=2))
            
        except Exception as e:
            logger.error(f"Silver quality gate failed: {e}")
            logger.error(traceback.format_exc())
            sys.exit(1)

    def cmd_silver_transform(self, args):
        """Run Silver layer data cleaning and transformation."""
        logger.info("=" * 70)
        logger.info("SYNIQ DATA LAKEHOUSE - SILVER TRANSFORM")
        logger.info("=" * 70)
        try:
            import pandas as pd
            from silver_transformer import SilverTransformer
            
            bronze_path = f"bronze/{args.source}/{args.entity}/"
            df = pd.read_parquet(bronze_path)
            
            transformer = SilverTransformer(config_path="config/cleaning_rules.yaml")
            cleaned_df = transformer.transform(df, source_system=args.source, bronze_location=bronze_path)
            
            # Save cleaned parquet
            output_path = f"silver/{args.source}/{args.entity}/cleaned-00001.parquet"
            Path(output_path).parent.mkdir(parents=True, exist_ok=True)
            transformer.save_to_parquet(cleaned_df, output_path)
            
            # Save cleaning summary
            summary = transformer.get_cleaning_summary()
            summary_path = f"silver/quality_reports/{args.source}/{args.entity}/cleaning_summary.json"
            Path(summary_path).parent.mkdir(parents=True, exist_ok=True)
            with open(summary_path, 'w') as f:
                json.dump(summary, f, indent=2, default=str)
            
            logger.info(f"Cleaning summary saved to: {summary_path}")
            print(json.dumps(summary, indent=2, default=str))
            
        except Exception as e:
            logger.error(f"Silver transform failed: {e}")
            logger.error(traceback.format_exc())
            sys.exit(1)

    def cmd_silver_report(self, args):
        """View Silver layer quality report."""
        logger.info("=" * 70)
        logger.info("SYNIQ DATA LAKEHOUSE - SILVER QUALITY REPORT")
        logger.info("=" * 70)
        try:
            report_path = f"silver/quality_reports/{args.source}/{args.entity}/quality_gate.json"
            with open(report_path, 'r') as f:
                report = json.load(f)
            print(json.dumps(report, indent=2))
        except Exception as e:
            logger.error(f"Silver report failed: {e}")
            logger.error(traceback.format_exc())
            sys.exit(1)
    
    # ========================================
    # CATALOG COMMANDS
    # ========================================
    
    def cmd_catalog_list(self, args):
        """List all tables in catalog."""
        from minio import Minio
        from catalog.catalog_manager import CatalogManager
        
        try:
            # Load MinIO config
            minio_config = load_config(self.minio_config_path)["minio"]
            client = Minio(
                minio_config["endpoint"],
                access_key=minio_config["access_key"],
                secret_key=minio_config["secret_key"],
                secure=minio_config.get("secure", False)
            )
            
            catalog = CatalogManager(client)
            tables = catalog.list_tables(source=args.source if hasattr(args, 'source') else None)
            
            print(f"\n📚 Catalog: {len(tables)} tables\n")
            print("=" * 80)
            
            if not tables:
                print("  (no tables found)")
            else:
                for table in tables:
                    print(f"  • {table['source']}.{table['entity']}")
                    print(f"      Version: v{table['version']} | Rows: {table['row_count']:,} | Updated: {table['updated_at'][:10]}")
            
            print("\n" + "=" * 80)
            
        except Exception as e:
            logger.error(f"Failed to list catalog: {e}")
            sys.exit(1)
    
    def cmd_catalog_info(self, args):
        """Show detailed table information."""
        from minio import Minio
        from catalog.catalog_manager import CatalogManager
        
        try:
            # Load MinIO config
            minio_config = load_config(self.minio_config_path)["minio"]
            client = Minio(
                minio_config["endpoint"],
                access_key=minio_config["access_key"],
                secret_key=minio_config["secret_key"],
                secure=minio_config.get("secure", False)
            )
            
            catalog = CatalogManager(client)
            metadata = catalog.get_table(args.source, args.entity)
            
            if not metadata:
                print(f"\n❌ Table not found: {args.source}.{args.entity}")
                sys.exit(1)
            
            print(f"\n📋 {args.source}.{args.entity}\n")
            print("=" * 80)
            print(f"  Version:     v{metadata['version']}")
            print(f"  Location:    {metadata['location']}")
            print(f"  Rows:        {metadata['row_count']:,}")
            print(f"  Partitions:  {', '.join(metadata['partition_keys']) if metadata['partition_keys'] else 'None'}")
            print(f"  Status:      {metadata['status']}")
            print(f"  Created:     {metadata['created_at']}")
            print(f"  Updated:     {metadata['updated_at']}")
            
            print(f"\n  Schema ({len(metadata['schema'])} columns):")
            for col, dtype in metadata['schema'].items():
                print(f"    • {col}: {dtype}")
            
            if metadata.get('tags'):
                print(f"\n  Tags:")
                for key, value in metadata['tags'].items():
                    print(f"    • {key}: {value}")
            
            print("\n" + "=" * 80)
            
        except Exception as e:
            logger.error(f"Failed to get table info: {e}")
            sys.exit(1)
    
    def cmd_catalog_history(self, args):
        """Show schema change history."""
        from minio import Minio
        from catalog.catalog_manager import CatalogManager
        
        try:
            # Load MinIO config
            minio_config = load_config(self.minio_config_path)["minio"]
            client = Minio(
                minio_config["endpoint"],
                access_key=minio_config["access_key"],
                secret_key=minio_config["secret_key"],
                secure=minio_config.get("secure", False)
            )
            
            catalog = CatalogManager(client)
            versions = catalog.get_schema_history(args.source, args.entity)
            
            if not versions:
                print(f"\n❌ No history found for: {args.source}.{args.entity}")
                sys.exit(1)
            
            print(f"\n📜 Schema History: {args.source}.{args.entity}\n")
            print("=" * 80)
            
            for v in versions:
                print(f"  v{v['version']}: {v['updated_at']} | {v['row_count']:,} rows | Hash: {v['schema_hash']}")
            
            print("\n" + "=" * 80)
            
        except Exception as e:
            logger.error(f"Failed to get schema history: {e}")
            sys.exit(1)
    
    # ========================================
    # QUALITY COMMANDS
    # ========================================
    
    def cmd_quality_report(self, args):
        """Show data quality report."""
        from minio import Minio
        from quality.profiler import DataProfiler
        
        try:
            # Load MinIO config
            minio_config = load_config(self.minio_config_path)["minio"]
            client = Minio(
                minio_config["endpoint"],
                access_key=minio_config["access_key"],
                secret_key=minio_config["secret_key"],
                secure=minio_config.get("secure", False)
            )
            
            profiler = DataProfiler(client)
            profile = profiler.get_latest_profile(args.source, args.entity)
            
            if not profile:
                print(f"\n❌ No profile found for: {args.source}.{args.entity}")
                sys.exit(1)
            
            overview = profile["overview"]
            metadata = profile["metadata"]
            
            print(f"\n📊 Data Quality Report: {args.source}.{args.entity}\n")
            print("=" * 80)
            print(f"  Quality Score:    {profile['quality_score']:.2f}%")
            print(f"  Profiled At:      {metadata['profiled_at']}")
            print(f"\n  Overview:")
            print(f"    Rows:           {overview['row_count']:,}")
            print(f"    Columns:        {overview['column_count']}")
            print(f"    Memory:         {overview['memory_usage_mb']:.2f} MB")
            print(f"    Missing:        {overview['missing_percentage']:.2f}%")
            print(f"    Duplicates:     {overview['duplicate_percentage']:.2f}%")
            
            # Show columns with issues
            print(f"\n  Column Quality Issues:")
            issues_found = False
            for col_name, col_data in profile["columns"].items():
                if col_data.get("missing_percentage", 0) > 10:
                    print(f"    ⚠️  {col_name}: {col_data['missing_percentage']:.1f}% missing")
                    issues_found = True
                if col_data.get("outliers", {}).get("percentage", 0) > 5:
                    print(f"    ⚠️  {col_name}: {col_data['outliers']['percentage']:.1f}% outliers")
                    issues_found = True
            
            if not issues_found:
                print(f"    ✅ No significant issues detected")
            
            print("\n" + "=" * 80)
            
        except Exception as e:
            logger.error(f"Failed to get quality report: {e}")
            sys.exit(1)
    
    # ========================================
    # LINEAGE COMMANDS
    # ========================================
    
    def cmd_lineage_show(self, args):
        """Show data lineage."""
        from minio import Minio
        from lineage.lineage_tracker import LineageTracker
        
        try:
            # Load MinIO config
            minio_config = load_config(self.minio_config_path)["minio"]
            client = Minio(
                minio_config["endpoint"],
                access_key=minio_config["access_key"],
                secret_key=minio_config["secret_key"],
                secure=minio_config.get("secure", False)
            )
            
            lineage_tracker = LineageTracker(client)
            report = lineage_tracker.generate_lineage_report(args.layer, args.location)
            
            print(report)
            
        except Exception as e:
            logger.error(f"Failed to get lineage: {e}")
            sys.exit(1)
    
    # ========================================
    # GOLD LAYER COMMANDS
    # ========================================
    
    def cmd_gold_list(self, args):
        """List all tables available for Gold layer processing."""
        from gold_aggregator import GoldAggregator
        
        logger.info("=" * 80)
        logger.info("GOLD LAYER - TABLE DISCOVERY")
        logger.info("=" * 80)
        
        try:
            aggregator = GoldAggregator()
            table_list = aggregator.list_all_tables()
            
            print(f"\n📊 Found {table_list['total_tables']} tables in Silver layer:\n")
            
            for source, tables in table_list['by_source'].items():
                print(f"  {source}:")
                for table in tables:
                    print(f"    - {table}")
            
            print(f"\n✅ Total: {table_list['total_tables']} tables ready for Gold processing")
            
        except Exception as e:
            logger.error(f"Failed to list tables: {e}")
            sys.exit(1)
    
    def cmd_gold_process(self, args):
        """Process a specific table or all tables for Gold layer."""
        from gold_aggregator import GoldAggregator
        
        logger.info("=" * 80)
        logger.info("GOLD LAYER - EDA PROCESSING")
        logger.info("=" * 80)
        
        try:
            aggregator = GoldAggregator()
            
            if args.source and args.entity:
                # Process specific table
                print(f"\n📊 Processing: {args.source}.{args.entity}")
                
                result = aggregator.process_table(
                    args.source,
                    args.entity,
                    save_gold=not args.no_save,
                    generate_report=not args.no_report
                )
                
                if result.get('status') == 'success':
                    print(f"\n✅ Processing completed successfully!")
                    print(f"   Rows processed: {result['rows_processed']:,}")
                    print(f"   Gold rows: {result['gold_rows']:,}")
                    print(f"   Quality score: {result['eda_summary']['quality_score']:.1f}/100")
                    print(f"   Completeness: {result['eda_summary']['completeness']:.1f}%")
                    print(f"   Insights: {result['eda_summary']['insights_count']}")
                else:
                    print(f"\n❌ Processing failed: {result.get('error')}")
                    sys.exit(1)
            else:
                # Process all tables
                print(f"\n📊 Processing all tables in Silver layer...\n")
                
                results = aggregator.process_all_tables(
                    save_gold=not args.no_save,
                    generate_reports=not args.no_report
                )
                
                print(f"\n{'='*80}")
                print("PROCESSING SUMMARY")
                print("=" * 80)
                print(f"Total tables: {results['total_tables']}")
                print(f"Succeeded: {len(results['processed'])}")
                print(f"Failed: {len(results['failed'])}")
                
                if results['processed']:
                    print(f"\n📊 Quality Scores:")
                    for result in results['processed']:
                        score = result['eda_summary']['quality_score']
                        emoji = "🟢" if score >= 90 else "🟡" if score >= 75 else "🟠" if score >= 60 else "🔴"
                        print(f"  {emoji} {result['source_system']}.{result['table_name']}: {score:.1f}/100")
                
                if results['failed']:
                    print(f"\n❌ Failed tables:")
                    for failed in results['failed']:
                        print(f"  - {failed['source_system']}.{failed['table_name']}: {failed['error']}")
                
                print(f"\n✅ Gold layer processing complete!")
                
        except Exception as e:
            logger.error(f"Failed to process Gold layer: {e}")
            logger.error(traceback.format_exc())
            sys.exit(1)
    
    def cmd_gold_report(self, args):
        """View Gold layer EDA report for a specific table."""
        from pathlib import Path
        import json
        
        logger.info("=" * 80)
        logger.info("GOLD LAYER - EDA REPORT VIEWER")
        logger.info("=" * 80)
        
        try:
            reports_dir = Path("gold/eda_reports")
            
            if not reports_dir.exists():
                print("\n❌ No EDA reports found. Run 'gold process' first.")
                sys.exit(1)
            
            # Find the most recent report for this table
            pattern = f"{args.source}_{args.entity}_eda_*.json"
            report_files = sorted(reports_dir.glob(pattern), reverse=True)
            
            if not report_files:
                print(f"\n❌ No report found for {args.source}.{args.entity}")
                print(f"   Run: python main.py gold process --source {args.source} --entity {args.entity}")
                sys.exit(1)
            
            report_file = report_files[0]
            
            with open(report_file, 'r') as f:
                report = json.load(f)
            
            # Display report
            print(f"\n📊 EDA Report: {args.source}.{args.entity}")
            print(f"   Report: {report_file.name}")
            print(f"   Analyzed: {report['analysis_timestamp']}")
            
            print(f"\n{'='*80}")
            print("BASIC INFORMATION")
            print("=" * 80)
            basic = report['basic_info']
            print(f"  Rows: {int(basic['row_count']):,}")
            print(f"  Columns: {int(basic['column_count'])}")
            print(f"  Memory: {float(basic['memory_usage_mb']):.2f} MB")
            print(f"  Duplicates: {int(basic['duplicates']):,}")
            
            print(f"\n{'='*80}")
            print("DATA QUALITY")
            print("=" * 80)
            quality = report['data_quality']
            score = float(quality['overall_quality_score'])
            emoji = "🟢" if score >= 90 else "🟡" if score >= 75 else "🟠" if score >= 60 else "🔴"
            print(f"  {emoji} Overall Quality Score: {score:.1f}/100")
            print(f"  Completeness: {float(quality['completeness_score']):.1f}%")
            print(f"  Missing Values: {int(quality['missing_value_count']):,} ({float(quality['missing_percentage']):.2f}%)")
            print(f"  Duplicates: {int(quality['duplicate_rows']):,} ({float(quality['duplicate_percentage']):.2f}%)")
            
            print(f"\n{'='*80}")
            print("KEY INSIGHTS")
            print("=" * 80)
            for i, insight in enumerate(report['insights'], 1):
                print(f"  {i}. {insight}")
            
            if report.get('business_metrics'):
                print(f"\n{'='*80}")
                print("BUSINESS METRICS")
                print("=" * 80)
                for metric, value in report['business_metrics'].items():
                    if isinstance(value, (int, float)):
                        print(f"  {metric.replace('_', ' ').title()}: {value:,.2f}")
            
            print(f"\n💡 View full report:")
            print(f"   HTML: {report_file.with_suffix('.html')}")
            print(f"   Markdown: {report_file.with_suffix('.md')}")
            
        except Exception as e:
            logger.error(f"Failed to view report: {e}")
            logger.error(traceback.format_exc())
            sys.exit(1)
    
    def cmd_gold_summary(self, args):
        """Get a quick summary of a table without full processing."""
        from gold_aggregator import GoldAggregator
        
        logger.info("=" * 80)
        logger.info("GOLD LAYER - TABLE SUMMARY")
        logger.info("=" * 80)
        
        try:
            aggregator = GoldAggregator()
            summary = aggregator.get_table_summary(args.source, args.entity)
            
            if not summary:
                print(f"\n❌ Table not found: {args.source}.{args.entity}")
                sys.exit(1)
            
            print(f"\n📊 Table: {args.source}.{args.entity}")
            print(f"\n  Rows: {summary['row_count']:,}")
            print(f"  Columns: {summary['column_count']}")
            print(f"  Memory: {summary['memory_mb']:.2f} MB")
            
            print(f"\n  Columns:")
            for col in summary['columns']:
                print(f"    - {col}")
            
            if args.sample:
                print(f"\n  Sample Data (first 5 rows):")
                import pandas as pd
                df = pd.DataFrame(summary['sample_data'])
                print(df.to_string(index=False))
            
        except Exception as e:
            logger.error(f"Failed to get summary: {e}")
            logger.error(traceback.format_exc())
            sys.exit(1)


def main():
    """Main entry point for CLI."""
    cli = SyniqCLI()
    
    # Create argument parser
    parser = argparse.ArgumentParser(
        description="Syniq Data Lakehouse - Data Ingestion Platform",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Ingestion
  python main.py ingest                         # Run all ingestion jobs
  python main.py ingest --source postgres_prod  # Run specific source
  python main.py ingest --entity customers      # Run specific entity
  
  # Silver Layer (Bronze → Silver transformation)
  python main.py silver validate postgres hosp_raya_patient_record   # Quality gate check
  python main.py silver transform postgres hosp_raya_patient_record  # Data cleaning
  python main.py silver report postgres hosp_raya_patient_record     # View quality report
  
  # Catalog Management
  python main.py catalog list                   # List all tables
  python main.py catalog list --source postgres # Filter by source
  python main.py catalog info postgres_prod customers  # Table details
  python main.py catalog history postgres_prod customers  # Schema history
  
  # Data Quality
  python main.py quality report postgres_prod customers  # Quality report
  
  # Data Lineage
  python main.py lineage show bronze postgres_prod/customers  # Show lineage
  
  # System Management
  python main.py test                           # Test all connections
  python main.py setup                          # Setup MinIO buckets
  python main.py list sources                   # List all sources
  python main.py list entities                  # List all entities
  python main.py validate                       # Validate configurations
  python main.py status                         # Show system status
        """
    )
    
    # Create subparsers
    subparsers = parser.add_subparsers(dest="command", help="Available commands")
    
    # INGEST command
    ingest_parser = subparsers.add_parser(
        "ingest",
        help="Run data ingestion"
    )
    ingest_parser.add_argument(
        "--source", "-s",
        help="Filter by source name"
    )
    ingest_parser.add_argument(
        "--entity", "-e",
        help="Filter by entity name"
    )
    ingest_parser.add_argument(
        "--report", "-r",
        help="Save report to JSON file"
    )
    ingest_parser.set_defaults(func=cli.cmd_ingest)
    
    # TEST command
    test_parser = subparsers.add_parser(
        "test",
        help="Test database and MinIO connections"
    )
    test_parser.set_defaults(func=cli.cmd_test)
    
    # VALIDATE command
    validate_parser = subparsers.add_parser(
        "validate",
        help="Validate configuration files"
    )
    validate_parser.set_defaults(func=cli.cmd_validate)
    
    # LIST command
    list_parser = subparsers.add_parser(
        "list",
        help="List sources or entities"
    )
    list_parser.add_argument(
        "type",
        choices=["sources", "entities"],
        help="What to list"
    )
    list_parser.add_argument(
        "--source",
        help="Filter entities by source"
    )
    list_parser.set_defaults(func=cli.cmd_list)
    
    # SETUP command
    setup_parser = subparsers.add_parser(
        "setup",
        help="Setup MinIO buckets"
    )
    setup_parser.set_defaults(func=cli.cmd_setup)
    
    # STATUS command
    status_parser = subparsers.add_parser(
        "status",
        help="Show system status"
    )
    status_parser.set_defaults(func=cli.cmd_status)
    
    # SILVER command group
    silver_parser = subparsers.add_parser(
        "silver",
        help="Silver layer pipeline commands"
    )
    silver_subparsers = silver_parser.add_subparsers(dest="silver_command", help="Silver commands")

    # silver validate
    silver_validate_parser = silver_subparsers.add_parser(
        "validate",
        help="Run Silver layer quality gate (light EDA)"
    )
    silver_validate_parser.add_argument("source", help="Source name")
    silver_validate_parser.add_argument("entity", help="Entity name")
    silver_validate_parser.set_defaults(func=cli.cmd_silver_validate)

    # silver transform
    silver_transform_parser = silver_subparsers.add_parser(
        "transform",
        help="Run Silver layer data cleaning and transformation"
    )
    silver_transform_parser.add_argument("source", help="Source name")
    silver_transform_parser.add_argument("entity", help="Entity name")
    silver_transform_parser.set_defaults(func=cli.cmd_silver_transform)

    # silver report
    silver_report_parser = silver_subparsers.add_parser(
        "report",
        help="View Silver layer quality report"
    )
    silver_report_parser.add_argument("source", help="Source name")
    silver_report_parser.add_argument("entity", help="Entity name")
    silver_report_parser.set_defaults(func=cli.cmd_silver_report)
    
    # ========================================
    # STREAM COMMAND
    # ========================================

    stream_parser = subparsers.add_parser(
        "stream",
        help="Direct streaming ingestion to MinIO"
    )
    stream_parser.add_argument(
        "config",
        help="Path to config.json"
    )
    stream_parser.add_argument(
        "--mode",
        choices=['streaming', 'hybrid'],
        default='streaming',
        help="streaming=direct to MinIO, hybrid=also save local backup"
    )
    stream_parser.set_defaults(func=cli.cmd_stream)

    # CATALOG commands
    catalog_parser = subparsers.add_parser(
        "catalog",
        help="Catalog management commands"
    )
    catalog_subparsers = catalog_parser.add_subparsers(dest="catalog_command", help="Catalog commands")
    
    # catalog list
    catalog_list_parser = catalog_subparsers.add_parser(
        "list",
        help="List all tables in catalog"
    )
    catalog_list_parser.add_argument(
        "--source", "-s",
        help="Filter by source"
    )
    catalog_list_parser.set_defaults(func=cli.cmd_catalog_list)
    
    # catalog info
    catalog_info_parser = catalog_subparsers.add_parser(
        "info",
        help="Show detailed table information"
    )
    catalog_info_parser.add_argument("source", help="Source name")
    catalog_info_parser.add_argument("entity", help="Entity name")
    catalog_info_parser.set_defaults(func=cli.cmd_catalog_info)
    
    # catalog history
    catalog_history_parser = catalog_subparsers.add_parser(
        "history",
        help="Show schema change history"
    )
    catalog_history_parser.add_argument("source", help="Source name")
    catalog_history_parser.add_argument("entity", help="Entity name")
    catalog_history_parser.set_defaults(func=cli.cmd_catalog_history)
    
    # QUALITY commands
    quality_parser = subparsers.add_parser(
        "quality",
        help="Data quality commands"
    )
    quality_subparsers = quality_parser.add_subparsers(dest="quality_command", help="Quality commands")
    
    # quality report
    quality_report_parser = quality_subparsers.add_parser(
        "report",
        help="Show data quality report"
    )
    quality_report_parser.add_argument("source", help="Source name")
    quality_report_parser.add_argument("entity", help="Entity name")
    quality_report_parser.set_defaults(func=cli.cmd_quality_report)
    
    # LINEAGE commands
    lineage_parser = subparsers.add_parser(
        "lineage",
        help="Data lineage commands"
    )
    lineage_subparsers = lineage_parser.add_subparsers(dest="lineage_command", help="Lineage commands")
    
    # lineage show
    lineage_show_parser = lineage_subparsers.add_parser(
        "show",
        help="Show data lineage"
    )
    lineage_show_parser.add_argument("layer", help="Layer name (bronze, silver, gold)")
    lineage_show_parser.add_argument("location", help="Location path")
    lineage_show_parser.set_defaults(func=cli.cmd_lineage_show)
    
    # GOLD LAYER commands
    gold_parser = subparsers.add_parser(
        "gold",
        help="Gold layer - EDA and business-ready datasets"
    )
    gold_subparsers = gold_parser.add_subparsers(dest="gold_command", help="Gold layer commands")
    
    # gold list
    gold_list_parser = gold_subparsers.add_parser(
        "list",
        help="List all tables available for Gold processing"
    )
    gold_list_parser.set_defaults(func=cli.cmd_gold_list)
    
    # gold process
    gold_process_parser = gold_subparsers.add_parser(
        "process",
        help="Process tables with full EDA and create Gold datasets"
    )
    gold_process_parser.add_argument(
        "--source", "-s",
        help="Source system (process specific table)"
    )
    gold_process_parser.add_argument(
        "--entity", "-e",
        help="Entity/table name (process specific table)"
    )
    gold_process_parser.add_argument(
        "--no-save",
        action="store_true",
        help="Skip saving Gold datasets (EDA only)"
    )
    gold_process_parser.add_argument(
        "--no-report",
        action="store_true",
        help="Skip generating EDA reports"
    )
    gold_process_parser.set_defaults(func=cli.cmd_gold_process)
    
    # gold report
    gold_report_parser = gold_subparsers.add_parser(
        "report",
        help="View EDA report for a specific table"
    )
    gold_report_parser.add_argument("source", help="Source name")
    gold_report_parser.add_argument("entity", help="Entity/table name")
    gold_report_parser.set_defaults(func=cli.cmd_gold_report)
    
    # gold summary
    gold_summary_parser = gold_subparsers.add_parser(
        "summary",
        help="Quick summary of a table (no processing)"
    )
    gold_summary_parser.add_argument("source", help="Source name")
    gold_summary_parser.add_argument("entity", help="Entity/table name")
    gold_summary_parser.add_argument(
        "--sample",
        action="store_true",
        help="Show sample data"
    )
    gold_summary_parser.set_defaults(func=cli.cmd_gold_summary)

    # Parse arguments
    args = parser.parse_args()

    # Show help if no command
    if not args.command:
        parser.print_help()
        sys.exit(1)

    # Execute command
    try:
        args.func(args)
    except KeyboardInterrupt:
        logger.info("\n⚠️  Interrupted by user")
        sys.exit(130)
    except Exception as e:
        logger.error(f"Command failed: {e}")
        logger.error(traceback.format_exc())
        sys.exit(1)


if __name__ == "__main__":
    main()
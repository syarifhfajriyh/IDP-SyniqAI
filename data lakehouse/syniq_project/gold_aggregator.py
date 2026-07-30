"""
Gold Layer Aggregator

Orchestrates the Gold layer processing:
- Discovers all tables from Silver layer
- Performs comprehensive EDA
- Creates aggregated views
- Generates business-ready datasets
"""

import pandas as pd
from pathlib import Path
from typing import Dict, List, Any, Optional
from datetime import datetime
import json
import logging

from gold_eda_engine import GoldEDAEngine
from gold_report_generator import GoldReportGenerator
from utils import get_logger

logger = get_logger(__name__)


class GoldAggregator:
    """
    Main orchestrator for Gold layer data processing.
    Discovers tables, performs EDA, and creates business-ready datasets.
    """
    
    def __init__(self, silver_path: str = "silver", 
                 gold_path: str = "gold"):
        """
        Initialize the Gold Aggregator.
        
        Args:
            silver_path: Path to Silver layer data
            gold_path: Path to Gold layer output
        """
        self.logger = logger
        self.silver_path = Path(silver_path)
        self.gold_path = Path(gold_path)
        self.eda_engine = GoldEDAEngine()
        self.report_generator = GoldReportGenerator()
        
        # Ensure gold directories exist
        self.gold_path.mkdir(parents=True, exist_ok=True)
        (self.gold_path / "eda_reports").mkdir(exist_ok=True)
        
    def discover_tables(self) -> List[Dict[str, str]]:
        """
        Discover all tables in the Silver layer.
        
        Returns:
            List of dictionaries with source_system and table_name
        """
        self.logger.info("Discovering tables in Silver layer...")
        tables = []
        
        if not self.silver_path.exists():
            self.logger.warning(f"Silver path does not exist: {self.silver_path}")
            return tables
        
        # Scan for source systems
        for source_dir in self.silver_path.iterdir():
            if source_dir.is_dir() and not source_dir.name.startswith('.'):
                source_system = source_dir.name
                
                # Skip quality_reports
                if source_system == 'quality_reports':
                    continue
                
                # Scan for tables in this source system
                for table_dir in source_dir.iterdir():
                    if table_dir.is_dir() and not table_dir.name.startswith('.'):
                        table_name = table_dir.name
                        
                        # Check for parquet files
                        parquet_files = list(table_dir.glob("*.parquet"))
                        if parquet_files:
                            tables.append({
                                'source_system': source_system,
                                'table_name': table_name,
                                'path': str(table_dir),
                                'file_count': len(parquet_files)
                            })
                            self.logger.info(f"Found table: {source_system}.{table_name} "
                                           f"({len(parquet_files)} files)")
        
        self.logger.info(f"Discovered {len(tables)} tables in Silver layer")
        return tables
    
    def list_all_tables(self) -> Dict[str, Any]:
        """
        List all available tables with metadata.
        
        Returns:
            Dictionary with table listing and summary
        """
        tables = self.discover_tables()
        
        summary = {
            'total_tables': len(tables),
            'timestamp': datetime.now().isoformat(),
            'tables': tables,
            'by_source': {}
        }
        
        # Group by source system
        for table in tables:
            source = table['source_system']
            if source not in summary['by_source']:
                summary['by_source'][source] = []
            summary['by_source'][source].append(table['table_name'])
        
        return summary
    
    def process_table(self, source_system: str, table_name: str,
                     save_gold: bool = True, 
                     generate_report: bool = True) -> Dict[str, Any]:
        """
        Process a single table: load, analyze, aggregate, and save.
        
        Args:
            source_system: Source system identifier
            table_name: Table name
            save_gold: Whether to save aggregated gold data
            generate_report: Whether to generate EDA report
            
        Returns:
            Dictionary with processing results
        """
        self.logger.info(f"Processing table: {source_system}.{table_name}")
        
        # Load silver data
        df = self._load_silver_table(source_system, table_name)
        if df is None:
            return {'error': 'Failed to load table'}
        
        self.logger.info(f"Loaded {len(df)} rows, {len(df.columns)} columns")
        
        # Perform EDA
        eda_results = self.eda_engine.analyze_dataframe(df, table_name, source_system)
        
        # Save EDA report
        if generate_report:
            report_path = self.report_generator.generate_report(
                eda_results, 
                output_dir=str(self.gold_path / "eda_reports")
            )
            self.logger.info(f"EDA report saved: {report_path}")
        
        # Create aggregated gold dataset
        gold_df = self._create_gold_dataset(df, eda_results)
        
        # Save gold data
        if save_gold:
            gold_file = self._save_gold_data(gold_df, source_system, table_name)
            self.logger.info(f"Gold data saved: {gold_file}")
        
        return {
            'source_system': source_system,
            'table_name': table_name,
            'status': 'success',
            'rows_processed': len(df),
            'gold_rows': len(gold_df),
            'eda_summary': {
                'quality_score': eda_results['data_quality']['overall_quality_score'],
                'completeness': eda_results['data_quality']['completeness_score'],
                'insights_count': len(eda_results['insights'])
            }
        }
    
    def process_all_tables(self, save_gold: bool = True,
                          generate_reports: bool = True) -> Dict[str, Any]:
        """
        Process all tables in the Silver layer.
        
        Args:
            save_gold: Whether to save gold datasets
            generate_reports: Whether to generate EDA reports
            
        Returns:
            Dictionary with overall processing results
        """
        self.logger.info("Starting Gold layer processing for all tables...")
        
        tables = self.discover_tables()
        results = {
            'timestamp': datetime.now().isoformat(),
            'total_tables': len(tables),
            'processed': [],
            'failed': []
        }
        
        for table_info in tables:
            try:
                result = self.process_table(
                    table_info['source_system'],
                    table_info['table_name'],
                    save_gold=save_gold,
                    generate_report=generate_reports
                )
                results['processed'].append(result)
            except Exception as e:
                self.logger.error(f"Failed to process {table_info['source_system']}."
                                f"{table_info['table_name']}: {str(e)}")
                results['failed'].append({
                    'source_system': table_info['source_system'],
                    'table_name': table_info['table_name'],
                    'error': str(e)
                })
        
        # Generate master report
        if generate_reports and results['processed']:
            self._generate_master_report(results)
        
        self.logger.info(f"Gold processing complete: {len(results['processed'])} succeeded, "
                        f"{len(results['failed'])} failed")
        
        return results
    
    def _load_silver_table(self, source_system: str, 
                          table_name: str) -> Optional[pd.DataFrame]:
        """Load a table from Silver layer."""
        table_path = self.silver_path / source_system / table_name
        
        if not table_path.exists():
            self.logger.error(f"Table path does not exist: {table_path}")
            return None
        
        # Find parquet files
        parquet_files = list(table_path.glob("*.parquet"))
        
        if not parquet_files:
            self.logger.error(f"No parquet files found in: {table_path}")
            return None
        
        # Load and combine all parquet files
        dfs = []
        for file in parquet_files:
            try:
                df = pd.read_parquet(file)
                dfs.append(df)
            except Exception as e:
                self.logger.error(f"Failed to load {file}: {str(e)}")
        
        if not dfs:
            return None
        
        # Combine all dataframes
        combined_df = pd.concat(dfs, ignore_index=True)
        return combined_df
    
    def _create_gold_dataset(self, df: pd.DataFrame, 
                            eda_results: Dict[str, Any]) -> pd.DataFrame:
        """
        Create business-ready Gold dataset with additional features.
        
        Args:
            df: Input DataFrame from Silver
            eda_results: EDA analysis results
            
        Returns:
            Enhanced DataFrame for Gold layer
        """
        gold_df = df.copy()
        
        # Add gold layer metadata
        gold_df['_gold_processed_at'] = datetime.now()
        gold_df['_quality_score'] = eda_results['data_quality']['overall_quality_score']
        
        # Remove technical metadata columns from silver if they exist
        tech_cols = [col for col in gold_df.columns 
                    if col.startswith('_bronze_') or col.startswith('_silver_')]
        
        # Keep only essential metadata
        essential_metadata = ['_bronze_ingested_at', '_silver_processed_at', 
                             '_silver_quality_passed']
        cols_to_drop = [col for col in tech_cols if col not in essential_metadata]
        
        if cols_to_drop:
            gold_df = gold_df.drop(columns=cols_to_drop)
        
        return gold_df
    
    def _save_gold_data(self, df: pd.DataFrame, 
                       source_system: str, table_name: str) -> str:
        """Save Gold layer data."""
        output_dir = self.gold_path / source_system / table_name
        output_dir.mkdir(parents=True, exist_ok=True)
        
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = output_dir / f"{table_name}_gold_{timestamp}.parquet"
        
        df.to_parquet(output_file, index=False, engine='pyarrow')
        
        return str(output_file)
    
    def _generate_master_report(self, results: Dict[str, Any]) -> None:
        """Generate a master report summarizing all tables."""
        master_report_path = self.gold_path / "eda_reports" / "MASTER_REPORT.json"
        
        with open(master_report_path, 'w') as f:
            json.dump(results, f, indent=2, default=str)
        
        self.logger.info(f"Master report saved: {master_report_path}")
    
    def get_table_summary(self, source_system: str, 
                         table_name: str) -> Optional[Dict[str, Any]]:
        """
        Get a quick summary of a table without full processing.
        
        Args:
            source_system: Source system identifier
            table_name: Table name
            
        Returns:
            Dictionary with table summary
        """
        df = self._load_silver_table(source_system, table_name)
        if df is None:
            return None
        
        return {
            'source_system': source_system,
            'table_name': table_name,
            'row_count': len(df),
            'column_count': len(df.columns),
            'columns': list(df.columns),
            'memory_mb': df.memory_usage(deep=True).sum() / (1024 * 1024),
            'sample_data': df.head(5).to_dict('records')
        }


def main():
    """Main entry point for Gold layer processing."""
    aggregator = GoldAggregator()
    
    # List all tables
    print("\n" + "="*80)
    print("GOLD LAYER - TABLE DISCOVERY")
    print("="*80)
    
    table_list = aggregator.list_all_tables()
    print(f"\nFound {table_list['total_tables']} tables:")
    for source, tables in table_list['by_source'].items():
        print(f"\n{source}:")
        for table in tables:
            print(f"  - {table}")
    
    # Process all tables
    print("\n" + "="*80)
    print("GOLD LAYER - EDA PROCESSING")
    print("="*80)
    
    results = aggregator.process_all_tables(
        save_gold=True,
        generate_reports=True
    )
    
    print(f"\nProcessing Complete!")
    print(f"  Succeeded: {len(results['processed'])}")
    print(f"  Failed: {len(results['failed'])}")
    
    if results['processed']:
        print("\nQuality Scores:")
        for result in results['processed']:
            print(f"  {result['source_system']}.{result['table_name']}: "
                  f"{result['eda_summary']['quality_score']:.1f}/100")


if __name__ == "__main__":
    main()

"""
Gold Layer EDA Service
Generates comprehensive EDA reports from Silver layer data
"""

import sys
from pathlib import Path
import io
import pandas as pd
import logging
from datetime import datetime
from typing import Dict, Optional
import json
import traceback

# Add data lakehouse path for GoldEDAEngine
lakehouse_path = Path(__file__).parent.parent.parent / "data lakehouse" / "syniq_project"
sys.path.insert(0, str(lakehouse_path))

try:
    from gold_eda_engine import GoldEDAEngine
except ImportError as e:
    logging.warning(f"Could not import GoldEDAEngine: {e}")
    GoldEDAEngine = None

from minio_utils import MinIOClient

logger = logging.getLogger(__name__)

class GoldEDAService:
    """Service for generating and managing Gold layer EDA reports"""
    
    def __init__(self, minio_client: MinIOClient):
        """
        Initialize the service.
        
        Args:
            minio_client: MinIO client for data operations
        """
        self.minio = minio_client
        
        # Note: Buckets are ensured during storage initialization
        # No need to check again here
        
        # Initialize EDA engine if available
        if GoldEDAEngine:
            self.eda_engine = GoldEDAEngine()
            logger.info("Initialized GoldEDAEngine")
        else:
            self.eda_engine = None
            logger.warning("GoldEDAEngine not available - using mock analysis")
    
    def generate_eda_report(self, source: str, entity: str) -> Dict:
        """
        Generate comprehensive EDA report from Silver layer data.
        
        Args:
            source: Source system name (e.g., 'postgres')
            entity: Entity/table name
        
        Returns:
            EDA report dictionary
        """
        try:
            logger.info(f"Generating EDA report for {source}.{entity}")
            
            # Read Silver data
            df = self._read_silver_data(source, entity)
            if df is None or df.empty:
                raise ValueError(f"No Silver data found for {source}.{entity}")
            
            logger.info(f"Loaded {len(df)} rows from Silver layer")
            
            # Generate EDA report
            if self.eda_engine:
                report = self.eda_engine.analyze_dataframe(
                    df=df,
                    table_name=entity,
                    source_system=source
                )
            else:
                # Basic mock analysis if engine not available
                report = self._generate_basic_analysis(df, source, entity)
            
            # Save report to Gold layer
            report_path = self._save_report_to_gold(report, source, entity)
            report['report_path'] = report_path
            
            logger.info(f"EDA report generated and saved to {report_path}")
            
            return {
                "success": True,
                "source": source,
                "entity": entity,
                "table_name": f"{source}.{entity}",
                "report": report,
                "generated_at": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error generating EDA report: {e}")
            logger.error(traceback.format_exc())
            return {
                "success": False,
                "error": str(e),
                "source": source,
                "entity": entity
            }
    
    def get_latest_report(self, source: str, entity: str) -> Optional[Dict]:
        """
        Get the latest EDA report for a table.
        
        Args:
            source: Source system name
            entity: Entity/table name
        
        Returns:
            EDA report or None if not found
        """
        try:
            # Try to get existing report from Gold layer
            report = self.minio.get_eda_report(source, entity)
            
            if report:
                logger.info(f"Retrieved existing EDA report for {source}.{entity}")
                return report
            else:
                logger.info(f"No existing report found for {source}.{entity}")
                return None
                
        except Exception as e:
            logger.error(f"Error retrieving EDA report: {e}")
            return None
    
    def list_available_tables(self) -> list:
        """
        List all Silver tables available for EDA.
        
        Returns:
            List of table dictionaries
        """
        try:
            tables = self.minio.list_tables("silver")
            return tables
        except Exception as e:
            logger.error(f"Error listing Silver tables: {e}")
            return []
    
    def _read_silver_data(self, source: str, entity: str) -> Optional[pd.DataFrame]:
        """Read data from Silver layer in MinIO"""
        try:
            df = self.minio.read_parquet("silver", source, entity)
            return df
        except Exception as e:
            logger.error(f"Error reading Silver data: {e}")
            return None
    
    def _generate_basic_analysis(self, df: pd.DataFrame, source: str, 
                                 entity: str) -> Dict:
        """Generate basic analysis when GoldEDAEngine is not available"""
        logger.info("Using basic analysis (GoldEDAEngine not available)")
        
        # Basic info
        basic_info = {
            'row_count': len(df),
            'column_count': len(df.columns),
            'columns': list(df.columns),
            'dtypes': df.dtypes.astype(str).to_dict()
        }
        
        # Data quality
        total_cells = df.shape[0] * df.shape[1]
        missing_cells = df.isnull().sum().sum()
        
        data_quality = {
            'completeness_score': ((total_cells - missing_cells) / total_cells * 100) if total_cells > 0 else 0,
            'missing_value_count': int(missing_cells),
            'duplicate_rows': int(df.duplicated().sum()),
            'overall_quality_score': 85.0  # Mock score
        }
        
        # Column analysis
        column_analysis = {}
        for col in df.columns:
            col_data = df[col]
            analysis = {
                'dtype': str(col_data.dtype),
                'non_null_count': int(col_data.count()),
                'null_count': int(col_data.isnull().sum()),
                'unique_count': int(col_data.nunique())
            }
            
            if pd.api.types.is_numeric_dtype(col_data):
                analysis['type'] = 'numeric'
                analysis['min'] = float(col_data.min())
                analysis['max'] = float(col_data.max())
                analysis['mean'] = float(col_data.mean())
                analysis['median'] = float(col_data.median())
            else:
                analysis['type'] = 'categorical'
            
            column_analysis[col] = analysis
        
        return {
            'table_name': entity,
            'source_system': source,
            'analysis_timestamp': datetime.now().isoformat(),
            'basic_info': basic_info,
            'data_quality': data_quality,
            'column_analysis': column_analysis,
            'statistical_summary': {},
            'correlations': {},
            'distributions': {},
            'insights': ["Basic analysis - full EDA engine not loaded"]
        }
    
    def _save_report_to_gold(self, report: Dict, source: str, entity: str) -> str:
        """Save EDA report to Gold layer in MinIO"""
        try:
            # Use the minio_utils method
            report_path = self.minio.save_eda_report(source, entity, report)
            return report_path
            
        except Exception as e:
            logger.error(f"Error saving EDA report: {e}")
            raise
    
    def get_visualization_data(self, source: str, entity: str) -> Optional[Dict]:
        """
        Get data formatted for frontend visualizations.
        
        Args:
            source: Source system name
            entity: Entity/table name
        
        Returns:
            Visualization-ready data
        """
        try:
            # Get or generate report
            report = self.get_latest_report(source, entity)
            
            if not report:
                # Generate new report
                result = self.generate_eda_report(source, entity)
                if not result['success']:
                    return None
                report = result['report']
            
            # Format for frontend
            viz_data = {
                'table_name': f"{source}.{entity}",
                'metrics': {
                    'total_rows': report.get('basic_info', {}).get('row_count', 0),
                    'total_columns': report.get('basic_info', {}).get('column_count', 0),
                    'quality_score': report.get('data_quality', {}).get('overall_quality_score', 0),
                    'completeness': report.get('data_quality', {}).get('completeness_score', 0),
                    'duplicates': report.get('data_quality', {}).get('duplicate_rows', 0),
                    'missing_values': report.get('data_quality', {}).get('missing_value_count', 0)
                },
                'columns': report.get('column_analysis', {}),
                'insights': report.get('insights', []),
                'correlations': report.get('correlations', {}),
                'distributions': report.get('distributions', {})
            }
            
            return viz_data
            
        except Exception as e:
            logger.error(f"Error getting visualization data: {e}")
            return None    
    def analyze_schema_structure(self, source: str, entity: str) -> Optional[Dict]:
        """
        Analyze table schema to detect dimensional model structure.
        
        Args:
            source: Source system name
            entity: Entity/table name
        
        Returns:
            Schema analysis with fact/dimension classification
        """
        try:
            # Read Silver data
            df = self._read_silver_data(source, entity)
            if df is None or df.empty:
                raise ValueError(f"No Silver data found for {source}.{entity}")
            
            logger.info(f"Analyzing schema structure for {source}.{entity}")
            
            # Analyze columns
            columns_analysis = []
            primary_key_candidates = []
            foreign_key_candidates = []
            
            for col in df.columns:
                col_data = df[col]
                unique_ratio = col_data.nunique() / len(df) if len(df) > 0 else 0
                null_ratio = col_data.isnull().sum() / len(df) if len(df) > 0 else 0
                
                # Detect column type
                is_numeric = pd.api.types.is_numeric_dtype(col_data)
                is_datetime = pd.api.types.is_datetime64_any_dtype(col_data)
                is_id_field = '_id' in col.lower() or col.lower().endswith('id')
                
                col_info = {
                    'name': col,
                    'dtype': str(col_data.dtype),
                    'unique_count': int(col_data.nunique()),
                    'unique_ratio': float(unique_ratio),
                    'null_ratio': float(null_ratio),
                    'is_numeric': is_numeric,
                    'is_datetime': is_datetime,
                    'is_id_field': is_id_field
                }
                
                columns_analysis.append(col_info)
                
                # Primary key detection (high uniqueness, low nulls)
                if unique_ratio > 0.95 and null_ratio < 0.01:
                    primary_key_candidates.append({
                        'column': col,
                        'confidence': float(unique_ratio * (1 - null_ratio))
                    })
                
                # Foreign key detection (ID fields with moderate cardinality)
                if is_id_field and unique_ratio < 0.5:
                    foreign_key_candidates.append({
                        'column': col,
                        'cardinality': int(col_data.nunique()),
                        'references_table': col.replace('_id', '').replace('id', '')
                    })
            
            # Classify table as fact or dimension
            # Heuristics:
            # - High row count + many foreign keys = FACT table
            # - Low row count + high uniqueness = DIMENSION table
            # - Presence of measure columns (numeric, not IDs) = FACT
            
            row_count = len(df)
            fk_count = len(foreign_key_candidates)
            numeric_measure_count = sum(1 for c in columns_analysis 
                                       if c['is_numeric'] and not c['is_id_field'])
            
            if fk_count >= 2 and numeric_measure_count >= 2:
                table_type = 'FACT'
                confidence = 0.8
            elif row_count < 10000 and len(primary_key_candidates) > 0:
                table_type = 'DIMENSION'
                confidence = 0.7
            else:
                # Default to fact if transactional nature
                table_type = 'FACT' if row_count > 1000 else 'DIMENSION'
                confidence = 0.5
            
            # Build schema structure
            schema_structure = {
                'table_name': entity,
                'source': source,
                'full_name': f"{source}.{entity}",
                'row_count': row_count,
                'column_count': len(df.columns),
                'table_type': table_type,
                'classification_confidence': confidence,
                'columns': columns_analysis,
                'primary_keys': primary_key_candidates,
                'foreign_keys': foreign_key_candidates,
                'schema_insights': {
                    'has_primary_key': len(primary_key_candidates) > 0,
                    'has_foreign_keys': len(foreign_key_candidates) > 0,
                    'numeric_measures': numeric_measure_count,
                    'is_normalized': unique_ratio > 0.8 if primary_key_candidates else False
                }
            }
            
            # Detect schema type based on structure
            if table_type == 'FACT' and fk_count > 0:
                if all(fk['cardinality'] < 100 for fk in foreign_key_candidates[:3]):
                    schema_type = 'Star'  # Denormalized dimensions
                elif any(fk['cardinality'] > 1000 for fk in foreign_key_candidates):
                    schema_type = 'Snowflake'  # Normalized dimensions
                else:
                    schema_type = 'Star'
            else:
                schema_type = 'Star'  # Default
            
            schema_structure['detected_schema_type'] = schema_type
            
            logger.info(f"Schema analysis complete: {table_type} table with {fk_count} foreign keys")
            
            return schema_structure
            
        except Exception as e:
            logger.error(f"Error analyzing schema structure: {e}")
            logger.error(traceback.format_exc())
            return None
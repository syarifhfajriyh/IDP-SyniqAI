"""
Generalized Quality Rule Generator
Introspects table schemas from MinIO Bronze and generates appropriate quality rules
Works for ANY table by analyzing column types and data patterns
"""
import duckdb
import logging
from typing import List, Dict, Any
from datetime import datetime
import uuid
import sys

from database import db_manager, rules_repo, initialize_database
from minio_utils import MinIOClient

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class QualityRuleGenerator:
    """Generates quality rules based on table schema analysis"""
    
    def __init__(self):
        self.minio_client = MinIOClient()
    
    def configure_duckdb_s3(self, conn: duckdb.DuckDBPyConnection):
        """Configure DuckDB to connect to MinIO S3"""
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        conn.execute("SET s3_endpoint='localhost:9000';")
        conn.execute("SET s3_use_ssl=false;")
        conn.execute("SET s3_access_key_id='admin';")
        conn.execute("SET s3_secret_access_key='password123';")
        conn.execute("SET s3_url_style='path';")
    
    def introspect_table_schema(
        self,
        table_name: str,
        domain: str = "finance",
        source: str = "postgres"
    ) -> List[Dict[str, Any]]:
        """Get table schema from MinIO Bronze data"""
        
        # Normalize source name
        normalized_source = source.lower().replace('sql', '')
        normalized_domain = domain.lower()
        
        # Construct S3 path
        s3_path = f"s3://syniqai-bronze/{normalized_domain}/{normalized_source}/{table_name}/*.parquet"
        
        logger.info(f"📊 Analyzing schema for: {table_name}")
        logger.info(f"📂 S3 Path: {s3_path}")
        
        # Initialize DuckDB
        conn = duckdb.connect(database=':memory:')
        self.configure_duckdb_s3(conn)
        
        try:
            # Read schema and sample data
            query = f"""
                SELECT * FROM '{s3_path}' LIMIT 100
            """
            result = conn.execute(query).fetchdf()
            
            # Get column info
            schema_info = []
            for col_name in result.columns:
                col_type = str(result[col_name].dtype)
                
                # Analyze column characteristics
                null_count = result[col_name].isnull().sum()
                total_count = len(result)
                null_percentage = (null_count / total_count) * 100 if total_count > 0 else 0
                
                # Check for uniqueness (for potential ID columns)
                unique_count = result[col_name].nunique()
                is_likely_id = unique_count == total_count and col_name.lower().endswith('id')
                
                # Get min/max for numeric columns
                min_val = None
                max_val = None
                if col_type in ['int64', 'float64', 'int32', 'float32']:
                    try:
                        min_val = result[col_name].min()
                        max_val = result[col_name].max()
                    except:
                        pass
                
                schema_info.append({
                    'column_name': col_name,
                    'data_type': col_type,
                    'null_percentage': null_percentage,
                    'unique_count': unique_count,
                    'is_likely_id': is_likely_id,
                    'min_value': min_val,
                    'max_value': max_val
                })
            
            logger.info(f"✓ Found {len(schema_info)} columns in {table_name}")
            return schema_info
            
        except Exception as e:
            logger.error(f"Error introspecting schema: {e}")
            return []
        finally:
            conn.close()
    
    def generate_rules_for_table(
        self,
        table_name: str,
        domain: str = "finance",
        source: str = "postgres"
    ) -> List[Dict[str, Any]]:
        """Generate quality rules based on schema analysis"""
        
        schema_info = self.introspect_table_schema(table_name, domain, source)
        
        if not schema_info:
            logger.warning(f"No schema info available for {table_name}")
            return []
        
        rules = []
        priority = 5  # Start with medium priority (1-10 allowed)
        
        for col_info in schema_info:
            col_name = col_info['column_name']
            col_type = col_info['data_type']
            
            # 1. COMPLETENESS RULES - Null checks
            # Create null check for columns with low null percentage (likely required fields)
            if col_info['null_percentage'] < 10:  # Less than 10% nulls = probably required
                rules.append({
                    'rule_name': f'Completeness: {col_name} Not Null',
                    'domain': domain,
                    'category': 'data_quality',
                    'rule_type': 'not_null',
                    'description': f'Ensures {col_name} column contains no null values',
                    'target_table': table_name,
                    'target_columns': [col_name],
                    'condition_expression': f'{col_name} IS NOT NULL',
                    'severity': 'CRITICAL' if col_info.get('is_likely_id') else 'HIGH',
                    'action': 'quarantine_row',
                    'execution_priority': 5,
                    'created_by': 'auto_generator'
                })
                
            
            # 2. UNIQUENESS RULES - For ID columns
            if col_info.get('is_likely_id'):
                rules.append({
                    'rule_name': f'Uniqueness: {col_name} Unique Values',
                    'domain': domain,
                    'category': 'data_quality',
                    'rule_type': 'unique',
                    'description': f'Ensures {col_name} contains only unique values (no duplicates)',
                    'target_table': table_name,
                    'target_columns': [col_name],
                    'condition_expression': f'{col_name} IS NOT NULL',  # Will be checked with GROUP BY in executor
                    'severity': 'CRITICAL',
                    'action': 'quarantine_row',
                    'execution_priority': 5,
                    'created_by': 'auto_generator'
                })
                
            
            # 3. VALIDITY RULES - Numeric ranges
            if col_type in ['int64', 'float64', 'int32', 'float32']:
                min_val = col_info.get('min_value')
                max_val = col_info.get('max_value')
                
                # For amount/value/price columns, ensure positive
                if any(keyword in col_name.lower() for keyword in ['amount', 'price', 'value', 'cost', 'balance']):
                    # Set reasonable upper limit (10x max observed value or 1 billion)
                    upper_limit = int(max_val * 10) if max_val and max_val > 0 else 1000000000
                    
                    rules.append({
                        'rule_name': f'Validity: {col_name} Positive Range',
                        'domain': domain,
                        'category': 'data_quality',
                        'rule_type': 'range_check',
                        'description': f'Ensures {col_name} is positive and within reasonable range',
                        'target_table': table_name,
                        'target_columns': [col_name],
                        'condition_expression': f'{col_name} > 0 AND {col_name} < {upper_limit}',
                        'severity': 'HIGH',
                        'action': 'quarantine_row',
                        'execution_priority': 5,
                        'created_by': 'auto_generator'
                    })
                    
            
            # 4. CONSISTENCY RULES - Date columns
            if 'date' in col_type.lower() or 'timestamp' in col_type.lower() or 'date' in col_name.lower():
                rules.append({
                    'rule_name': f'Consistency: {col_name} Not Future Date',
                    'domain': domain,
                    'category': 'data_quality',
                    'rule_type': 'range_check',
                    'description': f'Ensures {col_name} is not in the future',
                    'target_table': table_name,
                    'target_columns': [col_name],
                    'condition_expression': f"{col_name} <= CURRENT_DATE",
                    'severity': 'HIGH',
                    'action': 'quarantine_row',
                    'execution_priority': 5,
                    'created_by': 'auto_generator'
                })
                
            
            # 5. VALIDITY RULES - String format validation
            if col_type == 'object' or 'string' in col_type.lower():
                # Email validation
                if 'email' in col_name.lower():
                    rules.append({
                        'rule_name': f'Validity: {col_name} Email Format',
                        'domain': domain,
                        'category': 'data_quality',
                        'rule_type': 'regex_format',
                        'description': f'Ensures {col_name} contains valid email format',
                        'target_table': table_name,
                        'target_columns': [col_name],
                        'condition_expression': f"{col_name} ~ '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{{2,}}$'",
                        'severity': 'WARNING',
                        'action': 'log',
                        'execution_priority': 5,
                        'created_by': 'auto_generator'
                    })
                    
                
                # Status/Code columns - check for reasonable length
                if any(keyword in col_name.lower() for keyword in ['status', 'code', 'type']):
                    rules.append({
                        'rule_name': f'Validity: {col_name} Non-Empty',
                        'domain': domain,
                        'category': 'data_quality',
                        'rule_type': 'range_check',
                        'description': f'Ensures {col_name} is not empty',
                        'target_table': table_name,
                        'target_columns': [col_name],
                        'condition_expression': f"LENGTH({col_name}) > 0",
                        'severity': 'WARNING',
                        'action': 'quarantine_row',
                        'execution_priority': 5,
                        'created_by': 'auto_generator'
                    })
                    
        
        logger.info(f"✓ Generated {len(rules)} quality rules for {table_name}")
        return rules
    
    def insert_rules_to_database(self, rules: List[Dict[str, Any]]) -> int:
        """Insert generated rules into database"""
        inserted_count = 0
        
        for rule in rules:
            try:
                rule_id = rules_repo.create_rule(rule)
                logger.info(f"  ✓ Created: {rule['rule_name']}")
                inserted_count += 1
            except Exception as e:
                logger.error(f"  ✗ Failed to create rule '{rule['rule_name']}': {e}")
        
        return inserted_count
    
    def generate_and_insert_rules(
        self,
        table_name: str,
        domain: str = "finance",
        source: str = "postgres"
    ) -> Dict[str, Any]:
        """Main method: Generate and insert rules for a table"""
        
        logger.info(f"🎯 Generating quality rules for {domain}.{table_name}")
        
        # Generate rules
        rules = self.generate_rules_for_table(table_name, domain, source)
        
        if not rules:
            return {
                'success': False,
                'message': 'No rules generated',
                'rules_generated': 0
            }
        
        # Insert to database
        inserted_count = self.insert_rules_to_database(rules)
        
        return {
            'success': True,
            'table_name': table_name,
            'domain': domain,
            'rules_generated': len(rules),
            'rules_inserted': inserted_count
        }


def main():
    """CLI entry point"""
    
    # Initialize database
    if not initialize_database():
        logger.error("Failed to initialize database")
        return
    
    generator = QualityRuleGenerator()
    
    # You can specify tables via command line args
    if len(sys.argv) > 1:
        table_name = sys.argv[1]
        domain = sys.argv[2] if len(sys.argv) > 2 else "finance"
        source = sys.argv[3] if len(sys.argv) > 3 else "postgres"
        
        result = generator.generate_and_insert_rules(table_name, domain, source)
        print(f"\n{'='*70}")
        print(f"✓ Rules Generation Complete!")
        print(f"  Table: {result.get('table_name')}")
        print(f"  Domain: {result.get('domain')}")
        print(f"  Rules Generated: {result.get('rules_generated')}")
        print(f"  Rules Inserted: {result.get('rules_inserted')}")
        print(f"{'='*70}\n")
    else:
        # Generate for default table
        print("Usage: python generate_quality_rules.py <table_name> [domain] [source]")
        print("Example: python generate_quality_rules.py finance_transactions finance postgres")
        print("\nGenerating rules for default table: finance_transactions")
        
        result = generator.generate_and_insert_rules("finance_transactions", "finance", "postgres")
        print(f"\n{'='*70}")
        print(f"✓ Rules Generation Complete!")
        print(f"  Table: {result.get('table_name')}")
        print(f"  Domain: {result.get('domain')}")
        print(f"  Rules Generated: {result.get('rules_generated')}")
        print(f"  Rules Inserted: {result.get('rules_inserted')}")
        print(f"{'='*70}\n")


if __name__ == "__main__":
    main()

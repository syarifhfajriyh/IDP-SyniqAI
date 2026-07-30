"""
Database initialization script
Deploys PostgreSQL schema and loads domain rule templates
"""
import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import json
import logging
from pathlib import Path
import sys

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent.parent))

from api.config import config

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def create_database_if_not_exists():
    """Create the database if it doesn't exist"""
    try:
        # Connect to default postgres database
        conn = psycopg2.connect(
            host=config.postgres.host,
            port=config.postgres.port,
            database='postgres',
            user=config.postgres.user,
            password=config.postgres.password
        )
        conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
        cursor = conn.cursor()
        
        # Check if database exists
        cursor.execute(f"SELECT 1 FROM pg_database WHERE datname = '{config.postgres.database}'")
        exists = cursor.fetchone()
        
        if not exists:
            cursor.execute(f"CREATE DATABASE {config.postgres.database}")
            logger.info(f"✓ Created database: {config.postgres.database}")
        else:
            logger.info(f"✓ Database already exists: {config.postgres.database}")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"✗ Error creating database: {e}")
        return False


def deploy_schema():
    """Deploy the rules schema to PostgreSQL"""
    schema_file = Path(__file__).parent.parent / "data lakehouse" / "syniq_project" / "metadata" / "rules_schema.sql"
    
    if not schema_file.exists():
        logger.error(f"✗ Schema file not found: {schema_file}")
        return False
    
    try:
        conn = psycopg2.connect(
            host=config.postgres.host,
            port=config.postgres.port,
            database=config.postgres.database,
            user=config.postgres.user,
            password=config.postgres.password
        )
        cursor = conn.cursor()
        
        # Check if schema already deployed
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = 'public' AND table_name = 'rules_catalog'
        """)
        if cursor.fetchone()[0] > 0:
            logger.info("✓ Schema already deployed, skipping...")
            cursor.close()
            conn.close()
            return True
        
        # Read and execute schema
        with open(schema_file, 'r', encoding='utf-8') as f:
            schema_sql = f.read()
        
        cursor.execute(schema_sql)
        conn.commit()
        
        logger.info("✓ Schema deployed successfully")
        
        # Verify tables created
        cursor.execute("""
            SELECT table_name FROM information_schema.tables 
            WHERE table_schema = 'public' 
            ORDER BY table_name
        """)
        tables = cursor.fetchall()
        logger.info(f"✓ Created {len(tables)} tables:")
        for table in tables:
            logger.info(f"  - {table[0]}")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"✗ Error deploying schema: {e}")
        return False


def load_rule_templates():
    """Load domain rule templates into database"""
    metadata_dir = Path(__file__).parent.parent / "data lakehouse" / "syniq_project" / "metadata"
    
    template_files = {
        "finance": metadata_dir / "finance_rules_v1.json",
        "healthcare": metadata_dir / "healthcare_rules_v1.json",
        "general": metadata_dir / "general_rules_v1.json"
    }
    
    try:
        conn = psycopg2.connect(
            host=config.postgres.host,
            port=config.postgres.port,
            database=config.postgres.database,
            user=config.postgres.user,
            password=config.postgres.password
        )
        cursor = conn.cursor()
        
        # Check if templates already loaded
        cursor.execute("SELECT COUNT(*) FROM domain_rule_templates")
        if cursor.fetchone()[0] >= 3:
            logger.info("✓ Rule templates already loaded, skipping...")
            cursor.close()
            conn.close()
            return True
        
        for domain, template_file in template_files.items():
            if not template_file.exists():
                logger.warning(f"⚠ Template file not found: {template_file}")
                continue
            
            with open(template_file, 'r', encoding='utf-8') as f:
                template_data = json.load(f)
            
            # Check if template already exists
            cursor.execute("""
                SELECT template_id FROM domain_rule_templates 
                WHERE domain = %s AND template_name = %s
            """, (domain, template_data['template_name']))
            
            if cursor.fetchone():
                logger.info(f"✓ Template already exists: {domain} - {template_data['template_name']}")
                continue
            
            # Insert template
            cursor.execute("""
                INSERT INTO domain_rule_templates (
                    domain, template_name, version, description, default_rules
                ) VALUES (%s, %s, %s, %s, %s)
            """, (
                domain,
                template_data['template_name'],
                template_data['version'],
                template_data['description'],
                json.dumps(template_data['rules'])
            ))
            
            logger.info(f"✓ Loaded template: {domain} - {template_data['template_name']} ({len(template_data['rules'])} rules)")
        
        conn.commit()
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"✗ Error loading templates: {e}")
        return False


def activate_default_rules():
    """Activate default rules from templates"""
    try:
        conn = psycopg2.connect(
            host=config.postgres.host,
            port=config.postgres.port,
            database=config.postgres.database,
            user=config.postgres.user,
            password=config.postgres.password
        )
        cursor = conn.cursor()
        
        # For each domain template, insert rules into rules_catalog
        cursor.execute("SELECT domain, template_name, default_rules FROM domain_rule_templates")
        templates = cursor.fetchall()
        
        rules_inserted = 0
        for domain, template_name, default_rules in templates:
            # default_rules is already parsed by psycopg2 as a list
            rules = default_rules if isinstance(default_rules, list) else json.loads(default_rules)
            
            for rule_data in rules:
                try:
                    # Check if rule already exists
                    rule_code = rule_data.get('rule_id', rule_data.get('rule_code', f'{domain}_{len(rules)}'))
                    cursor.execute("""
                        SELECT rule_id FROM rules_catalog 
                        WHERE domain = %s AND rule_code = %s
                    """, (domain, rule_code))
                    
                    if cursor.fetchone():
                        continue
                    
                    # Insert rule
                    cursor.execute("""
                        INSERT INTO rules_catalog (
                            rule_name, rule_code, domain, category, rule_type,
                            description, source_table, target_column,
                            condition_expression, severity, action,
                            execution_order, status, created_by
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                        )
                    """, (
                        rule_data.get('rule_name', 'unnamed_rule'),
                        rule_code,
                        domain,
                        rule_data.get('category', 'general'),
                        rule_data.get('rule_type', 'validation'),
                        rule_data.get('description', ''),
                        rule_data.get('target_table', '*'),
                        ','.join(rule_data.get('target_columns', ['*'])) if 'target_columns' in rule_data else '*',
                        json.dumps(rule_data.get('condition', {})),
                        rule_data.get('severity', 'MEDIUM'),
                        rule_data.get('action', 'flag'),
                        rule_data.get('execution_order', 100),
                        'active' if rule_data.get('is_active', True) else 'inactive',
                        'system'
                    ))
                    rules_inserted += 1
                except Exception as e:
                    logger.error(f"Error inserting rule: {e} - Rule data: {rule_data}")
        
        conn.commit()
        logger.info(f"✓ Activated {rules_inserted} default rules")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"✗ Error activating rules: {e}")
        return False


def verify_installation():
    """Verify database installation"""
    try:
        conn = psycopg2.connect(
            host=config.postgres.host,
            port=config.postgres.port,
            database=config.postgres.database,
            user=config.postgres.user,
            password=config.postgres.password
        )
        cursor = conn.cursor()
        
        # Count tables
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        table_count = cursor.fetchone()[0]
        
        # Count rules
        cursor.execute("SELECT COUNT(*) FROM rules_catalog")
        rules_count = cursor.fetchone()[0]
        
        # Count templates
        cursor.execute("SELECT COUNT(*) FROM domain_rule_templates")
        templates_count = cursor.fetchone()[0]
        
        logger.info("\n=== Installation Summary ===")
        logger.info(f"✓ Tables created: {table_count}")
        logger.info(f"✓ Rule templates: {templates_count}")
        logger.info(f"✓ Active rules: {rules_count}")
        
        # Show rules by domain
        cursor.execute("""
            SELECT domain, COUNT(*) as count 
            FROM rules_catalog 
            GROUP BY domain 
            ORDER BY domain
        """)
        logger.info("\nRules by domain:")
        for domain, count in cursor.fetchall():
            logger.info(f"  - {domain}: {count} rules")
        
        cursor.close()
        conn.close()
        return True
    except Exception as e:
        logger.error(f"✗ Verification failed: {e}")
        return False


def main():
    """Main initialization process"""
    logger.info("=== SyniqAI Database Initialization ===\n")
    
    steps = [
        ("Creating database", create_database_if_not_exists),
        ("Deploying schema", deploy_schema),
        ("Loading rule templates", load_rule_templates),
        ("Activating default rules", activate_default_rules),
        ("Verifying installation", verify_installation)
    ]
    
    for step_name, step_func in steps:
        logger.info(f"\n{step_name}...")
        if not step_func():
            logger.error(f"\n✗ Initialization failed at: {step_name}")
            return False
    
    logger.info("\n✓ Database initialization completed successfully!")
    logger.info("\nNext steps:")
    logger.info("1. Run: cd gui/api && python database.py  # Test connection")
    logger.info("2. Run: cd gui/api && python storage.py   # Test MinIO")
    logger.info("3. Run: cd gui && streamlit run app.py    # Launch UI")
    
    return True


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

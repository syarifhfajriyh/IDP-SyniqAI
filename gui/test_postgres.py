"""Direct PostgreSQL connection test"""
import psycopg2
import os
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(env_path)

print(f"Loading config from: {env_path}")
print(f"\nTesting PostgreSQL connection...")
print(f"Host: {os.getenv('POSTGRES_HOST', 'localhost')}:{os.getenv('POSTGRES_PORT', '5432')}")
print(f"Database: {os.getenv('POSTGRES_DB', 'syniqai_metadata')}")
print(f"User: {os.getenv('POSTGRES_USER', 'syniqai_user')}")

try:
    # Try to connect
    conn = psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "localhost"),
        port=os.getenv("POSTGRES_PORT", "5432"),
        database=os.getenv("POSTGRES_DB", "syniqai_metadata"),
        user=os.getenv("POSTGRES_USER", "syniqai_user"),
        password=os.getenv("POSTGRES_PASSWORD", "syniqai_password"),
        connect_timeout=3
    )
    
    cursor = conn.cursor()
    
    # Get PostgreSQL version
    cursor.execute("SELECT version();")
    version = cursor.fetchone()[0]
    
    print(f"\n✓ PostgreSQL Connected Successfully!")
    print(f"✓ Database: {os.getenv('POSTGRES_DB')}")
    print(f"✓ User: {os.getenv('POSTGRES_USER')}")
    print(f"✓ PostgreSQL version: {version.split(',')[0]}")
    
    # Check if tables exist
    cursor.execute("""
        SELECT COUNT(*) FROM information_schema.tables 
        WHERE table_schema = 'public'
    """)
    table_count = cursor.fetchone()[0]
    
    if table_count > 0:
        print(f"✓ Found {table_count} tables in database")
    else:
        print(f"⚠ No tables found - run 'python init_database.py' to initialize schema")
    
    cursor.close()
    conn.close()
    
    print("\n✓ PostgreSQL is ready!")
    
except psycopg2.OperationalError as e:
    error_msg = str(e)
    print(f"\n✗ PostgreSQL connection failed!")
    
    if "password authentication failed" in error_msg:
        print("\n❌ Authentication Error")
        print("   User 'syniqai_user' may not exist or password is incorrect")
        print("\n📋 To fix in pgAdmin:")
        print("   1. Open pgAdmin Query Tool")
        print("   2. Run the SQL from 'setup_postgres.sql'")
        print("   3. Or see PGADMIN_SETUP.md for step-by-step guide")
        
    elif "database" in error_msg and "does not exist" in error_msg:
        print("\n❌ Database Does Not Exist")
        print("   Database 'syniqai_metadata' needs to be created")
        print("\n📋 To fix in pgAdmin:")
        print("   1. Open pgAdmin Query Tool")
        print("   2. Run: CREATE DATABASE syniqai_metadata;")
        print("   3. Or run the SQL from 'setup_postgres.sql'")
        
    elif "could not connect" in error_msg or "Connection refused" in error_msg:
        print("\n❌ PostgreSQL Server Not Running")
        print("   Check that PostgreSQL service is started")
        print("\n📋 To fix:")
        print("   - Windows: Services → PostgreSQL → Start")
        print("   - Or check if PostgreSQL is installed")
        
    else:
        print(f"\nError details: {error_msg}")
        
except Exception as e:
    print(f"\n✗ Unexpected error: {e}")

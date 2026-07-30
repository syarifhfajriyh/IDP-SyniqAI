"""
Quick service check for PostgreSQL and MinIO
"""
import sys
from pathlib import Path

# Add parent directory to path
sys.path.append(str(Path(__file__).parent))

from api.config import config
import psycopg2
from minio import Minio

def check_postgresql():
    """Check if PostgreSQL is accessible"""
    try:
        # Try to connect to postgres default database first
        conn = psycopg2.connect(
            host=config.postgres.host,
            port=config.postgres.port,
            database='postgres',
            user=config.postgres.user,
            password=config.postgres.password,
            connect_timeout=3
        )
        conn.close()
        print("✓ PostgreSQL: Connected successfully")
        print(f"  Host: {config.postgres.host}:{config.postgres.port}")
        return True
    except psycopg2.OperationalError as e:
        error_msg = str(e)
        if "password authentication failed" in error_msg:
            print("✗ PostgreSQL: Authentication failed")
            print(f"  Check user '{config.postgres.user}' exists and password is correct")
            print(f"  Run: psql -U postgres -c \"CREATE USER {config.postgres.user} WITH PASSWORD '{config.postgres.password}' CREATEDB;\"")
        elif "could not connect" in error_msg or "Connection refused" in error_msg:
            print("✗ PostgreSQL: Server not running or not accessible")
            print(f"  Host: {config.postgres.host}:{config.postgres.port}")
            print("  Install: https://www.postgresql.org/download/")
            print("  Or use Docker: docker run -d -p 5432:5432 -e POSTGRES_PASSWORD=postgres postgres")
        else:
            print(f"✗ PostgreSQL: {error_msg}")
        return False
    except Exception as e:
        print(f"✗ PostgreSQL: Unexpected error - {e}")
        return False


def check_minio():
    """Check if MinIO is accessible"""
    try:
        client = Minio(
            config.minio.endpoint,
            access_key=config.minio.access_key,
            secret_key=config.minio.secret_key,
            secure=config.minio.secure
        )
        
        # Try to list buckets
        buckets = list(client.list_buckets())
        print("✓ MinIO: Connected successfully")
        print(f"  Endpoint: {config.minio.endpoint}")
        print(f"  Buckets: {len(buckets)} found")
        for bucket in buckets:
            print(f"    - {bucket.name}")
        return True
    except Exception as e:
        error_msg = str(e)
        if "Connection refused" in error_msg or "Failed to establish" in error_msg:
            print("✗ MinIO: Server not running or not accessible")
            print(f"  Endpoint: {config.minio.endpoint}")
            print("  Install: https://min.io/download")
            print("  Start server: minio server C:\\minio-data --console-address \":9001\"")
            print("  Or use Docker: docker run -d -p 9000:9000 -p 9001:9001 minio/minio server /data --console-address \":9001\"")
        elif "Invalid" in error_msg or "Access Denied" in error_msg:
            print("✗ MinIO: Authentication failed")
            print(f"  Check access key: {config.minio.access_key}")
            print("  Default credentials: minioadmin / minioadmin")
        else:
            print(f"✗ MinIO: {error_msg}")
        return False


def main():
    """Run all service checks"""
    print("=== SyniqAI Service Connectivity Check ===\n")
    
    postgres_ok = check_postgresql()
    print()
    minio_ok = check_minio()
    
    print("\n=== Summary ===")
    if postgres_ok and minio_ok:
        print("✓ All services are ready!")
        print("\nNext steps:")
        print("1. Run: cd gui && python init_database.py  # Initialize database")
        print("2. Upload sample data to MinIO Bronze layer")
        print("3. Run: streamlit run app.py  # Launch UI")
        return True
    else:
        print("✗ Some services are not accessible")
        print("\nPlease fix the issues above and try again.")
        print("See PHASE2_SETUP.md for detailed setup instructions.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

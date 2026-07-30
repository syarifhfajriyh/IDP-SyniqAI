"""Check if Bronze tables exist in MinIO"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from minio_utils import MinIOClient

def main():
    try:
        print("Connecting to MinIO...")
        client = MinIOClient()
        
        print("\n" + "="*60)
        print("BRONZE LAYER TABLES")
        print("="*60)
        
        tables = client.list_tables('bronze')
        
        if not tables:
            print("\n⚠️  No Bronze tables found!")
            print("\nTo add data:")
            print("1. Go to Bronze layer in the GUI")
            print("2. Click 'New Ingestion Job'")
            print("3. Configure your database connection")
            print("4. Select tables to ingest")
            print("5. Run ingestion")
        else:
            print(f"\n✅ Found {len(tables)} table(s):\n")
            for table in tables:
                print(f"  • {table['source']}/{table['entity']}")
                
        print("\n" + "="*60)
        print("SILVER LAYER TABLES")
        print("="*60)
        
        silver_tables = client.list_tables('silver')
        
        if not silver_tables:
            print("\n⚠️  No Silver tables found!")
        else:
            print(f"\n✅ Found {len(silver_tables)} table(s):\n")
            for table in silver_tables:
                print(f"  • {table['source']}/{table['entity']}")
                
        return len(tables) > 0
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        print("\nMake sure MinIO is running:")
        print("  - Default endpoint: localhost:9000")
        print("  - Default credentials: admin / password123")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

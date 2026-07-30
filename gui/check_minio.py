import sys
sys.path.insert(0, 'api')
from minio_utils import MinIOClient

m = MinIOClient()
print("\n=== MinIO Bronze Tables ===")
tables = m.list_tables('bronze')
for t in tables:
    print(f"  Source: {t['source']}, Entity: {t['entity']}, Path: {t.get('minio_path', 'N/A')}")

print(f"\nTotal: {len(tables)} tables")

# List actual objects in syniqai-bronze bucket
print("\n=== Actual MinIO Objects in syniqai-bronze ===")
objects = list(m.client.list_objects('syniqai-bronze', recursive=True))
for obj in objects[:20]:  # First 20 objects
    print(f"  {obj.object_name}")

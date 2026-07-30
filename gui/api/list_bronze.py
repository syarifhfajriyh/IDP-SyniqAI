"""List MinIO bronze objects"""
import storage

storage.initialize_storage()

objects = storage.storage_manager.list_objects('bronze')
print(f"Found {len(objects)} objects in bronze bucket:\n")
for obj in objects[:15]:
    print(f"  {obj['object_name']} ({obj['size']} bytes)")

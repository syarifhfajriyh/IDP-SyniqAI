# generate_video_outputs.py
import os
import json
import zipfile
from datetime import datetime, timezone
from dotenv import load_dotenv

from pymongo import MongoClient
from bson import ObjectId
from gridfs import GridFS

# ---------------- CONFIG ----------------
# Load environment configuration - try multiple paths
script_dir = os.path.dirname(os.path.abspath(__file__))
env_paths = [
    os.path.join(script_dir, 'on_prem.env'),
    'on_prem.env',
    os.path.join(os.getcwd(), 'on_prem.env')
]
for env_path in env_paths:
    if os.path.exists(env_path):
        load_dotenv(env_path)
        break

# Build MongoDB URI - supports both Atlas (mongodb+srv://) and On-Prem (mongodb://)
MONGO_URI = os.getenv('MONGO_URI')  # Atlas or custom connection string
DB_NAME = os.getenv('MONGO_DB', 'media_db')

if not MONGO_URI:
    # Fall back to On-Prem individual variables (backward compatibility)
    MONGO_HOST = os.getenv('MONGO_HOST', 'localhost')
    MONGO_PORT = os.getenv('MONGO_PORT', '27017')
    MONGO_USER = os.getenv('MONGO_USER')
    MONGO_PASS = os.getenv('MONGO_PASS')
    MONGO_AUTH_SOURCE = os.getenv('MONGO_AUTH_SOURCE', 'admin')
    
    if MONGO_USER and MONGO_PASS:
        MONGO_URI = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/?authSource={MONGO_AUTH_SOURCE}"
    else:
        MONGO_URI = f"mongodb://{MONGO_HOST}:{MONGO_PORT}/"

VIDEOS_METADATA_COLLECTION = "videos_metadata"
STATE_COLLECTION = "ingestion_state"
CONNECTOR_VERSION = "1.0.0"

OUTPUT_ROOT = os.path.join("output", "data_metadata")
# ---------------------------------------

def safe_json(v):
    if isinstance(v, ObjectId):
        return str(v)
    if isinstance(v, datetime):
        return v.replace(tzinfo=timezone.utc).isoformat().replace("+00:00", "Z")
    return v

def generate_video_outputs():
    client = MongoClient(MONGO_URI)
    db = client[DB_NAME]

    meta_col = db[VIDEOS_METADATA_COLLECTION]
    state_col = db[STATE_COLLECTION]
    fs = GridFS(db)

    st = state_col.find_one({"_id": "videos"}) or {}
    run_id = st.get("last_run_id")

    if not run_id:
        print("❌ No last_run_id found. Run upload first.")
        return

    # ✅ Check if this run_id was already exported
    exported_runs = st.get("exported_run_ids", [])
    if run_id in exported_runs:
        print(f"⏭️ run_id {run_id} already exported. Skipping.")
        return

    # ✅ Export ONLY this run
    meta_docs = list(meta_col.find({"run_id": run_id}))
    print("Export run_id =", run_id, "selected =", len(meta_docs))

    if not meta_docs:
        print("No docs for this run_id.")
        return

    run_folder = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    out_dir = os.path.join(OUTPUT_ROOT, "videos", run_folder)
    os.makedirs(out_dir, exist_ok=True)

    zip_path = os.path.join(out_dir, "data.zip")
    meta_path = os.path.join(out_dir, "metadata.jsonl")

    packed = 0
    skipped = 0
    total_bytes = 0
    records = []

    with zipfile.ZipFile(zip_path, "w", zipfile.ZIP_DEFLATED) as z:
        for m in meta_docs:
            gridfs_id = m.get("gridfs_id")
            file_name = m.get("file_name") or f"video_{packed+1}.bin"

            if not gridfs_id:
                skipped += 1
                continue

            try:
                gridout = fs.get(ObjectId(str(gridfs_id)))
                content = gridout.read()
            except Exception:
                skipped += 1
                continue

            z.writestr(file_name, content)

            size_bytes = m.get("size_bytes") or len(content)
            total_bytes += int(size_bytes)

            rec = {k: safe_json(v) for k, v in m.items()}
            rec["data_file"] = "data.zip"
            records.append(rec)

            packed += 1

    header = {
        "source_type": "video",
        "entity": "dataset",
        "entity_name": "videos",
        "db_name": DB_NAME,
        "run_id": run_id,
        "file_count": packed,
        "total_size_bytes": total_bytes,
        "data_file": "data.zip"
    }

    with open(meta_path, "w", encoding="utf-8") as f:
        f.write(json.dumps(header) + "\n")
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")

    # ✅ Mark this run_id as exported
    state_col.update_one(
        {"_id": "videos"},
        {"$addToSet": {"exported_run_ids": run_id}},
        upsert=True
    )

    print("✅ DONE")
    print("Folder:", os.path.abspath(out_dir))
    print("Packed:", packed, "Skipped:", skipped)
    print("Data:", os.path.abspath(zip_path))
    print("Metadata:", os.path.abspath(meta_path))


if __name__ == "__main__":
    generate_video_outputs()

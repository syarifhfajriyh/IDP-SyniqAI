# MinIO Connection Troubleshooting

## Issue: InvalidAccessKeyId

The error "InvalidAccessKeyId" means the credentials in `config/minio_config.yaml` don't match your MinIO setup.

## Your MinIO Setup

Your Docker MinIO uses:
```bash
MINIO_ROOT_USER=admin
MINIO_ROOT_PASSWORD=password123
```

## Solution

You have two options:

### Option 1: Use Root Credentials (Quick)

The root user credentials from Docker should work, but MinIO might be expecting them in a specific format.

Try accessing the MinIO Console:
1. Open browser: http://localhost:9001
2. Login with:
   - Username: `admin`
   - Password: `password123`
3. Go to "Access Keys" → "Create access key"
4. Copy the Access Key and Secret Key
5. Update `config/minio_config.yaml`:

```yaml
minio:
  endpoint: localhost:9000
  access_key: <your-generated-access-key>
  secret_key: <your-generated-secret-key>
  secure: false
```

### Option  2: Test Connection Manually

Test if MinIO is accepting connections:

**PowerShell:**
```powershell
# Install MinIO Client (optional)
# Download from: https://min.io/docs/minio/windows/reference/minio-mc.html

# Or test with Python:
cd "c:\Users\Syarifah\OneDrive - M Telecommunication Sdn Bhd\INTERNSHIP\SyniqAi\data lakehouse"
.\syniq_env\Scripts\python.exe
```

**Python Test:**
```python
from minio import Minio

client = Minio(
    "localhost:9000",
    access_key="admin",
    secret_key="password123",
    secure=False
)

try:
    buckets = client.list_buckets()
    print("✅ Connected! Buckets:", [b.name for b in buckets])
except Exception as e:
    print("❌ Failed:", e)
    print("\nTip: Create access keys in MinIO Console (http://localhost:9001)")
```

### Option 3: Recreate MinIO with Different Credentials

If the above doesn't work, stop and restart MinIO with different credentials:

```powershell
# Stop existing container
docker stop <container-id>
docker rm <container-id>

# Start with new credentials
docker run -p 9000:9000 -p 9001:9001 \
  -e "MINIO_ROOT_USER=minioadmin" \
  -e "MINIO_ROOT_PASSWORD=minioadmin" \
  minio/minio server /data --console-address ":9001"
```

Then update `config/minio_config.yaml`:
```yaml
minio:
  endpoint: localhost:9000
  access_key: minioadmin
  secret_key: minioadmin
  secure: false
```

## After Fixing Credentials

Run these commands:

```bash
# Test connection
python main.py status

# Setup buckets
python main.py setup

# Test again
python main.py status
```

Expected output:
```
📦 MinIO:
  Status:  ✅ Connected
  Buckets: 3
```

## Current Config

Your current config has these defaults:
```yaml
minio:
  endpoint: ${MINIO_ENDPOINT:localhost:9000}
  access_key: ${MINIO_ACCESS_KEY:admin}
  secret_key: ${MINIO_SECRET_KEY:password123}
```

This means:
- It will use `localhost:9000` if `MINIO_ENDPOINT` env var is not set
- It will use `admin` if `MINIO_ACCESS_KEY` env var is not set
- It will use `password123` if `MINIO_SECRET_KEY` env var is not set

## Next Steps

1. Go to http://localhost:9001
2. Login with admin/password123
3. Create Access Keys
4. Update config/minio_config.yaml
5. Run `python main.py setup`

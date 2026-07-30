# SYNIQ AWS Data Pipeline - Complete Documentation

## **Overview**
This is a complete AWS data ingestion and management system with a GUI interface. It allows you to upload/download files to/from AWS S3, sync between S3 buckets, and manage both structured (CSV/Excel) and unstructured (images/videos/PDFs) data.

---

## **System Architecture**

### **Main Components:**
1. **gui.py** - Main graphical user interface
2. **ingest_structured.py** - Handles CSV/Excel file uploads
3. **ingest_unstructured.py** - Handles image/video/PDF uploads
4. **s3.py** - AWS S3 operations (upload, download, sync)

---

## **1. GUI.PY - The Main Interface**

### **Purpose:**
The control center for all operations. Provides a professional dark-themed interface with tabs for different functions.

### **Key Features:**

#### **A. Window Setup (Lines 20-58)**
```python
def __init__(self, root):
    self.root.title("SYNIQ AWS Data Ingestion Pipeline")
    self.root.geometry("1400x900")  # Window size
```
- Sets up the main window
- Defines color scheme (dark professional theme)
- Initializes state variables

#### **B. Four Main Tabs:**

**Tab 1: Instructions**
- Complete user guide
- Step-by-step workflow
- Best practices

**Tab 2: Configuration** (Split into 2 sub-tabs)
- **Main AWS Config**: Your primary AWS account
  - AWS Region (e.g., ap-southeast-1)
  - S3 Bucket name
  - Output directory for local files
  - Test connection button
  
- **Client S3 Config**: For S3-to-S3 sync
  - Client bucket name
  - Client credentials (Access Key, Secret Key)
  - Only needed for cross-account sync

**Tab 3: Local Operations**
- **Upload Structured Data**: CSV/Excel files
- **Upload Unstructured Data**: Images, videos, PDFs
- **Download All**: Get everything from S3
- **Download Structured**: Get only Parquet files

**Tab 4: S3 Operations**
- **S3 Bucket Sync**: Copy from client S3 to your S3

#### **C. Key Functions:**

**Connection Testing**
```python
def _test_aws_connection(self):
```
- Validates AWS credentials
- Checks bucket access
- Shows bucket info (region, file count, size)
- Updates status badge

**Upload Operations**
```python
def _ingest_structured(self):
def _ingest_unstructured(self):
```
- Runs in background thread (non-blocking)
- Calls the respective ingestion scripts
- Shows progress and results

**Download Operations**
```python
def _download_all_from_s3(self):
def _download_structured_from_s3(self):
```
- Creates local download directory
- Handles S3 path → Windows path conversion
- Skips folder markers (keys ending in `/`)
- Shows download progress

---

## **2. INGEST_STRUCTURED.PY - CSV/Excel Handler**

### **Purpose:**
Uploads CSV or Excel files to S3 with deduplication and metadata tracking.

### **Workflow:**

#### **Step 1: File Selection**
```python
paths = filedialog.askopenfilenames(
    title="Select CSV or Excel files",
    filetypes=[("CSV or Excel", "*.csv *.xlsx")]
)
```
- User selects one or more files
- Supports CSV and XLSX formats

#### **Step 2: Auto-Generate Dataset Name**
```python
first_file = Path(paths[0]).stem  # Get filename without extension
dataset = safe_name(first_file)
```
- Uses first selected filename as dataset name
- Removes special characters for safety

#### **Step 3: Generate Run ID**
```python
rid = run_id()    # e.g., "20260213T022352Z"
rdate = run_date() # e.g., "2026-02-13"
```
- Creates unique timestamp-based identifiers
- Ensures no overwrites

#### **Step 4: Process Each File**
```python
for path in paths:
    # 1. Read file (CSV or Excel)
    if path.endswith('.csv'):
        df = pd.read_csv(path)
    else:
        df = pd.read_excel(path)
    
    # 2. Calculate file hash (for deduplication)
    file_hash = compute_file_hash(path)
    
    # 3. Check if already uploaded
    if file_hash in known_hashes:
        skipped_count += 1
        continue
    
    # 4. Convert to Parquet
    parquet_path = local_data_dir / f"{fname}.parquet"
    df.to_parquet(parquet_path)
    
    # 5. Upload to S3
    s3.upload_file(str(parquet_path), S3_BUCKET, s3_key)
    
    # 6. Save metadata
    meta_rows.append({
        'file_hash': file_hash,
        'original_name': fname,
        'row_count': len(df),
        'column_count': len(df.columns),
        's3_key': s3_key
    })
```

#### **Step 5: Save Metadata**
```python
# Save as Parquet
meta_df = pd.DataFrame(meta_rows)
meta_df.to_parquet(meta_path)

# Save as human-readable text
with open(pretty_path, 'w') as f:
    f.write(meta_df.to_string())

# Upload both to S3
s3.upload_file(str(meta_path), S3_BUCKET, s3_meta_key)
s3.upload_file(str(pretty_path), S3_BUCKET, s3_pretty_key)
```

### **Deduplication Logic:**
- Computes SHA-256 hash of each file
- Compares against previously uploaded files
- Only uploads if hash is new
- Prevents duplicate data

---

## **3. INGEST_UNSTRUCTURED.PY - Media File Handler**

### **Purpose:**
Uploads images, videos, PDFs, and other binary files to S3.

### **Workflow:**

#### **Step 1: File Selection**
```python
paths = filedialog.askopenfilenames(
    title="Select unstructured files",
    filetypes=[
        ("Images", "*.png *.jpg *.jpeg"),
        ("Videos", "*.mp4 *.avi *.mov"),
        ("PDFs", "*.pdf"),
        ("All files", "*.*")
    ]
)
```

#### **Step 2: Process Each File**
```python
for path in paths:
    # 1. Calculate hash
    file_hash = compute_file_hash(path)
    
    # 2. Check for duplicates
    if file_hash in known_hashes:
        skipped_count += 1
        continue
    
    # 3. Copy to local output directory
    shutil.copy2(path, dest_path)
    
    # 4. Upload to S3
    s3.upload_file(str(dest_path), S3_BUCKET, s3_key)
    
    # 5. Save metadata
    meta_rows.append({
        'file_hash': file_hash,
        'original_name': fname,
        'file_size': os.path.getsize(path),
        'file_type': ext,
        's3_key': s3_key
    })
```

#### **Step 3: Save Metadata as JSON Lines**
```python
with open(meta_path, 'w') as f:
    for row in meta_rows:
        f.write(json.dumps(row) + "\n")

s3.upload_file(str(meta_path), S3_BUCKET, s3_meta_key)
```
- JSONL format: One JSON object per line
- Easy to append and parse

---

## **4. S3.PY - AWS Operations**

### **Purpose:**
Low-level S3 operations shared by all scripts.

### **Key Functions:**

#### **upload_file() - Upload to S3**
```python
def upload_file(local_path, bucket, s3_key):
    s3 = boto3.client('s3')
    s3.upload_file(local_path, bucket, s3_key)
    print(f"UPLOADED: {local_path} → s3://{bucket}/{s3_key}")
```

#### **download_object() - Download from S3**
```python
def download_object(bucket, s3_key, local_path):
    s3 = boto3.client('s3')
    s3.download_file(bucket, s3_key, local_path)
```

#### **copy_s3_to_s3() - Cross-Bucket Sync**
```python
def copy_s3_to_s3(source_bucket, dest_bucket, client_credentials):
    # 1. Create client S3 connection
    source_s3 = boto3.client('s3',
        aws_access_key_id=client_credentials['access_key'],
        aws_secret_access_key=client_credentials['secret_key']
    )
    
    # 2. List all objects in source
    paginator = source_s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=source_bucket)
    
    # 3. Copy each object
    for page in pages:
        for obj in page.get('Contents', []):
            key = obj['Key']
            
            # Check if already exists in destination
            if not object_exists(dest_bucket, key):
                # Download from source
                temp_file = tempfile.NamedTemporaryFile(delete=False)
                source_s3.download_file(source_bucket, key, temp_file.name)
                
                # Upload to destination
                upload_file(temp_file.name, dest_bucket, key)
                
                # Clean up
                os.remove(temp_file.name)
```

---

## **Key Concepts & Design Decisions**

### **1. Deduplication**
- **Why?** Prevents uploading the same file twice
- **How?** SHA-256 hash of file content
- **Where?** Both structured and unstructured ingestion

### **2. Run IDs & Partitioning**
- **Format:** `run_date=2026-02-13/run_id=20260213T022352Z/`
- **Purpose:** 
  - Time-based organization
  - Easy to query specific time ranges
  - No overwrites

### **3. Metadata Tracking**
- **Structured:** Parquet + pretty text
- **Unstructured:** JSONL format
- **Contains:** File info, hashes, S3 locations, stats

### **4. Local + S3 Storage**
- **Local:** `output/` directory for backup
- **S3:** Cloud storage for access anywhere
- **Benefit:** Redundancy and portability

### **5. Threading**
- **Why?** GUI stays responsive during uploads
- **How?** `threading.Thread(target=run_ingest, daemon=True).start()`
- **Benefit:** User can still interact with GUI

### **6. Error Handling**
- Try/except blocks throughout
- User-friendly error messages
- Graceful failure (doesn't crash)

---

## **Data Flow Example**

### **Uploading a CSV file:**
```
1. User clicks "Upload Structured Data"
2. File dialog opens → user selects "sales.csv"
3. Dataset name auto-generated: "sales"
4. Run ID created: "20260213T022352Z"
5. CSV read into pandas DataFrame
6. File hash calculated
7. Check against known hashes
8. Convert to Parquet format
9. Save locally: 
   output/data/structured/tables/sales/run_date=2026-02-13/run_id=20260213T022352Z/sales.parquet
10. Upload to S3: 
    s3://bucket/data/structured/tables/sales/run_date=2026-02-13/run_id=20260213T022352Z/sales.parquet
11. Save metadata locally and to S3
12. Show success message with file paths
```

### **Downloading from S3:**
```
1. User clicks "Download All from S3"
2. Confirmation dialog appears
3. User clicks "Yes"
4. System lists all S3 objects
5. For each object:
   - Skip folder markers (keys ending in /)
   - Convert S3 path to Windows path
   - Create local directories
   - Download file
6. Show completion message with stats
```

### **S3-to-S3 Sync:**
```
1. User configures Client S3 credentials
2. User clicks "S3 Bucket Sync"
3. System connects to both S3 accounts
4. Lists all objects in client bucket
5. For each object:
   - Check if exists in destination
   - If not, download from client
   - Upload to destination bucket
   - Delete temporary file
6. Show sync completion summary
```

---

## **Configuration**

### **Environment Variables (.env file):**
```bash
AWS_REGION=ap-southeast-1
S3_BUCKET=my-bucket-name
OUTPUT_DIR=output
```

### **AWS Credentials:**
Located at `~/.aws/credentials`:
```ini
[default]
aws_access_key_id = YOUR_KEY
aws_secret_access_key = YOUR_SECRET
```

### **AWS CLI Setup:**
```bash
# Install AWS CLI first
pip install awscli

# Configure credentials
aws configure

# It will ask for:
# - AWS Access Key ID
# - AWS Secret Access Key
# - Default region name
# - Default output format (json)
```

---

## **File Structure**

```
SYNIQ-AWS/
│
├── gui.py                      # Main GUI application
├── ingest_structured.py        # CSV/Excel upload handler
├── ingest_unstructured.py      # Media file upload handler
├── s3.py                       # S3 operations module
├── .env                        # Configuration (optional)
│
└── output/                     # Local data storage
    ├── data/
    │   ├── structured/
    │   │   └── tables/
    │   │       └── [dataset]/
    │   │           └── run_date=[date]/
    │   │               └── run_id=[timestamp]/
    │   │                   └── *.parquet
    │   │
    │   └── unstructured/
    │       └── run_date=[date]/
    │           └── run_id=[timestamp]/
    │               ├── images/
    │               ├── videos/
    │               └── documents/
    │
    └── metadata/
        ├── structured_parquet/
        │   └── dataset=[name]/
        │       └── run_date=[date]/
        │           └── run_id=[timestamp]/
        │               ├── metadata.parquet
        │               └── metadata_pretty.txt
        │
        └── unstructured_json/
            └── run_date=[date]/
                └── run_id=[timestamp]/
                    └── metadata.jsonl
```

---

## **Common Use Cases**

### **1. Daily Data Upload**
**Scenario:** You receive daily sales reports in CSV format
**Steps:**
1. Open the GUI
2. Go to Configuration tab → Enter S3 bucket
3. Test connection
4. Go to Local Operations tab
5. Click "Upload Structured Data"
6. Select all CSV files
7. System automatically:
   - Names dataset after first file
   - Checks for duplicates
   - Converts to Parquet
   - Uploads to S3
   - Saves metadata

### **2. Client Data Sync**
**Scenario:** Client uploads data to their S3, you need to copy it
**Steps:**
1. Go to Configuration tab → Client S3 Config sub-tab
2. Enter client's bucket name
3. Enter client's access keys
4. Test client connection
5. Go to S3 Operations tab
6. Click "S3 Bucket Sync"
7. System copies all new files from client to your bucket

### **3. Download for Analysis**
**Scenario:** Need to analyze data locally
**Steps:**
1. Go to Configuration tab → Set your bucket
2. Go to Local Operations tab
3. Choose download type:
   - "Download All" for everything
   - "Download Structured Only" for just Parquet files
4. Files download to your Downloads folder

### **4. Upload Media Files**
**Scenario:** Need to store images/videos in S3
**Steps:**
1. Go to Local Operations tab
2. Click "Upload Unstructured Data"
3. Select files (images, videos, PDFs)
4. System:
   - Checks for duplicates
   - Organizes by type
   - Uploads to S3
   - Tracks metadata

---

## **Troubleshooting**

### **Issue: "Can't Find Credentials"**
**Solution:**
```bash
# Run AWS configure
aws configure

# Or set environment variables
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
```

### **Issue: "Access Denied to Bucket"**
**Solution:**
- Check bucket name is correct
- Verify your AWS user has S3 permissions
- Check bucket policy allows your account

### **Issue: "Download Failed - Permission Error"**
**Solution:**
- This was fixed in the code
- Make sure you're running the latest version
- Check your Downloads folder isn't write-protected

### **Issue: "No New Data" Message**
**Reason:** All selected files were already uploaded (duplicates detected)
**Solution:**
- This is normal - the system prevents duplicate uploads
- Check console output for details
- View metadata files to see what was uploaded before

---

## **Performance Considerations**

### **Large Files:**
- Structured data is converted to Parquet (highly compressed)
- Unstructured data is uploaded as-is
- Use threading to prevent GUI freezing

### **Many Files:**
- Batch processing supported
- Progress shown in console
- Deduplication saves bandwidth

### **Network:**
- AWS SDK handles retries automatically
- Progress bar shows activity
- Background threads prevent blocking

---

## **Security Best Practices**

### **1. Credentials:**
- Never hardcode access keys in code
- Use AWS CLI configuration
- Or use IAM roles (if running on EC2)

### **2. Client S3 Access:**
- Request read-only permissions from client
- Store credentials securely
- Don't share your main AWS credentials with clients

### **3. Bucket Permissions:**
- Use least-privilege principle
- Separate buckets for different purposes
- Enable versioning for important data

---

## **Advanced Features**

### **Metadata Queries:**
```python
# Read metadata from S3
import pandas as pd

# Structured metadata
df = pd.read_parquet('s3://bucket/metadata/structured_parquet/...')
print(df[['original_name', 'row_count', 'column_count']])

# Unstructured metadata
import json
with open('metadata.jsonl') as f:
    for line in f:
        meta = json.loads(line)
        print(meta['original_name'], meta['file_size'])
```

### **Custom Processing:**
You can modify the code to add:
- Data validation
- Custom transformations
- Notifications (email/Slack)
- Logging to external services

---

## **Summary**

**What it does:** Complete data pipeline for AWS S3

**Who it's for:** Users who need to manage data uploads/downloads with S3

**Key benefits:**
- ✅ Easy-to-use GUI (no command line needed)
- ✅ Automatic deduplication
- ✅ Metadata tracking
- ✅ Both structured and unstructured data
- ✅ Cross-account S3 sync
- ✅ Progress tracking and error handling
- ✅ Natural, conversational messages
- ✅ Professional dark theme UI
- ✅ Background processing (non-blocking)

**Requirements:**
- Python 3.7+
- boto3 (AWS SDK)
- pandas
- pyarrow
- tkinter (usually included with Python)

**Installation:**
```bash
pip install boto3 pandas pyarrow python-dotenv
```

**Running:**
```bash
python gui.py
```

---

## **Support & Maintenance**

### **Logs Location:**
- Console output in GUI
- AWS CloudWatch (if configured)
- Local metadata files

### **Backup Strategy:**
- Data stored both locally and in S3
- Metadata preserved for every run
- Run IDs ensure no overwrites

### **Updates:**
- Code is modular (easy to modify)
- Add new features by extending classes
- UI can be customized in gui.py

---

**Last Updated:** February 13, 2026
**Version:** 2.0.0

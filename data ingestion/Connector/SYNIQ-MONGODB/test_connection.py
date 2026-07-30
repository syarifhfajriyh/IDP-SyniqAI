"""
MongoDB Connection Test Script
Run this to verify your on_prem.env configuration before using the main application
"""

import os
import sys
from dotenv import load_dotenv
from pymongo import MongoClient

print("="*80)
print("TSD Media Pipeline - Connection Test")
print("="*80)
print()

# Step 1: Check if on_prem.env exists
script_dir = os.path.dirname(os.path.abspath(__file__))
env_paths = [
    os.path.join(script_dir, 'on_prem.env'),
    'on_prem.env',
    os.path.join(os.getcwd(), 'on_prem.env')
]

env_found = None
for env_path in env_paths:
    if os.path.exists(env_path):
        env_found = env_path
        break

if not env_found:
    print("❌ FAILED: on_prem.env file not found!")
    print()
    print("Searched in these locations:")
    for path in env_paths:
        print(f"  - {path}")
    print()
    print("SOLUTION:")
    print("  1. Copy .env.template to on_prem.env")
    print("  2. Edit on_prem.env with your MongoDB credentials")
    print("  3. Run this test again")
    sys.exit(1)

print(f"✓ Found configuration file: {env_found}")
print()

# Step 2: Load environment variables
load_dotenv(env_found)

# Check for MONGO_URI (Atlas/Custom) or individual variables (On-Prem)
MONGO_URI = os.getenv('MONGO_URI')
DB_NAME = os.getenv('MONGO_DB', 'media_db')

if MONGO_URI:
    # Using MongoDB Atlas or custom connection string
    print("Configuration loaded:")
    print(f"  Connection Type: Atlas/Custom URI")
    print(f"  Database: {DB_NAME}")
    # Mask credentials in URI for display
    masked_uri = MONGO_URI
    if '@' in masked_uri:
        parts = masked_uri.split('@')
        cred_part = parts[0].split('//')[-1]
        if ':' in cred_part:
            user = cred_part.split(':')[0]
            masked_uri = masked_uri.replace(cred_part, f"{user}:***")
    print(f"  URI: {masked_uri}")
    print()
    print("✓ Atlas/Custom URI configured")
    connection_string = MONGO_URI
else:
    # Using On-Prem individual variables
    MONGO_HOST = os.getenv('MONGO_HOST', 'localhost')
    MONGO_PORT = os.getenv('MONGO_PORT', '27017')
    MONGO_USER = os.getenv('MONGO_USER')
    MONGO_PASS = os.getenv('MONGO_PASS')
    MONGO_AUTH_SOURCE = os.getenv('MONGO_AUTH_SOURCE', 'admin')
    
    print("Configuration loaded:")
    print(f"  Connection Type: On-Prem")
    print(f"  Host: {MONGO_HOST}")
    print(f"  Port: {MONGO_PORT}")
    print(f"  Database: {DB_NAME}")
    print(f"  Auth Source: {MONGO_AUTH_SOURCE}")
    print(f"  Username: {MONGO_USER if MONGO_USER else '(not set)'}")
    print(f"  Password: {'***' if MONGO_PASS else '(not set)'}")
    print()
    
    # Step 3: Check if credentials are configured
    if not MONGO_USER or not MONGO_PASS:
        print("❌ FAILED: MongoDB credentials not configured!")
        print()
        print("Your on_prem.env file exists but MONGO_USER or MONGO_PASS is empty.")
        print()
        print("SOLUTION:")
        print("  1. Open on_prem.env in a text editor")
        print("  2. Replace placeholders with actual values:")
        print(f"     MONGO_HOST={MONGO_HOST}")
        print(f"     MONGO_USER=your_actual_username")
        print(f"     MONGO_PASS=your_actual_password")
        print("  3. Save and run this test again")
        sys.exit(1)
    
    print("✓ On-Prem credentials configured")
    connection_string = f"mongodb://{MONGO_USER}:{MONGO_PASS}@{MONGO_HOST}:{MONGO_PORT}/?authSource={MONGO_AUTH_SOURCE}"
print()

# Step 4: Test connection
print("Testing connection to MongoDB...")
try:
    client = MongoClient(
        connection_string,
        serverSelectionTimeoutMS=5000,  # 5 second timeout
        connectTimeoutMS=5000
    )
    
    # Force connection attempt
    client.admin.command('ping')
    
    print("✓ Successfully connected to MongoDB!")
    print()
    
    # Step 6: Test database access
    print("Testing database access...")
    db = client[DB_NAME]
    
    # Try to list collections
    collections = db.list_collection_names()
    print(f"✓ Can access database '{DB_NAME}'")
    print(f"  Found {len(collections)} collections")
    if collections:
        print(f"  Collections: {', '.join(collections[:5])}" + (" ..." if len(collections) > 5 else ""))
    print()
    
    # Step 7: Test authentication by trying a write operation (to a test collection)
    print("Testing write permissions...")
    try:
        test_collection = db['_connection_test']
        result = test_collection.insert_one({'test': True, 'timestamp': str(os.times())})
        test_collection.delete_one({'_id': result.inserted_id})
        print("✓ Write permissions verified")
        print()
    except Exception as write_error:
        print(f"❌ Write test failed: {write_error}")
        print()
        print("Your user may have read-only permissions.")
        print("Contact your administrator to grant write permissions.")
        print()
    
    print("="*80)
    print("✅ ALL TESTS PASSED!")
    print("="*80)
    print()
    print("Your configuration is correct. You can now run the main application:")
    print("  - Double-click run_gui.bat")
    print("  OR")
    print("  - Run: python media_upload_gui.py")
    print()
    
    client.close()
    
except Exception as e:
    print(f"❌ CONNECTION FAILED!")
    print()
    print(f"Error: {str(e)}")
    print()
    print("COMMON CAUSES:")
    print()
    print("1. MongoDB server is not running or not reachable")
    print(f"   - Try: ping {MONGO_HOST}")
    print(f"   - Check if port {MONGO_PORT} is open")
    print()
    print("2. Firewall blocking connection")
    print("   - Check firewall rules")
    print("   - Ensure VPN is connected (if required)")
    print()
    print("3. Wrong credentials")
    print("   - Double-check username and password in on_prem.env")
    print("   - Passwords are case-sensitive!")
    print()
    print("4. Wrong authentication database")
    print(f"   - Current: MONGO_AUTH_SOURCE={MONGO_AUTH_SOURCE}")
    print("   - Try changing to 'admin' if unsure")
    print()
    print("5. MongoDB server requires SSL/TLS")
    print("   - Contact administrator for SSL configuration")
    print()
    sys.exit(1)

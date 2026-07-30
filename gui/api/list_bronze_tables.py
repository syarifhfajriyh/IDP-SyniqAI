"""List Available Bronze Tables for Joins"""
import boto3
from botocore.client import Config

# Initialize MinIO client
s3 = boto3.client(
    's3',
    endpoint_url='http://localhost:9000',
    aws_access_key_id='admin',
    aws_secret_access_key='password123',
    config=Config(signature_version='s3v4'),
    region_name='us-east-1'
)

try:
    print("📊 Available Bronze Tables for Joins:\n")
    print("=" * 60)
    
    # List tables in finance/postgres/
    response = s3.list_objects_v2(
        Bucket='syniqai-bronze',
        Prefix='finance/postgres/',
        Delimiter='/'
    )
    
    if 'CommonPrefixes' in response:
        tables = []
        for prefix in response['CommonPrefixes']:
            # Extract table name
            table_name = prefix['Prefix'].replace('finance/postgres/', '').rstrip('/')
            tables.append(table_name)
        
        print(f"\n✅ Found {len(tables)} tables in Bronze layer:\n")
        for i, table in enumerate(sorted(tables), 1):
            print(f"   {i}. {table}")
        
        print("\n" + "=" * 60)
        print("\n💡 Use these table names in the 'right_table' field for joins")
        print("   Example: 'user_profile' or 'account_details'\n")
    else:
        print("\n❌ No tables found in syniqai-bronze/finance/postgres/\n")
    
except Exception as e:
    print(f"\n❌ Error: {e}\n")

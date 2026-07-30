"""
Upload sample data to Bronze layer for testing
Creates sample datasets for Finance, Healthcare, and General domains
"""
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import sys
from pathlib import Path
from dotenv import load_dotenv
import os

# Load environment variables FIRST
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(env_path)

# Then import modules that use config
sys.path.append(str(Path(__file__).parent))

from api.storage import storage_manager

def create_finance_data(num_records=1000):
    """Create sample finance transaction data"""
    np.random.seed(42)
    
    df = pd.DataFrame({
        'transaction_id': [f'TXN{i:06d}' for i in range(1, num_records + 1)],
        'customer_id': [f'CUST{np.random.randint(1, 500):04d}' for _ in range(num_records)],
        'amount': np.random.uniform(10.0, 50000.0, num_records).round(2),
        'currency': np.random.choice(['USD', 'EUR', 'GBP'], num_records, p=[0.7, 0.2, 0.1]),
        'transaction_type': np.random.choice(['PURCHASE', 'REFUND', 'TRANSFER'], num_records, p=[0.7, 0.1, 0.2]),
        'merchant_name': np.random.choice(['Amazon', 'Walmart', 'Target', 'Best Buy', 'Apple'], num_records),
        'transaction_date': [datetime.now() - timedelta(hours=i) for i in range(num_records)],
        'card_number': [f'****{np.random.randint(1000, 9999)}' for _ in range(num_records)],
        'status': np.random.choice(['COMPLETED', 'PENDING', 'FAILED'], num_records, p=[0.85, 0.10, 0.05])
    })
    
    # Add some intentional data quality issues for testing
    df.loc[np.random.choice(df.index, 50, replace=False), 'amount'] = -100.0  # Negative amounts
    df.loc[np.random.choice(df.index, 30, replace=False), 'currency'] = 'XXX'  # Invalid currency
    df.loc[np.random.choice(df.index, 20, replace=False), 'customer_id'] = None  # Missing customer
    
    return df


def create_healthcare_data(num_records=500):
    """Create sample healthcare patient data"""
    np.random.seed(43)
    
    df = pd.DataFrame({
        'patient_id': [f'PT{i:05d}' for i in range(1, num_records + 1)],
        'patient_name': [f'Patient_{i}' for i in range(num_records)],
        'ssn': [f'{np.random.randint(100, 999)}-{np.random.randint(10, 99)}-{np.random.randint(1000, 9999)}' for _ in range(num_records)],
        'date_of_birth': [datetime.now() - timedelta(days=np.random.randint(7000, 30000)) for _ in range(num_records)],
        'diagnosis_code': np.random.choice(['A01.1', 'B02.2', 'C03.3', 'D04.4', 'E05.5'], num_records),
        'admission_date': [datetime.now() - timedelta(days=np.random.randint(1, 90)) for _ in range(num_records)],
        'blood_pressure_systolic': np.random.randint(90, 180, num_records),
        'blood_pressure_diastolic': np.random.randint(60, 120, num_records),
        'temperature_f': np.random.uniform(96.0, 103.0, num_records).round(1),
        'insurance_status': np.random.choice(['INSURED', 'UNINSURED', 'PENDING'], num_records, p=[0.75, 0.15, 0.10])
    })
    
    # Add some intentional data quality issues
    df.loc[np.random.choice(df.index, 25, replace=False), 'blood_pressure_systolic'] = 300  # Invalid BP
    df.loc[np.random.choice(df.index, 20, replace=False), 'diagnosis_code'] = 'INVALID'  # Invalid code
    df.loc[np.random.choice(df.index, 15, replace=False), 'ssn'] = None  # Missing SSN
    
    return df


def create_general_data(num_records=800):
    """Create sample general user data"""
    np.random.seed(44)
    
    df = pd.DataFrame({
        'user_id': [f'USR{i:06d}' for i in range(1, num_records + 1)],
        'email': [f'user{i}@example.com' for i in range(num_records)],
        'username': [f'user_{i}' for i in range(num_records)],
        'registration_date': [datetime.now() - timedelta(days=np.random.randint(1, 365)) for _ in range(num_records)],
        'last_login': [datetime.now() - timedelta(days=np.random.randint(0, 30)) for _ in range(num_records)],
        'account_status': np.random.choice(['ACTIVE', 'INACTIVE', 'SUSPENDED'], num_records, p=[0.8, 0.15, 0.05]),
        'subscription_tier': np.random.choice(['FREE', 'BASIC', 'PREMIUM'], num_records, p=[0.6, 0.3, 0.1]),
        'country': np.random.choice(['US', 'UK', 'CA', 'AU', 'DE'], num_records),
        'age': np.random.randint(18, 80, num_records),
        'total_purchases': np.random.randint(0, 100, num_records)
    })
    
    # Add some intentional data quality issues
    df.loc[np.random.choice(df.index, 40, replace=False), 'email'] = 'invalid_email'  # Invalid email
    df.loc[np.random.choice(df.index, 30, replace=False), 'age'] = -5  # Invalid age
    df.loc[np.random.choice(df.index, 25, replace=False), 'username'] = None  # Missing username
    
    # Add some exact duplicates
    duplicate_indices = np.random.choice(df.index, 20, replace=False)
    df = pd.concat([df, df.loc[duplicate_indices]], ignore_index=True)
    
    return df


def upload_data():
    """Upload sample data to MinIO Bronze layer"""
    print("=== SyniqAI Sample Data Upload ===\n")
    
    # Initialize storage
    print("Initializing MinIO storage...")
    try:
        storage_manager.initialize()
        print("✓ Storage initialized\n")
    except Exception as e:
        print(f"✗ Failed to initialize storage: {e}")
        print("\nMake sure MinIO is running:")
        print("  docker run -d -p 9000:9000 -p 9001:9001 minio/minio server /data --console-address \":9001\"")
        return False
    
    datasets = [
        {
            'domain': 'finance',
            'table': 'transactions',
            'func': create_finance_data,
            'records': 1000
        },
        {
            'domain': 'healthcare',
            'table': 'patient_records',
            'func': create_healthcare_data,
            'records': 500
        },
        {
            'domain': 'general',
            'table': 'user_data',
            'func': create_general_data,
            'records': 800
        }
    ]
    
    uploaded_count = 0
    for dataset in datasets:
        print(f"Creating {dataset['domain']} {dataset['table']}...")
        try:
            df = dataset['func'](dataset['records'])
            print(f"  Generated {len(df)} records with {len(df.columns)} columns")
            
            # Upload to Bronze layer
            object_name = f"{dataset['domain']}/{dataset['table']}/sample_data.parquet"
            success = storage_manager.write_parquet("bronze", object_name, df)
            
            if success:
                print(f"  ✓ Uploaded to: bronze/{object_name}")
                print(f"  Size: {df.memory_usage(deep=True).sum() / 1024:.2f} KB")
                uploaded_count += 1
            else:
                print(f"  ✗ Failed to upload {object_name}")
        except Exception as e:
            print(f"  ✗ Error: {e}")
        print()
    
    if uploaded_count == len(datasets):
        print(f"✓ Successfully uploaded {uploaded_count}/{len(datasets)} datasets!")
        print("\nNext steps:")
        print("1. Run: streamlit run app.py")
        print("2. Select a domain (Finance/Healthcare/General)")
        print("3. Navigate to Bronze Explorer")
        print("4. View the uploaded data and run quality checks")
        return True
    else:
        print(f"⚠ Uploaded {uploaded_count}/{len(datasets)} datasets")
        print("Check errors above and retry.")
        return False


def main():
    """Main execution"""
    success = upload_data()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()

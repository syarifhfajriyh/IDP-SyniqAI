"""
Debug Pipeline Builder Execution - Check what's being sent to the API
"""
import requests
import json

# Test the exact payload that Pipeline Builder would send
test_pipeline = {
    "source_table": "finance_transactions",
    "target_table": "pipeline_output",
    "transformations": [
        {
            "operation": "join_inner",
            "params": {
                "right_table": "finance_transactions",
                "on": "user_id"
            }
        },
        {
            "operation": "filter",
            "params": {
                "condition": "amount > 0"
            }
        }
    ],
    "output_config": {
        "domain": "finance",
        "format": "parquet"
    }
}

print("📤 Sending request to backend...")
print(json.dumps(test_pipeline, indent=2))
print("\n" + "="*60 + "\n")

try:
    response = requests.post(
        'http://localhost:8000/api/silver/execute-transformation',
        json=test_pipeline,
        timeout=30
    )
    
    print(f"📡 Status Code: {response.status_code}")
    print(f"📡 Headers: {dict(response.headers)}\n")
    
    if response.status_code == 200:
        result = response.json()
        print("✅ SUCCESS!")
        print(json.dumps(result, indent=2))
    else:
        print("❌ ERROR!")
        print(f"Status: {response.status_code}")
        print(f"Response Text: {response.text}")
        
        try:
            error_json = response.json()
            print("\n📋 Error Details:")
            print(json.dumps(error_json, indent=2))
            
            # Analyze error structure
            if 'detail' in error_json:
                detail = error_json['detail']
                print(f"\n🔍 Detail Type: {type(detail)}")
                
                if isinstance(detail, list):
                    print(f"🔍 Detail is a list with {len(detail)} items:")
                    for i, item in enumerate(detail, 1):
                        print(f"   {i}. {item}")
                elif isinstance(detail, dict):
                    print(f"🔍 Detail is a dict with keys: {list(detail.keys())}")
                else:
                    print(f"🔍 Detail: {detail}")
                    
        except Exception as e:
            print(f"\n⚠️  Could not parse error JSON: {e}")

except requests.exceptions.ConnectionError:
    print("❌ Connection Error: Backend is not running!")
    print("   Please start the backend with: cd gui/api && python backend.py")
except requests.exceptions.Timeout:
    print("❌ Timeout: Backend took too long to respond")
except Exception as e:
    print(f"❌ Unexpected error: {e}")
    print(f"   Type: {type(e)}")
    import traceback
    traceback.print_exc()

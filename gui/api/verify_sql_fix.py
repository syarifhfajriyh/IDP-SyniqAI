"""
Quick diagnostic script to verify SQL endpoint configuration
"""
import sys
sys.path.insert(0, 'c:/Users/Syarifah/OneDrive - M Telecommunication Sdn Bhd/INTERNSHIP/SyniqAi/gui/api')

from sql_query_routes import router

# Check the execute endpoint
for route in router.routes:
    if 'execute' in route.path:
        print(f"✅ Found endpoint: {route.path}")
        print(f"   Method: {route.methods}")
        print(f"   Function: {route.endpoint.__name__}")

print("\n📝 Checking table paths in sql_query_routes.py...")
import inspect
source = inspect.getsource(router.routes[0].endpoint)

if 'syniqai-bronze' in source:
    print("✅ Code has correct bucket name: syniqai-bronze")
else:
    print("❌ Code still has wrong bucket name")

if 'finance/postgres/finance_transactions' in source:
    print("✅ Code has correct path structure: finance/postgres/finance_transactions")
else:
    print("❌ Code has wrong path structure")

print("\n💡 Backend restart required for changes to take effect!")

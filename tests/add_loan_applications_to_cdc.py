#!/usr/bin/env python3
"""
Add loan_applications table to Debezium CDC monitoring
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'Kafka Integration'))

from debezium_manager import DebeziumManager

def main():
    print("🔧 Updating Debezium connector to include loan_applications table...")
    
    manager = DebeziumManager(kafka_connect_url="http://localhost:8083")
    
    # Get current connector config
    connector_name = "postgres-cdc-connector"
    
    try:
        current_connector = manager.get_connector(connector_name)
        if not current_connector:
            print(f"❌ Connector '{connector_name}' not found!")
            print("\nAvailable connectors:")
            connectors = manager.list_connectors()
            for conn in connectors:
                print(f"  - {conn}")
            return 1
            
        print(f"✓ Found connector: {connector_name}")
        current_config = current_connector.get('config', {})
        print(f"  Current tables: {current_config.get('table.include.list', 'N/A')}")
        
        # Update table.include.list to add loan_applications
        new_table_list = "public.hosp_raya_patient_record,public.cdc_test,public.loan_applications"
        
        # Get the full config and update just the table list
        updated_config = current_config.copy()
        updated_config["table.include.list"] = new_table_list
        
        print(f"\n📝 Updating table list to: {new_table_list}")
        result = manager.update_connector(connector_name, updated_config)
        
        print("✅ Connector updated successfully!")
        print("   The loan_applications table is now being monitored for CDC events")
        print("\n📌 Next steps:")
        print("   1. Check Kafka UI - you should see a new topic: cdc.syniq_postgres.public.loan_applications")
        print("   2. Make changes to loan_applications (INSERT/UPDATE/DELETE)")
        print("   3. CDC events will automatically flow: PostgreSQL → Kafka → Bronze Iceberg")
        
    except Exception as e:
        print(f"❌ Error: {e}")
        print("\nTroubleshooting:")
        print("  1. Make sure Kafka/Debezium is running (check localhost:8083)")
        print("  2. Check if connector exists: curl http://localhost:8083/connectors")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())

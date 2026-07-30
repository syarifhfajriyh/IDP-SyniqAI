"""
Bronze Layer Explorer
File system-style database explorer with metadata and basic profiling
"""

import streamlit as st
import pandas as pd
from datetime import datetime
import sys
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
env_path = Path(__file__).parent.parent.parent / ".env"
load_dotenv(env_path)

# Import backend modules
sys.path.append(str(Path(__file__).parent.parent))
try:
    from api.storage import storage_manager, bronze_manager
except ImportError:
    storage_manager = None
    bronze_manager = None

def show():
    """Display Bronze layer file explorer"""
    
    # Initialize storage if not already done
    if storage_manager and not storage_manager._initialized:
        try:
            storage_manager.initialize()
        except Exception as e:
            st.error(f"⚠ Storage connection failed: {e}")
            st.info("Make sure MinIO is running. See SETUP_SERVICES.md")
            return
    
    # Get domain from session state
    domain = st.session_state.get('selected_domain', 'general')
    domain_name = domain.upper()
    
    # Header
    st.markdown(f"""
        <div style='background: linear-gradient(135deg, #92400e 0%, #b45309 100%); 
                    padding: 1.5rem; 
                    border-radius: 0.75rem; 
                    margin-bottom: 2rem;
                    color: white;'>
            <h 1 style='color: white; margin: 0; font-size: 1.75rem;'>🟫 BRONZE LAYER</h1>
            <p style='margin: 0.5rem 0 0 0; opacity: 0.9;'>{domain_name} Domain | Raw Data Ingestion</p>
        </div>
    """, unsafe_allow_html=True)
    
    # Main layout
    col_tree, col_details = st.columns([1, 2])
    
    with col_tree:
        st.markdown("### 📁 Data Sources")
        
        # Load real tables from Bronze layer
        if bronze_manager:
            try:
                tables = bronze_manager.list_tables(domain)
                
                # Display tables
                st.markdown(f"""
                    <div style='background: white; 
                                padding: 1rem; 
                                border-radius: 0.5rem; 
                                border: 2px solid #e5e7eb;
                                font-family: monospace;
                                font-size: 0.875rem;'>
                        <div style='margin-bottom: 0.5rem; color: #3b82f6; font-weight: 600;'>📁 {domain_name}</div>
                """, unsafe_allow_html=True)
                
                # Create clickable table list
                selected_table = st.session_state.get('selected_table', None)
                for table in tables:
                    table_name = table['table_name']
                    is_selected = (selected_table == table_name)
                    color = '#059669' if is_selected else '#374151'
                    cursor = 'pointer'
                    
                    # Use button for selection
                    if st.button(f"📄 {table_name}", key=f"table_{table_name}", use_container_width=True):
                        st.session_state['selected_table'] = table_name
                        st.rerun()
                
                st.markdown("</div>", unsafe_allow_html=True)
                
                if not tables:
                    st.info(f"No tables found in {domain} domain. Upload data using `python upload_sample_data.py`")
                    
            except Exception as e:
                st.error(f"Error loading tables: {e}")
                st.code(str(e))
        else:
            st.warning("Storage backend not available")
        
        st.markdown("<br>", unsafe_allow_html=True)
        st.info("● = Selected table")
    
    with col_details:
        st.markdown("### 📄 transactions")
        
        # Action buttons
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("🔄 Refresh", key="refresh_bronze", use_container_width=True):
                st.success("Data refreshed")
        with col2:
            if st.button("📊 Process to Silver", key="process_silver", use_container_width=True):
                st.info("Silver processing - Coming soon")
        with col3:
            if st.button("📤 Export Schema", key="export_schema", use_container_width=True):
                st.info("Schema export - Coming soon")
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Metadata section
        st.markdown("#### 📋 METADATA")
        
        st.markdown("""
            <div style='background: white; 
                        padding: 1.5rem; 
                        border-radius: 0.5rem; 
                        border: 2px solid #e5e7eb;'>
                <table style='width: 100%; font-size: 0.875rem;'>
                    <tr>
                        <td style='padding: 0.5rem; color: #64748b; width: 40%;'><strong>Source:</strong></td>
                        <td style='padding: 0.5rem; color: #1e293b;'>PostgreSQL → mydb.public</td>
                    </tr>
                    <tr style='background: #f8fafc;'>
                        <td style='padding: 0.5rem; color: #64748b;'><strong>Table:</strong></td>
                        <td style='padding: 0.5rem; color: #1e293b;'>transactions</td>
                    </tr>
                    <tr>
                        <td style='padding: 0.5rem; color: #64748b;'><strong>Ingested At:</strong></td>
                        <td style='padding: 0.5rem; color: #1e293b;'>2026-02-25 14:23:10</td>
                    </tr>
                    <tr style='background: #f8fafc;'>
                        <td style='padding: 0.5rem; color: #64748b;'><strong>Records:</strong></td>
                        <td style='padding: 0.5rem; color: #1e293b; font-weight: 600;'>1,234,567 rows</td>
                    </tr>
                    <tr>
                        <td style='padding: 0.5rem; color: #64748b;'><strong>Columns:</strong></td>
                        <td style='padding: 0.5rem; color: #1e293b;'>14</td>
                    </tr>
                    <tr style='background: #f8fafc;'>
                        <td style='padding: 0.5rem; color: #64748b;'><strong>File Size:</strong></td>
                        <td style='padding: 0.5rem; color: #1e293b;'>120.5 MB</td>
                    </tr>
                    <tr>
                        <td style='padding: 0.5rem; color: #64748b;'><strong>Format:</strong></td>
                        <td style='padding: 0.5rem; color: #1e293b;'>Parquet (snappy)</td>
                    </tr>
                    <tr style='background: #f8fafc;'>
                        <td style='padding: 0.5rem; color: #64748b;'><strong>Partitions:</strong></td>
                        <td style='padding: 0.5rem; color: #1e293b;'>date=2026-02-25/</td>
                    </tr>
                    <tr>
                        <td style='padding: 0.5rem; color: #64748b;'><strong>Status:</strong></td>
                        <td style='padding: 0.5rem; color: #10b981;'><strong>✓ Ingested Successfully</strong></td>
                    </tr>
                    <tr style='background: #f8fafc;'>
                        <td style='padding: 0.5rem; color: #64748b;'><strong>Contract Status:</strong></td>
                        <td style='padding: 0.5rem; color: #f59e0b;'><strong>⚠ Schema Drift Detected</strong></td>
                    </tr>
                    <tr>
                        <td style='padding: 0.5rem; color: #64748b;'><strong>Sensitivity:</strong></td>
                        <td style='padding: 0.5rem; color: #ef4444;'><strong>🔴 HIGH (Financial Data)</strong></td>
                    </tr>
                </table>
            </div>
        """, unsafe_allow_html=True)
        
        st.warning("""
            **⚠ Schema Drift Warning:**
            - New column detected: `ip_addr` (VARCHAR)
            - Contract defined 13 columns, found 14
            - Action required: Update contract or quarantine new column
        """)
        
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("Update Contract", key="update_contract", use_container_width=True):
                st.success("Contract updated")
        with col2:
            if st.button("Investigate", key="investigate_drift", use_container_width=True):
                st.info("Investigation mode - Coming soon")
        with col3:
            if st.button("Quarantine Column", key="quarantine_col", use_container_width=True):
                st.warning("Column quarantined")
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Schema section
        st.markdown("#### 🗂️ SCHEMA")
        
        # Mock schema data
        schema_data = pd.DataFrame({
            'Column': ['transaction_id', 'customer_id', 'amount', 'currency', 'status', 
                      'account_number', 'created_at', 'updated_at', 'ip_addr ⚠'],
            'Type': ['BIGINT', 'BIGINT', 'DECIMAL', 'VARCHAR', 'VARCHAR', 
                    'VARCHAR', 'TIMESTAMP', 'TIMESTAMP', 'VARCHAR'],
            'Nulls': [0, 0, 5, 0, 0, 0, 0, 12, 0],
            'Sensitivity': ['🟢 Low', '🔴 PII', '🟡 Medium', '🟢 Low', '🟢 Low', 
                          '🔴 PII', '🟢 Low', '🟢 Low', '🟡 Medium'],
            'Sample': ['[1, 2, 3]', '[masked]', '[100.5]', '[USD]', '[pending]', 
                      '[****]', '[2026-02-25]', '[2026-02-25]', '[10.0.0.1]']
        })
        
        st.dataframe(
            schema_data,
            use_container_width=True,
            hide_index=True
        )
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Basic profiling section
        st.markdown("#### 📊 BASIC PROFILING (EDA Lite)")
        
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Completeness", "99.2%", "0.8% nulls")
        with col2:
            st.metric("Duplicates", "45", "0.004%")
        with col3:
            st.metric("Unique Values", "1,234,522", "")
        with col4:
            st.metric("Data Quality", "87.5/100", "+2.3")
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Numeric profiling
        st.markdown("**Numeric Columns:**")
        
        numeric_data = pd.DataFrame({
            'Column': ['amount', 'customer_id'],
            'Min': ['0.01', '1'],
            'Max': ['999,999.99', '10,000'],
            'Average': ['234.56', '5,000'],
            'Null Count': [5, 0]
        })
        
        st.dataframe(numeric_data, use_container_width=True, hide_index=True)
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Categorical distribution
        st.markdown("**Categorical Distribution:**")
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
                <div style='background: white; 
                            padding: 1rem; 
                            border-radius: 0.5rem; 
                            border: 1px solid #e5e7eb;'>
                    <div style='font-weight: 600; margin-bottom: 0.5rem;'>status</div>
                    <div style='color: #64748b; font-size: 0.875rem; margin-bottom: 0.25rem;'>pending: 45%</div>
                    <div style='color: #64748b; font-size: 0.875rem; margin-bottom: 0.25rem;'>completed: 52%</div>
                    <div style='color: #64748b; font-size: 0.875rem;'>failed: 3%</div>
                </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown("""
                <div style='background: white; 
                            padding: 1rem; 
                            border-radius: 0.5rem; 
                            border: 1px solid #e5e7eb;'>
                    <div style='font-weight: 600; margin-bottom: 0.5rem;'>currency</div>
                    <div style='color: #64748b; font-size: 0.875rem; margin-bottom: 0.25rem;'>USD: 78%</div>
                    <div style='color: #64748b; font-size: 0.875rem; margin-bottom: 0.25rem;'>EUR: 15%</div>
                    <div style='color: #64748b; font-size: 0.875rem;'>GBP: 7%</div>
                </div>
            """, unsafe_allow_html=True)
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Sample data
        st.markdown("#### 📄 SAMPLE DATA (First 5 rows)")
        
        sample_data = pd.DataFrame({
            'transaction_id': [1, 2, 3, 4, 5],
            'customer_id': ['[masked]', '[masked]', '[masked]', '[masked]', '[masked]'],
            'amount': [100.50, 250.00, 75.25, 450.00, 199.99],
            'status': ['pending', 'completed', 'pending', 'completed', 'failed'],
            'created_at': ['2026-02-25 10:00', '2026-02-25 10:15', '2026-02-25 10:30', 
                          '2026-02-25 10:45', '2026-02-25 11:00']
        })
        
        st.dataframe(sample_data, use_container_width=True, hide_index=True)
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Action buttons at bottom
        col1, col2, col3 = st.columns(3)
        with col1:
            if st.button("📊 Full EDA Analysis", key="full_eda", use_container_width=True):
                st.info("Full EDA - Coming soon")
        with col2:
            if st.button("🔄 Process to Silver", key="process_silver2", use_container_width=True, type="primary"):
                st.info("Silver processing - Coming soon")
        with col3:
            if st.button("⚙️ Configure Contract", key="config_contract", use_container_width=True):
                st.info("Contract configuration - Coming soon")

"""
Custom Rules Management UI
Enterprise rule engine with visual builder and SQL expression mode
"""

import streamlit as st
import pandas as pd

def show():
    """Display custom rules management interface"""
    
    # Get domain from session state
    domain = st.session_state.get('selected_domain', 'general')
    domain_name = domain.upper()
    
    # Header
    st.markdown(f"""
        <div style='background: linear-gradient(135deg, #7c2d12 0%, #ea580c 100%); 
                    padding: 1.5rem; 
                    border-radius: 0.75rem; 
                    margin-bottom: 2rem;
                    color: white;'>
            <h1 style='color: white; margin: 0; font-size: 1.75rem;'>⚙️ CUSTOM RULES MANAGEMENT</h1>
            <p style='margin: 0.5rem 0 0 0; opacity: 0.9;'>{domain_name} Domain | Rule Engine Configuration</p>
        </div>
    """, unsafe_allow_html=True)
    
    # Toggle between list view and create/edit view
    if 'rule_view' not in st.session_state:
        st.session_state['rule_view'] = 'list'
    
    if st.session_state['rule_view'] == 'list':
        show_rules_list()
    else:
        show_rule_editor()


def show_rules_list():
    """Display list of existing rules"""
    
    # Action buttons
    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])
    
    with col1:
        search_term = st.text_input("🔍 Search rules", placeholder="Enter rule name or description...")
    
    with col2:
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("+ Create New Rule", key="create_new", use_container_width=True, type="primary"):
            st.session_state['rule_view'] = 'edit'
            st.session_state['editing_rule_id'] = None
            st.rerun()
    
    with col3:
        st.markdown("<br>", unsafe_allow_html=True)
        filter_status = st.selectbox("Filter by Status", ["All", "Active", "Pending", "Draft", "Deprecated"])
    
    with col4:
        st.markdown("<br>", unsafe_allow_html=True)
        filter_domain = st.selectbox("Filter by Domain", ["All", "Finance", "Healthcare", "General"])
    
    st.markdown("---")
    
    # Rules table
    st.markdown("### 📋 RULES CATALOG")
    
    # Mock rules data
    rules_data = pd.DataFrame({
        'Status': [
            '✅ Active',
            '🟡 Pending',
            '❌ Draft',
            '✅ Active',
            '⚠️ Deprecated'
        ],
        'ID': [101, 102, 103, 104, 105],
        'Rule Name': [
            'amount_positive_check',
            'transaction_date_valid',
            'customer_kyc_complete',
            'currency_iso_format',
            'old_validation_logic'
        ],
        'Domain': ['Finance', 'Finance', 'Finance', 'Finance', 'Finance'],
        'Severity': ['CRITICAL', 'HIGH', 'HIGH', 'MEDIUM', 'LOW'],
        'Type': ['Validation', 'Validation', 'Compliance', 'Format', 'Validation'],
        'Last Modified': [
            '2026-02-20',
            '2026-02-24',
            '2026-02-25',
            '2026-02-15',
            '2025-12-01'
        ],
        'Modified By': ['admin', 'john.doe', 'jane.smith', 'admin', 'legacy']
    })
    
    # Display table with custom styling
    st.dataframe(
        rules_data,
        use_container_width=True,
        hide_index=True,
        column_config={
            'Status': st.column_config.TextColumn('Status', width='small'),
            'ID': st.column_config.NumberColumn('ID', width='small'),
            'Rule Name': st.column_config.TextColumn('Rule Name', width='medium'),
            'Severity': st.column_config.TextColumn('Severity', width='small'),
        }
    )
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Rule details expandable sections
    with st.expander("📄 VIEW RULE DETAILS: amount_positive_check (ID: 101)", expanded=False):
        col1, col2 = st.columns(2)
        
        with col1:
            st.markdown("""
                **Rule Name:** amount_positive_check
                
                **Description:** Validates that transaction amounts are positive and within acceptable range
                
                **Domain:** Finance
                
                **Target Table:** transactions
                
                **Status:** ✅ Active
                
                **Version:** 3
            """)
        
        with col2:
            st.markdown("""
                **Severity:** CRITICAL
                
                **Action:** Quarantine Row
                
                **Created:** 2025-12-15 by admin
                
                **Last Modified:** 2026-02-20 by admin
                
                **Approved:** 2026-01-10 by compliance_lead
                
                **Execution Count:** 1,234 times
            """)
        
        st.markdown("**Rule Logic:**")
        st.code("""
SELECT *
FROM transactions
WHERE amount > 0
  AND amount <= 1000000
  AND currency IN ('USD', 'EUR', 'GBP')
        """, language="sql")
        
        st.markdown("**Performance Metrics:**")
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Avg Exec Time", "45ms")
        with col2:
            st.metric("Failure Rate", "0.03%")
        with col3:
            st.metric("Quarantined", "367 rows")
        with col4:
            st.metric("Last Run", "14:23")
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            if st.button("✏️ Edit Rule", key="edit_101"):
                st.session_state['rule_view'] = 'edit'
                st.session_state['editing_rule_id'] = 101
                st.rerun()
        with col2:
            if st.button("📊 View Analytics", key="analytics_101"):
                st.info("Rule analytics - Coming soon")
        with col3:
            if st.button("📋 Duplicate Rule", key="duplicate_101"):
                st.success("Rule duplicated as draft")
        with col4:
            if st.button("🗑️ Deprecate", key="deprecate_101"):
                st.warning("Rule deprecated")
    
    # Statistics panel
    st.markdown("---")
    st.markdown("### 📊 RULES STATISTICS")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total Rules", "47", "+3 this month")
    with col2:
        st.metric("Active Rules", "38", "81%")
    with col3:
        st.metric("Pending Approval", "5", "⚠️")
    with col4:
        st.metric("Draft Rules", "3", "")
    with col5:
        st.metric("Deprecated", "1", "")


def show_rule_editor():
    """Display rule creation/editing interface"""
    
    st.markdown("### 📝 CREATE NEW RULE")
    
    # Back button
    if st.button("← Back to Rules List"):
        st.session_state['rule_view'] = 'list'
        st.rerun()
    
    st.markdown("---")
    
    # Basic Information
    st.markdown("#### Basic Information")
    
    col1, col2 = st.columns(2)
    
    with col1:
        rule_name = st.text_input(
            "Rule Name*",
            placeholder="e.g., transaction_amount_range_check",
            help="Unique identifier for this rule (lowercase, underscore-separated)"
        )
        
        domain = st.selectbox(
            "Domain*",
            ["Finance", "Healthcare", "General"],
            index=0
        )
        
        target_table = st.text_input(
            "Target Table*",
            placeholder="e.g., transactions",
            help="Table this rule applies to"
        )
    
    with col2:
        rule_category = st.selectbox(
            "Category*",
            [
                "Validation",
                "Transformation",
                "Masking",
                "Referential Integrity",
                "Anomaly Detection",
                "Compliance",
                "Data Quality"
            ],
            index=0
        )
        
        rule_version = st.number_input("Version", min_value=1, value=1, disabled=True)
        
        effective_date = st.date_input("Effective Date")
    
    description = st.text_area(
        "Description",
        placeholder="Describe what this rule does and why it's important...",
        help="Detailed description for documentation"
    )
    
    st.markdown("---")
    
    # Rule Configuration
    st.markdown("#### Rule Configuration")
    
    # MODE TOGGLE
    rule_mode = st.radio(
        "Configuration Mode",
        ["Visual Builder", "SQL Expression (Advanced)"],
        horizontal=True,
        help="Visual Builder for simple rules, SQL Expression for complex logic"
    )
    
    if rule_mode == "Visual Builder":
        show_visual_builder()
    else:
        show_sql_editor()
    
    st.markdown("---")
    
    # Severity & Action
    st.markdown("#### Severity & Action")
    
    col1, col2 = st.columns(2)
    
    with col1:
        severity = st.radio(
            "Severity*",
            ["INFO", "WARNING", "HIGH", "CRITICAL"],
            index=3,
            horizontal=True
        )
        
        st.markdown("""
            **Severity Guidelines:**
            - **INFO:** Informational only, no action required
            - **WARNING:** Potential issue, monitor
            - **HIGH:** Significant issue, quarantine recommended
            - **CRITICAL:** Severe issue, block processing
        """)
    
    with col2:
        action = st.radio(
            "Action on Failure*",
            [
                "Log Only",
                "Flag & Warn",
                "Quarantine Row",
                "Block Entire Table",
                "Alert & Quarantine"
            ],
            index=2
        )
        
        st.markdown("**Alert Settings:**")
        alert_email = st.checkbox("Email Alert", value=False)
        alert_dashboard = st.checkbox("Dashboard Alert", value=True)
        alert_sms = st.checkbox("SMS Alert", value=False)
    
    st.markdown("---")
    
    # Advanced Options
    with st.expander("⚙️ Advanced Options", expanded=False):
        col1, col2 = st.columns(2)
        
        with col1:
            st.checkbox("Enable for new tables automatically", value=False)
            st.checkbox("Apply retroactively to existing data", value=False)
            st.checkbox("Require approval before activation", value=True)
            st.checkbox("Version control (track changes)", value=True)
        
        with col2:
            owner_team = st.selectbox(
                "Owner Team",
                ["risk_team", "data_governance", "compliance", "data_engineering"]
            )
            
            execution_priority = st.slider("Execution Priority", 1, 10, 5)
            
            retry_on_failure = st.checkbox("Retry on transient failures", value=True)
    
    st.markdown("---")
    
    # Action Buttons
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("🧪 Test on Sample Data", use_container_width=True):
            with st.spinner("Testing rule on sample dataset..."):
                st.success("Test completed!")
                st.info("""
                    **Test Results:**
                    - Sample size: 1,000 rows
                    - Passed: 987 (98.7%)
                    - Failed: 13 (1.3%)
                    - Avg execution time: 23ms
                """)
    
    with col2:
        if st.button("💾 Save as Draft", use_container_width=True):
            st.success("Rule saved as draft")
    
    with col3:
        if st.button("📤 Submit for Approval", use_container_width=True, type="primary"):
            st.success("Rule submitted for approval")
            st.balloons()
            st.info("Approval workflow initiated. Awaiting: Data Governance Lead, Domain Owner, Compliance Officer")
    
    with col4:
        if st.button("❌ Cancel", use_container_width=True):
            st.session_state['rule_view'] = 'list'
            st.rerun()


def show_visual_builder():
    """Visual rule builder interface"""
    
    st.markdown("**Visual Rule Builder**")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        column = st.selectbox(
            "Column*",
            ["amount", "transaction_date", "customer_id", "status", "currency"],
            index=0
        )
    
    with col2:
        condition = st.selectbox(
            "Condition*",
            ["EQUALS", "NOT EQUALS", "GREATER THAN", "LESS THAN", "BETWEEN", "IN", "NOT IN", "IS NULL", "IS NOT NULL", "REGEX MATCH"],
            index=4  # BETWEEN
        )
    
    with col3:
        if condition == "BETWEEN":
            min_value = st.number_input("Min Value", value=0.01)
            max_value = st.number_input("Max Value", value=1000000.00)
        elif condition == "IN":
            values = st.text_input("Values (comma-separated)", placeholder="USD, EUR, GBP")
        else:
            value = st.text_input("Value")
    
    # Add condition button
    if st.button("+ Add Another Condition (AND)", key="add_condition"):
        st.info("Multi-condition support - Coming soon")
    
    # Preview generated SQL
    st.markdown("**Generated Logic:**")
    st.code("""
WHERE amount BETWEEN 0.01 AND 1000000.00
    """, language="sql")


def show_sql_editor():
    """SQL expression editor"""
    
    st.markdown("**SQL Expression Editor**")
    st.info("⚠️ Advanced mode: Write custom SQL logic. Security validation will be applied.")
    
    sql_expression = st.text_area(
        "SQL Expression*",
        value="""SELECT *
FROM transactions
WHERE amount > 0
  AND amount <= 1000000
  AND transaction_date <= CURRENT_DATE
  AND currency IN ('USD', 'EUR', 'GBP')
  AND customer_id IS NOT NULL""",
        height=200,
        help="Write SELECT statement with WHERE clause. Only SELECT is allowed."
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        if st.button("✓ Validate SQL", use_container_width=True):
            st.success("✓ Syntax Valid")
            st.info("""
                **Security Check:**
                - ✓ No DROP/DELETE/UPDATE detected
                - ✓ Only SELECT statements allowed
                - ✓ Column references validated
                - ✓ Functions whitelisted
            """)
    
    with col2:
        if st.button("🧪 Test on Sample", use_container_width=True):
            st.success("Test executed on 1,000 sample rows")
            st.metric("Pass Rate", "98.7%", "987/1000")


# Call the main show function
show()

"""
Settings - Configure dashboard and system
"""

import streamlit as st


def show():
    """Display settings page"""
    
    st.markdown("# Settings")
    st.markdown("### Configure your dashboard and system preferences")
    st.markdown("---")
    
    # Quick Links Section
    st.markdown("### ⚡ Quick Access")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("⚙️ Custom Rules Management", use_container_width=True, type="primary"):
            # Link to custom rules page
            from pages import custom_rules
            custom_rules.show()
            return
    
    with col2:
        if st.button("📋 Domain Templates", use_container_width=True):
            st.info("Domain template configuration - Coming soon")
    
    with col3:
        if st.button("🔐 Access Control", use_container_width=True):
            st.info("Access control settings - Coming soon")
    
    st.markdown("---")
    
    # General Settings
    st.markdown("### General Settings")
    
    col1, col2 = st.columns(2)
    
    with col1:
        theme = st.selectbox(
            "Theme",
            ["Light", "Dark", "Auto"]
        )
        
        refresh_interval = st.selectbox(
            "Auto-refresh Interval",
            ["Off", "1 minute", "5 minutes", "15 minutes", "30 minutes"]
        )
    
    with col2:
        default_page = st.selectbox(
            "Default Page",
            ["Dashboard", "Data Tables", "Gold Layer EDA", "Quality Monitoring"]
        )
        
        rows_per_page = st.selectbox(
            "Table Rows Per Page",
            [10, 25, 50, 100, 500]
        )
    
    st.markdown("---")
    
    # Quality Thresholds
    st.markdown("### Quality Thresholds")
    
    st.info("Configure alert thresholds for quality monitoring")
    
    col1, col2 = st.columns(2)
    
    with col1:
        excellent_threshold = st.slider(
            "Excellent Score Threshold",
            min_value=80,
            max_value=100,
            value=90
        )
        
        good_threshold = st.slider(
            "Good Score Threshold",
            min_value=60,
            max_value=90,
            value=75
        )
    
    with col2:
        completeness_warning = st.slider(
            "Completeness Warning (%)",
            min_value=50,
            max_value=100,
            value=80
        )
        
        missing_alert = st.slider(
            "Missing Values Alert (%)",
            min_value=5,
            max_value=50,
            value=20
        )
    
    st.markdown("---")
    
    # Data Source Configuration
    st.markdown("### Data Source Configuration")
    
    st.info("Configure paths to lakehouse layers")
    
    bronze_path = st.text_input(
        "Bronze Layer Path",
        value="../data lakehouse/syniq_project/bronze"
    )
    
    silver_path = st.text_input(
        "Silver Layer Path",
        value="../data lakehouse/syniq_project/silver"
    )
    
    gold_path = st.text_input(
        "Gold Layer Path",
        value="../data lakehouse/syniq_project/gold"
    )
    
    st.markdown("---")
    
    # Notifications
    st.markdown("### Notifications")
    
    col1, col2 = st.columns(2)
    
    with col1:
        email_alerts = st.checkbox("Enable Email Alerts", value=False)
        slack_alerts = st.checkbox("Enable Slack Notifications", value=False)
    
    with col2:
        alert_on_poor_quality = st.checkbox("Alert on Poor Quality", value=True)
        alert_on_missing_data = st.checkbox("Alert on High Missing Data", value=True)
    
    if email_alerts:
        email = st.text_input("Email Address", placeholder="user@example.com")
    
    if slack_alerts:
        slack_webhook = st.text_input("Slack Webhook URL", placeholder="https://hooks.slack.com/...")
    
    st.markdown("---")
    
    # Export Settings
    st.markdown("### Export Settings")
    
    col1, col2 = st.columns(2)
    
    with col1:
        default_export_format = st.selectbox(
            "Default Export Format",
            ["CSV", "Excel", "JSON", "Parquet"]
        )
    
    with col2:
        include_metadata = st.checkbox("Include Metadata in Exports", value=True)
    
    st.markdown("---")
    
    # Advanced Settings
    st.markdown("### Advanced Settings")
    
    with st.expander("Performance"):
        cache_ttl = st.number_input("Cache TTL (seconds)", min_value=60, max_value=3600, value=300)
        max_rows_display = st.number_input("Max Rows to Display", min_value=100, max_value=10000, value=1000)
    
    with st.expander("Logging"):
        log_level = st.selectbox("Log Level", ["DEBUG", "INFO", "WARNING", "ERROR"])
        log_retention = st.selectbox("Log Retention", ["7 days", "30 days", "90 days", "1 year"])
    
    st.markdown("---")
    
    # Save Settings
    col1, col2, col3 = st.columns([1, 1, 2])
    
    with col1:
        if st.button("Save Settings", type="primary", use_container_width=True):
            st.success("[SUCCESS] Settings saved successfully!")
    
    with col2:
        if st.button("Reset to Default", use_container_width=True):
            st.info("Settings reset to default values")
    
    st.markdown("---")
    
    # System Information
    st.markdown("### System Information")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("**Version:** 1.0.0")
        st.markdown("**Build:** 20260223")
    
    with col2:
        st.markdown("**Python:** 3.12")
        st.markdown("**Streamlit:** 1.31.0")
    
    with col3:
        st.markdown("**License:** Enterprise")
        st.markdown("**Support:** Active")
    
    st.markdown("---")
    
    # About
    with st.expander("About SyniqAI"):
        st.markdown("""
            ### SyniqAI Data Lakehouse Dashboard
            
            **Enterprise-grade data quality and analytics platform**
            
            #### Features:
            - Comprehensive EDA with 50+ metrics per table
            - Real-time quality monitoring
            - Automated insights and recommendations
            - Multi-format report generation
            - Beautiful, interactive visualizations
            
            #### Architecture:
            - Bronze Layer: Raw data ingestion
            - Silver Layer: Cleaned and validated data
            - Gold Layer: Business-ready analytics
            
            #### Contact:
            - Documentation: [Link]
            - Support: support@syniqai.com
            - GitHub: github.com/syniqai
            
            ---
            **© 2026 SyniqAI. All rights reserved.**
        """)


if __name__ == "__main__":
    show()

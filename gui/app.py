"""
SyniqAI Data Lakehouse Dashboard
Enterprise-grade data quality and EDA visualization platform
"""

import streamlit as st
from pathlib import Path
import sys

# Add project paths
PROJECT_ROOT = Path(__file__).parent.parent / "data lakehouse" / "syniq_project"
sys.path.insert(0, str(PROJECT_ROOT))

# Page configuration
st.set_page_config(
    page_title="SyniqAI Data Lakehouse",
    page_icon="⬣",
    layout="wide",
    initial_sidebar_state="expanded" if 'selected_domain' in st.session_state else "collapsed"
)

# Initialize session state
if 'selected_domain' not in st.session_state:
    st.session_state['selected_domain'] = None
if 'page' not in st.session_state:
    st.session_state['page'] = 'dashboard'

# Custom CSS for enterprise look
st.markdown("""
    <style>
    /* Main theme */
    .main {
        background-color: #f8f9fa;
    }
    
    /* Metric cards */
    .metric-card {
        background: white;
        padding: 1.5rem;
        border-radius: 0.5rem;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin-bottom: 1rem;
    }
    
    /* Headers */
    h1 {
        color: #1e40af;
        font-weight: 600;
    }
    
    h2 {
        color: #3b82f6;
        font-weight: 500;
    }
    
    h3 {
        color: #6366f1;
        font-weight: 500;
    }
    
    /* Sidebar */
    .css-1d391kg {
        background-color: #1e293b;
    }
    
    /* Quality score badges */
    .quality-excellent {
        color: #10b981;
        font-weight: bold;
        font-size: 2rem;
    }
    
    .quality-good {
        color: #3b82f6;
        font-weight: bold;
        font-size: 2rem;
    }
    
    .quality-fair {
        color: #f59e0b;
        font-weight: bold;
        font-size: 2rem;
    }
    
    .quality-poor {
        color: #ef4444;
        font-weight: bold;
        font-size: 2rem;
    }
    
    /* Info boxes */
    .stAlert {
        border-radius: 0.5rem;
    }
    
    /* Buttons */
    .stButton>button {
        border-radius: 0.375rem;
        font-weight: 500;
    }
    
    /* Data tables */
    .dataframe {
        border-radius: 0.5rem;
    }
    </style>
""", unsafe_allow_html=True)

# Check if domain is selected
if not st.session_state['selected_domain']:
    # Show domain selection page
    from pages import domain_selection
    domain_selection.show()
else:
    # Domain selected - show sidebar and main dashboard
    # Sidebar
    with st.sidebar:
        st.image("https://via.placeholder.com/200x60/3b82f6/ffffff?text=SyniqAI", use_container_width=True)
        st.markdown("---")
        
        # Current domain
        domain_icons = {
            'finance': '$',
            'healthcare': '+',
            'general': '◈'
        }
        domain_colors = {
            'finance': '#10b981',
            'healthcare': '#ef4444',
            'general': '#3b82f6'
        }
        current_domain = st.session_state['selected_domain']
        domain_icon = domain_icons.get(current_domain, '◈')
        domain_color = domain_colors.get(current_domain, '#3b82f6')
        
        st.markdown(f"""
            <div style='background: linear-gradient(135deg, {domain_color}15, {domain_color}05); 
                        border-left: 4px solid {domain_color}; 
                        padding: 1rem; 
                        border-radius: 0.5rem;
                        margin-bottom: 1rem;'>
                <div style='font-size: 1.5rem; text-align: center;'>{domain_icon}</div>
                <div style='font-weight: 600; 
                           text-align: center; 
                           color: {domain_color};
                           text-transform: uppercase;
                           letter-spacing: 1px;'>
                    {current_domain}
                </div>
            </div>
        """, unsafe_allow_html=True)
        
        if st.button("← Change Domain", use_container_width=True):
            st.session_state['selected_domain'] = None
            st.rerun()
        
        st.markdown("---")
        
        st.markdown("### Navigation")
        page = st.radio(
            "Select Page",
            [
                "Dashboard",
                "Data Ingestion",
                "Bronze Layer",
                "Silver Layer",
                "Gold Layer EDA",
                "Quality Monitoring",
                "Reports",
                "Settings"
            ],
            label_visibility="collapsed"
        )
        
        st.markdown("---")
        st.markdown("### System Status")
        st.metric("Pipeline", "ACTIVE")
        st.metric("Last Run", "Today 14:20")
        
        st.markdown("---")
        st.markdown("### Quick Links")
        st.markdown("- [Documentation]()")
        st.markdown("- [API Reference]()")
        st.markdown("- [Support]()")

    # Main content routing
    if "Dashboard" in page:
        from pages import dashboard_lakehouse
        dashboard_lakehouse.show()
    elif "Data Ingestion" in page:
        st.title("🔄 Data Ingestion")
        st.info("Data Ingestion page coming soon...")
    elif "Bronze Layer" in page:
        from pages import bronze_explorer
        bronze_explorer.show()
    elif "Silver Layer" in page:
        from pages import silver_workspace
        silver_workspace.show()
    elif "Gold Layer EDA" in page:
        from pages import gold_eda
        gold_eda.show()
    elif "Quality Monitoring" in page:
        from pages import quality_monitoring
        quality_monitoring.show()
    elif "Reports" in page:
        from pages import reports
        reports.show()
    elif "Settings" in page:
        from pages import settings
        settings.show()

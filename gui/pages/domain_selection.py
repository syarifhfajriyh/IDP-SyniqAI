"""
Domain Selection Page
Enterprise data lakehouse - domain-based entry point
"""

import streamlit as st

def show():
    """Display domain selection page"""
    
    # Custom CSS for domain selection
    st.markdown("""
        <style>
        .domain-header {
            text-align: center;
            padding: 2rem 0;
        }
        
        .domain-title {
            font-size: 2.5rem;
            font-weight: 600;
            color: #1e40af;
            margin-bottom: 0.5rem;
        }
        
        .domain-subtitle {
            font-size: 1.25rem;
            color: #64748b;
            font-weight: 400;
        }
        
        .domain-card {
            background: white;
            border: 2px solid #e2e8f0;
            border-radius: 12px;
            padding: 3rem 2rem;
            text-align: center;
            transition: all 0.3s ease;
            cursor: pointer;
            height: 100%;
            min-height: 300px;
            display: flex;
            flex-direction: column;
            justify-content: center;
            align-items: center;
        }
        
        .domain-card:hover {
            border-color: #3b82f6;
            box-shadow: 0 10px 25px rgba(59, 130, 246, 0.15);
            transform: translateY(-4px);
        }
        
        .domain-icon {
            font-size: 4rem;
            margin-bottom: 1.5rem;
            color: #3b82f6;
        }
        
        .domain-name {
            font-size: 1.75rem;
            font-weight: 600;
            color: #1e293b;
            margin-bottom: 1rem;
        }
        
        .domain-button {
            margin-top: 1.5rem;
            width: 100%;
        }
        
        /* Hide default streamlit elements */
        #MainMenu {visibility: hidden;}
        footer {visibility: hidden;}
        header {visibility: hidden;}
        </style>
    """, unsafe_allow_html=True)
    
    # Header
    st.markdown("""
        <div class="domain-header">
            <div class="domain-title">⬣ SyniqAI Data Lakehouse</div>
            <div class="domain-subtitle">Select Your Data Domain</div>
        </div>
    """, unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Domain Cards
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
            <div class="domain-card">
                <div class="domain-icon">$</div>
                <div class="domain-name">FINANCE</div>
            </div>
        """, unsafe_allow_html=True)
        if st.button("Select Domain", key="finance", use_container_width=True, type="primary"):
            st.session_state['selected_domain'] = 'finance'
            st.session_state['page'] = 'dashboard'
            st.rerun()
    
    with col2:
        st.markdown("""
            <div class="domain-card">
                <div class="domain-icon">+</div>
                <div class="domain-name">HEALTHCARE</div>
            </div>
        """, unsafe_allow_html=True)
        if st.button("Select Domain", key="healthcare", use_container_width=True, type="primary"):
            st.session_state['selected_domain'] = 'healthcare'
            st.session_state['page'] = 'dashboard'
            st.rerun()
    
    with col3:
        st.markdown("""
            <div class="domain-card">
                <div class="domain-icon">◈</div>
                <div class="domain-name">GENERAL</div>
            </div>
        """, unsafe_allow_html=True)
        if st.button("Select Domain", key="general", use_container_width=True, type="primary"):
            st.session_state['selected_domain'] = 'general'
            st.session_state['page'] = 'dashboard'
            st.rerun()
    
    # Info section
    st.markdown("<br><br>", unsafe_allow_html=True)
    st.info("""
        **Domain Selection Impact:**
        - Pre-configured rule templates for your industry
        - Domain-specific data validation and quality checks
        - Compliance and governance settings
        - Optimized data classification policies
    """)
    
    # Version info
    st.markdown("<br>", unsafe_allow_html=True)
    st.markdown("""
        <div style='text-align: center; color: #94a3b8; font-size: 0.875rem;'>
            SyniqAI Lakehouse Platform v1.0.0 | Enterprise Data Governance
        </div>
    """, unsafe_allow_html=True)

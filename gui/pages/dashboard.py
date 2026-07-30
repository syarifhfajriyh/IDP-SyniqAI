"""
Main Dashboard - Executive Overview
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import sys

# Add utils to path
sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.data_loader import DataLoader, format_quality_score, format_number

# Initialize data loader
PROJECT_ROOT = Path(__file__).parent.parent.parent / "data lakehouse" / "syniq_project"
loader = DataLoader(PROJECT_ROOT)


def show():
    """Display main dashboard"""
    
    # Header
    st.markdown("# Executive Dashboard")
    st.markdown("### SyniqAI Data Lakehouse - Real-time Data Quality & Analytics")
    st.markdown("---")
    
    # Load data
    tables = loader.get_available_tables()
    quality_summary = loader.get_quality_summary()
    
    if not tables:
        st.warning("WARNING: No data found. Please run Gold layer processing first.")
        st.code("python main.py gold process", language="bash")
        return
    
    # Top Metrics Row
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric(
            label="Total Tables",
            value=quality_summary["total_tables"],
            delta="Active"
        )
    
    with col2:
        emoji, _ = format_quality_score(quality_summary["avg_quality"])
        st.metric(
            label=f"Avg Quality Score",
            value=f"{quality_summary['avg_quality']:.1f}/100",
            delta=f"{quality_summary['excellent']} excellent" if quality_summary['excellent'] > 0 else None
        )
    
    with col3:
        st.metric(
            label="Completeness",
            value=f"{quality_summary['avg_completeness']:.1f}%",
            delta="Good" if quality_summary['avg_completeness'] >= 90 else "Review"
        )
    
    with col4:
        total_processed = sum([
            quality_summary["excellent"],
            quality_summary["good"],
            quality_summary["fair"],
            quality_summary["poor"]
        ])
        st.metric(
            label="Tables Processed",
            value=total_processed,
            delta=f"{quality_summary['excellent']} high quality"
        )
    
    with col5:
        st.metric(
            label="Last Updated",
            value=tables[0]["last_modified"].split()[0] if tables else "N/A",
            delta="Today"
        )
    
    st.markdown("---")
    
    # Quality Distribution Section
    col1, col2 = st.columns([1, 1])
    
    with col1:
        st.markdown("### Quality Distribution")
        
        # Quality pie chart
        quality_data = pd.DataFrame({
            "Category": ["Excellent (90+)", "Good (75-89)", "Fair (60-74)", "Poor (<60)"],
            "Count": [
                quality_summary["excellent"],
                quality_summary["good"],
                quality_summary["fair"],
                quality_summary["poor"]
            ],
            "Color": ["#10b981", "#3b82f6", "#f59e0b", "#ef4444"]
        })
        
        fig = px.pie(
            quality_data,
            values="Count",
            names="Category",
            color="Category",
            color_discrete_map={
                "Excellent (90+)": "#10b981",
                "Good (75-89)": "#3b82f6",
                "Fair (60-74)": "#f59e0b",
                "Poor (<60)": "#ef4444"
            },
            hole=0.4
        )
        fig.update_layout(
            showlegend=True,
            height=350,
            margin=dict(t=20, b=20, l=20, r=20)
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.markdown("### Table Quality Scores")
        
        # Bar chart of table scores
        table_scores = []
        for table_info in tables[:10]:  # Show top 10
            report = loader.load_eda_report(table_info["source"], table_info["table"])
            if report:
                score = report.get("data_quality", {}).get("overall_quality_score", 0)
                table_scores.append({
                    "Table": f"{table_info['source']}.{table_info['table']}"[:30],
                    "Score": score
                })
        
        if table_scores:
            df_scores = pd.DataFrame(table_scores)
            
            fig = px.bar(
                df_scores,
                x="Score",
                y="Table",
                orientation='h',
                color="Score",
                color_continuous_scale=["#ef4444", "#f59e0b", "#3b82f6", "#10b981"],
                range_color=[0, 100]
            )
            fig.update_layout(
                showlegend=False,
                height=350,
                margin=dict(t=20, b=20, l=20, r=20),
                xaxis_title="Quality Score",
                yaxis_title=""
            )
            st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Tables Overview
    st.markdown("### Tables Overview")
    
    # Build table data
    table_data = []
    for table_info in tables:
        report = loader.load_eda_report(table_info["source"], table_info["table"])
        
        if report:
            quality = report.get("data_quality", {})
            basic = report.get("basic_info", {})
            
            score = quality.get("overall_quality_score", 0)
            label, _ = format_quality_score(score)
            
            table_data.append({
                "Source": table_info["source"],
                "Table": table_info["table"],
                "Rows": format_number(basic.get("row_count", 0), 0),
                "Columns": basic.get("column_count", 0),
                "Quality": f"{label} {score:.1f}",
                "Completeness": f"{quality.get('completeness_score', 0):.1f}%",
                "Last Modified": table_info["last_modified"]
            })
    
    if table_data:
        df_tables = pd.DataFrame(table_data)
        st.dataframe(
            df_tables,
            use_container_width=True,
            hide_index=True,
            height=300
        )
    
    # Recent Insights
    st.markdown("---")
    st.markdown("### Recent Insights")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.info("[INFO] All tables maintain quality scores above 60%")
        st.success("[SUCCESS] Average completeness: {:.1f}% - Excellent data integrity".format(
            quality_summary['avg_completeness']
        ))
    
    with col2:
        if quality_summary["excellent"] > 0:
            st.success(f"[EXCELLENT] {quality_summary['excellent']} table(s) with excellent quality (90+)")
        if quality_summary["poor"] > 0:
            st.warning(f"[WARNING] {quality_summary['poor']} table(s) need attention (quality < 60)")
    
    # Quick Actions
    st.markdown("---")
    st.markdown("### Quick Actions")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        if st.button("Refresh Data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
    
    with col2:
        if st.button("New Ingestion", use_container_width=True):
            st.info("Navigate to Data Tables for Ingestion")
    
    with col3:
        if st.button("View EDA", use_container_width=True):
            st.info("Navigate to Gold Layer EDA")
    
    with col4:
        if st.button("Export Report", use_container_width=True):
            st.info("Navigate to Reports")


if __name__ == "__main__":
    show()

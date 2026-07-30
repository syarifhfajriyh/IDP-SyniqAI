"""
Quality Monitoring - Track data quality over time
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.data_loader import DataLoader, format_quality_score

PROJECT_ROOT = Path(__file__).parent.parent.parent / "data lakehouse" / "syniq_project"
loader = DataLoader(PROJECT_ROOT)


def show():
    """Display quality monitoring page"""
    
    st.markdown("# Quality Monitoring")
    st.markdown("### Track data quality trends and alerts")
    st.markdown("---")
    
    # Load data
    tables = loader.get_available_tables()
    quality_summary = loader.get_quality_summary()
    
    if not tables:
        st.warning("WARNING: No data found. Please run Gold layer processing first.")
        return
    
    # Quality Overview
    st.markdown("### Quality Score Distribution")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Excellent",
            quality_summary["excellent"],
            f"{quality_summary['excellent']/quality_summary['total_tables']*100:.0f}%"
        )
    
    with col2:
        st.metric(
            "Good",
            quality_summary["good"],
            f"{quality_summary['good']/quality_summary['total_tables']*100:.0f}%"
        )
    
    with col3:
        st.metric(
            "Fair",
            quality_summary["fair"],
            f"{quality_summary['fair']/quality_summary['total_tables']*100:.0f}%"
        )
    
    with col4:
        st.metric(
            "Poor",
            quality_summary["poor"],
            f"{quality_summary['poor']/quality_summary['total_tables']*100:.0f}%"
        )
    
    st.markdown("---")
    
    # Quality Trends Chart
    st.markdown("### Quality Scores by Table")
    
    # Build quality data
    quality_data = []
    for table_info in tables:
        report = loader.load_eda_report(table_info["source"], table_info["table"])
        if report:
            quality = report.get("data_quality", {})
            quality_data.append({
                "Table": f"{table_info['source']}.{table_info['table']}",
                "Quality Score": quality.get("overall_quality_score", 0),
                "Completeness": quality.get("completeness_score", 0),
                "Missing %": quality.get("missing_percentage", 0),
                "Duplicate %": quality.get("duplicate_percentage", 0)
            })
    
    if quality_data:
        df_quality = pd.DataFrame(quality_data)
        
        # Interactive scatter plot
        fig = px.scatter(
            df_quality,
            x="Completeness",
            y="Quality Score",
            size="Missing %",
            color="Quality Score",
            hover_data=["Table", "Duplicate %"],
            color_continuous_scale=["#ef4444", "#f59e0b", "#3b82f6", "#10b981"],
            title="Quality Score vs Completeness"
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")
        
        # Detailed table
        st.markdown("### Detailed Quality Metrics")
        
        # Add quality category
        df_quality["Category"] = df_quality["Quality Score"].apply(
            lambda x: "Excellent" if x >= 90 else
                     "Good" if x >= 75 else
                     "Fair" if x >= 60 else
                     "Poor"
        )
        
        # Sort by quality score
        df_quality = df_quality.sort_values("Quality Score", ascending=False)
        
        st.dataframe(
            df_quality[[
                "Table", "Category", "Quality Score", 
                "Completeness", "Missing %", "Duplicate %"
            ]],
            use_container_width=True,
            hide_index=True,
            height=400
        )
    
    st.markdown("---")
    
    # Alerts and Recommendations
    st.markdown("### Quality Alerts")
    
    alerts = []
    
    for table_info in tables:
        report = loader.load_eda_report(table_info["source"], table_info["table"])
        if report:
            quality = report.get("data_quality", {})
            table_name = f"{table_info['source']}.{table_info['table']}"
            
            # Check for issues
            score = quality.get("overall_quality_score", 100)
            completeness = quality.get("completeness_score", 100)
            missing_pct = quality.get("missing_percentage", 0)
            dup_pct = quality.get("duplicate_percentage", 0)
            
            if score < 60:
                alerts.append({
                    "severity": "[CRITICAL]",
                    "table": table_name,
                    "issue": f"Low quality score: {score:.1f}/100",
                    "action": "Review data integrity"
                })
            elif score < 75:
                alerts.append({
                    "severity": "[WARNING]",
                    "table": table_name,
                    "issue": f"Quality score below threshold: {score:.1f}/100",
                    "action": "Consider data cleanup"
                })
            
            if completeness < 80:
                alerts.append({
                    "severity": "[WARNING]",
                    "table": table_name,
                    "issue": f"Low completeness: {completeness:.1f}%",
                    "action": "Review missing values"
                })
            
            if missing_pct > 20:
                alerts.append({
                    "severity": "[WARNING]",
                    "table": table_name,
                    "issue": f"High missing data: {missing_pct:.1f}%",
                    "action": "Validate data collection"
                })
            
            if dup_pct > 5:
                alerts.append({
                    "severity": "[INFO]",
                    "table": table_name,
                    "issue": f"Duplicates detected: {dup_pct:.1f}%",
                    "action": "Review ingestion logic"
                })
    
    if alerts:
        df_alerts = pd.DataFrame(alerts)
        st.dataframe(df_alerts, use_container_width=True, hide_index=True, height=300)
    else:
        st.success("[SUCCESS] No quality issues detected - all tables are in good shape!")
    
    st.markdown("---")
    
    # Quality Rules
    st.markdown("### Quality Thresholds")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("**Score Categories:**")
        st.markdown("- **Excellent:** 90-100")
        st.markdown("- **Good:** 75-89")
        st.markdown("- **Fair:** 60-74")
        st.markdown("- **Poor:** < 60")
    
    with col2:
        st.markdown("**Alert Triggers:**")
        st.markdown("- Quality score < 60")
        st.markdown("- Completeness < 80%")
        st.markdown("- Missing values > 20%")
        st.markdown("- Duplicates > 5%")
    
    st.markdown("---")
    
    # Actions
    st.markdown("### Quick Actions")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("Refresh Metrics", use_container_width=True):
            st.cache_data.clear()
            st.rerun()
    
    with col2:
        if st.button("Export Report", use_container_width=True):
            if quality_data:
                csv = pd.DataFrame(quality_data).to_csv(index=False)
                st.download_button(
                    "Download CSV",
                    csv,
                    "quality_report.csv",
                    "text/csv",
                    use_container_width=True
                )
    
    with col3:
        if st.button("Configure Alerts", use_container_width=True):
            st.info("Navigate to Settings to configure alert thresholds")


if __name__ == "__main__":
    show()

"""
Gold Layer EDA - Comprehensive exploratory data analysis
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.data_loader import DataLoader, format_quality_score, format_number

PROJECT_ROOT = Path(__file__).parent.parent.parent / "data lakehouse" / "syniq_project"
loader = DataLoader(PROJECT_ROOT)


def show():
    """Display Gold Layer EDA page"""
    
    st.markdown("# Gold Layer EDA")
    st.markdown("### Comprehensive Exploratory Data Analysis")
    st.markdown("---")
    
    # Load available tables
    tables = loader.get_available_tables()
    
    if not tables:
        st.warning("WARNING: No data found. Please run Gold layer processing first.")
        st.code("python main.py gold process", language="bash")
        return
    
    # Table selector
    col1, col2 = st.columns([1, 3])
    
    with col1:
        sources = list(set([t["source"] for t in tables]))
        selected_source = st.selectbox("Source System", sources)
    
    with col2:
        source_tables = [t for t in tables if t["source"] == selected_source]
        table_options = {f"{t['table']}": t for t in source_tables}
        selected_table_name = st.selectbox("Table Name", list(table_options.keys()))
    
    # Load EDA report
    report = loader.load_eda_report(selected_source, selected_table_name)
    
    if not report:
        st.warning("WARNING: EDA report not found. Run Gold processing for this table.")
        st.code(f"python main.py gold process --source {selected_source} --entity {selected_table_name}", language="bash")
        return
    
    st.markdown("---")
    
    # Overview Metrics
    st.markdown("### Overview Metrics")
    
    basic_info = report.get("basic_info", {})
    quality_info = report.get("data_quality", {})
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.metric("Total Rows", format_number(basic_info.get("row_count", 0), 0))
    with col2:
        st.metric("Total Columns", basic_info.get("column_count", 0))
    with col3:
        st.metric("Memory", f"{basic_info.get('memory_usage_mb', 0):.2f} MB")
    with col4:
        st.metric("Duplicates", format_number(basic_info.get("duplicates", 0), 0))
    with col5:
        analyzed_at = report.get("analysis_timestamp", "").split("T")[0]
        st.metric("Analyzed", analyzed_at)
    
    st.markdown("---")
    
    # Data Quality Section
    st.markdown("### Data Quality Assessment")
    
    col1, col2 = st.columns([1, 2])
    
    with col1:
        # Quality score display
        score = quality_info.get("overall_quality_score", 0)
        label, css_class = format_quality_score(score)
        
        st.markdown(f"""
            <div style='text-align: center; padding: 2rem; background: white; border-radius: 0.5rem; box-shadow: 0 2px 4px rgba(0,0,0,0.1);'>
                <div class='{css_class}'>{label} {score:.1f}/100</div>
                <h3>Overall Quality Score</h3>
            </div>
        """, unsafe_allow_html=True)
        
        st. markdown("<br>", unsafe_allow_html=True)
        
        # Quality metrics
        st.metric("Completeness", f"{quality_info.get('completeness_score', 0):.1f}%")
        st.metric("Missing Values", format_number(quality_info.get("missing_value_count", 0), 0))
        st.metric("Duplicate %", f"{quality_info.get('duplicate_percentage', 0):.2f}%")
    
    with col2:
        # Quality breakdown chart
        quality_metrics = {
            "Completeness": quality_info.get("completeness_score", 0),
            "Data Integrity": 100 - quality_info.get("duplicate_percentage", 0),
            "Consistency": score
        }
        
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            x=list(quality_metrics.keys()),
            y=list(quality_metrics.values()),
            marker_color=['#3b82f6', '#10b981', '#6366f1'],
            text=[f"{v:.1f}%" for v in quality_metrics.values()],
            textposition='auto'
        ))
        
        fig.update_layout(
            title="Quality Metrics Breakdown",
            yaxis_title="Score (%)",
            yaxis_range=[0, 100],
            height=300,
            showlegend=False
        )
        
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    
    # Column Analysis
    st.markdown("### Column Analysis")
    
    column_analysis = report.get("column_analysis", {})
    
    if column_analysis:
        # Create DataFrame for column stats
        col_data = []
        for col_name, col_info in column_analysis.items():
            col_data.append({
                "Column": col_name,
                "Type": col_info.get("type", col_info.get("dtype", "unknown")),
                "Non-Null": format_number(col_info.get("non_null_count", 0), 0),
                "Null %": f"{col_info.get('null_percentage', 0):.1f}%",
                "Unique": format_number(col_info.get("unique_count", 0), 0),
                "Unique %": f"{col_info.get('unique_percentage', 0):.1f}%"
            })
        
        df_cols = pd.DataFrame(col_data)
        
        # Tabs for different views
        tab1, tab2, tab3 = st.tabs(["All Columns", "Numeric Columns", "Categorical Columns"])
        
        with tab1:
            st.dataframe(df_cols, use_container_width=True, hide_index=True, height=400)
        
        with tab2:
            numeric_cols = [col for col, info in column_analysis.items() 
                          if info.get("type") == "numeric"]
            
            if numeric_cols:
                selected_numeric = st.selectbox("Select numeric column", numeric_cols)
                
                if selected_numeric:
                    col_info = column_analysis[selected_numeric]
                    
                    col1, col2, col3, col4 = st.columns(4)
                    with col1:
                        st.metric("Min", f"{col_info.get('min', 0):.2f}")
                    with col2:
                        st.metric("Median", f"{col_info.get('median', 0):.2f}")
                    with col3:
                        st.metric("Mean", f"{col_info.get('mean', 0):.2f}")
                    with col4:
                        st.metric("Max", f"{col_info.get('max', 0):.2f}")
                    
                    col1, col2, col3 = st.columns(3)
                    with col1:
                        st.metric("Std Dev", f"{col_info.get('std', 0):.2f}")
                    with col2:
                        st.metric("Skewness", f"{col_info.get('skewness', 0):.3f}")
                    with col3:
                        st.metric("Kurtosis", f"{col_info.get('kurtosis', 0):.3f}")
            else:
                st.info("No numeric columns found")
        
        with tab3:
            categorical_cols = [col for col, info in column_analysis.items() 
                              if info.get("type") == "categorical"]
            
            if categorical_cols:
                selected_cat = st.selectbox("Select categorical column", categorical_cols)
                
                if selected_cat:
                    col_info = column_analysis[selected_cat]
                    
                    col1, col2 = st.columns(2)
                    
                    with col1:
                        st.markdown("**Most Frequent:**")
                        st.write(f"Value: {col_info.get('most_frequent', 'N/A')}")
                        st.write(f"Count: {format_number(col_info.get('most_frequent_count', 0), 0)}")
                    
                    with col2:
                        st.markdown("**Least Frequent:**")
                        st.write(f"Value: {col_info.get('least_frequent', 'N/A')}")
                        st.write(f"Count: {format_number(col_info.get('least_frequent_count', 0), 0)}")
                    
                    # Top values
                    top_values = col_info.get("top_10_values", {})
                    if top_values:
                        st.markdown("**Top 10 Values:**")
                        top_df = pd.DataFrame({
                            "Value": list(top_values.keys()),
                            "Count": list(top_values.values())
                        })
                        
                        fig = px.bar(top_df, x="Count", y="Value", orientation='h')
                        fig.update_layout(height=350)
                        st.plotly_chart(fig, use_container_width=True)
            else:
                st.info("No categorical columns found")
    
    st.markdown("---")
    
    # Correlations
    st.markdown("### 🔗 Correlations")
    
    correlations = report.get("correlations", {})
    strong_corrs = correlations.get("strong_correlations", [])
    
    if strong_corrs:
        st.info(f"Found {len(strong_corrs)} strong correlations (|r| > 0.7)")
        
        for corr in strong_corrs:
            col1, col2, col3 = st.columns([2, 2, 1])
            with col1:
                st.write(f"**{corr['column1']}**")
            with col2:
                st.write(f"**{corr['column2']}**")
            with col3:
                corr_value = corr['correlation']
                color = "[+]" if corr_value > 0 else "[-]"
                st.write(f"{color} {corr_value:.3f}")
    else:
        st.info("No strong correlations detected")
    
    st.markdown("---")
    
    # Key Insights
    st.markdown("### Key Insights")
    
    insights = report.get("insights", [])
    
    if insights:
        for i, insight in enumerate(insights, 1):
            st.info(f"**{i}.** {insight}")
    else:
        st.success("[SUCCESS] No issues detected - data quality looks good!")
    
    st.markdown("---")
    
    # Business Metrics
    business_metrics = report.get("business_metrics", {})
    
    if business_metrics:
        st.markdown("### Business Metrics")
        
        cols = st.columns(min(len(business_metrics), 4))
        for i, (metric, value) in enumerate(business_metrics.items()):
            if isinstance(value, (int, float)):
                with cols[i % 4]:
                    st.metric(
                        metric.replace('_', ' ').title(),
                        format_number(value, 2)
                    )
    
    st.markdown("---")
    
    # View Full Report
    st.markdown("### Full Report")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        if st.button("View HTML Report", use_container_width=True):
            html_file = loader.eda_reports_path / f"{selected_source}_{selected_table_name}_eda_*.html"
            html_files = list(loader.eda_reports_path.glob(f"{selected_source}_{selected_table_name}_eda_*.html"))
            if html_files:
                st.success(f"Report: {html_files[0].name}")
                st.code(f"start {html_files[0]}", language="bash")
            else:
                st.error("HTML report not found")
    
    with col2:
        if st.button("View Markdown", use_container_width=True):
            md_files = list(loader.eda_reports_path.glob(f"{selected_source}_{selected_table_name}_eda_*.md"))
            if md_files:
                st.success(f"Report: {md_files[0].name}")
            else:
                st.error("Markdown report not found")
    
    with col3:
        if st.button("Download JSON", use_container_width=True):
            import json
            json_str = json.dumps(report, indent=2, default=str)
            st.download_button(
                label="Download JSON Report",
                data=json_str,
                file_name=f"{selected_source}_{selected_table_name}_eda.json",
                mime="application/json"
            )


if __name__ == "__main__":
    show()

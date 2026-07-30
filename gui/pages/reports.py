"""
Reports - Generate and download reports
"""

import streamlit as st
import pandas as pd
from pathlib import Path
import sys
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))
from utils.data_loader import DataLoader

PROJECT_ROOT = Path(__file__).parent.parent.parent / "data lakehouse" / "syniq_project"
loader = DataLoader(PROJECT_ROOT)


def show():
    """Display reports page"""
    
    st.markdown("# Reports")
    st.markdown("### Generate and download comprehensive data reports")
    st.markdown("---")
    
    # Report types
    st.markdown("### Available Report Types")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.markdown("""
            <div style='padding: 1.5rem; background: white; border-radius: 0.5rem; border-left: 4px solid #3b82f6;'>
                <h3>Executive Summary</h3>
                <p>High-level overview of data quality and system health</p>
                <ul>
                    <li>Quality scores</li>
                    <li>Table summaries</li>
                    <li>Key insights</li>
                </ul>
            </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown("""
            <div style='padding: 1.5rem; background: white; border-radius: 0.5rem; border-left: 4px solid #10b981;'>
                <h3>EDA Report</h3>
                <p>Detailed exploratory data analysis for specific tables</p>
                <ul>
                    <li>Statistical analysis</li>
                    <li>Distributions</li>
                    <li>Correlations</li>
                </ul>
            </div>
        """, unsafe_allow_html=True)
    
    with col3:
        st.markdown("""
            <div style='padding: 1.5rem; background: white; border-radius: 0.5rem; border-left: 4px solid #f59e0b;'>
                <h3>Quality Report</h3>
                <p>Data quality assessment across all tables</p>
                <ul>
                    <li>Quality metrics</li>
                    <li>Issues & alerts</li>
                    <li>Recommendations</li>
                </ul>
            </div>
        """, unsafe_allow_html=True)
    
    st.markdown("---")
    
    # Report generator
    st.markdown("### Generate Report")
    
    report_type = st.selectbox(
        "Select Report Type",
        ["Executive Summary", "EDA Report", "Quality Report", "Custom Report"]
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        format_type = st.selectbox(
            "Format",
            ["JSON", "CSV", "HTML", "Markdown"]
        )
    
    with col2:
        include_visualizations = st.checkbox("Include Visualizations", value=True)
    
    if report_type == "EDA Report":
        # Table selection for EDA report
        tables = loader.get_available_tables()
        if tables:
            sources = list(set([t["source"] for t in tables]))
            selected_source = st.selectbox("Source System", sources)
            
            source_tables = [t for t in tables if t["source"] == selected_source]
            table_options = {f"{t['table']}": t for t in source_tables}
            selected_table = st.selectbox("Table Name", list(table_options.keys()))
    
    st.markdown("---")
    
    # Generate button
    if st.button("Generate Report", type="primary", use_container_width=True):
        with st.spinner("Generating report..."):
            if report_type == "Executive Summary":
                report = generate_executive_summary(loader)
                st.success("Executive summary generated!")
                
                # Download button
                if format_type == "JSON":
                    import json
                    report_str = json.dumps(report, indent=2, default=str)
                    st.download_button(
                        "Download Report",
                        report_str,
                        f"executive_summary_{datetime.now().strftime('%Y%m%d')}.json",
                        "application/json",
                        use_container_width=True
                    )
                elif format_type == "CSV":
                    # Convert to flat structure for CSV
                    df = pd.DataFrame([report])
                    csv = df.to_csv(index=False)
                    st.download_button(
                        "Download Report",
                        csv,
                        f"executive_summary_{datetime.now().strftime('%Y%m%d')}.csv",
                        "text/csv",
                        use_container_width=True
                    )
                
                # Preview
                st.markdown("### Report Preview")
                st.json(report)
            
            elif report_type == "Quality Report":
                quality_summary = loader.get_quality_summary()
                st.success("Quality report generated!")
                
                st.markdown("### Report Preview")
                
                col1, col2, col3 = st.columns(3)
                with col1:
                    st.metric("Total Tables", quality_summary["total_tables"])
                with col2:
                    st.metric("Avg Quality", f"{quality_summary['avg_quality']:.1f}")
                with col3:
                    st.metric("Avg Completeness", f"{quality_summary['avg_completeness']:.1f}%")
                
                # Download
                if format_type == "CSV":
                    df = pd.DataFrame([quality_summary])
                    csv = df.to_csv(index=False)
                    st.download_button(
                        "Download Report",
                        csv,
                        f"quality_report_{datetime.now().strftime('%Y%m%d')}.csv",
                        "text/csv",
                        use_container_width=True
                    )
            
            elif report_type == "EDA Report":
                if selected_table:
                    report = loader.load_eda_report(selected_source, selected_table)
                    if report:
                        st.success(f"[SUCCESS] EDA report loaded for {selected_source}.{selected_table}")
                        
                        # Download
                        if format_type == "JSON":
                            import json
                            report_str = json.dumps(report, indent=2, default=str)
                            st.download_button(
                                "Download Report",
                                report_str,
                                f"eda_{selected_source}_{selected_table}_{datetime.now().strftime('%Y%m%d')}.json",
                                "application/json",
                                use_container_width=True
                            )
                    else:
                        st.error("Report not found")
    
    st.markdown("---")
    
    # Available reports
    st.markdown("### Available Reports")
    
    eda_reports_path = loader.eda_reports_path
    if eda_reports_path.exists():
        html_reports = list(eda_reports_path.glob("*.html"))
        json_reports = list(eda_reports_path.glob("*.json"))
        md_reports = list(eda_reports_path.glob("*.md"))
        
        st.info(f"Found {len(html_reports)} HTML, {len(json_reports)} JSON, and {len(md_reports)} Markdown reports")
        
        # Show recent reports
        all_reports = html_reports + json_reports + md_reports
        all_reports.sort(key=lambda x: x.stat().st_mtime, reverse=True)
        
        if all_reports[:10]:
            report_data = []
            for report_file in all_reports[:10]:
                report_data.append({
                    "Name": report_file.name,
                    "Type": report_file.suffix[1:].upper(),
                    "Size": f"{report_file.stat().st_size / 1024:.1f} KB",
                    "Modified": datetime.fromtimestamp(
                        report_file.stat().st_mtime
                    ).strftime("%Y-%m-%d %H:%M:%S")
                })
            
            df_reports = pd.DataFrame(report_data)
            st.dataframe(df_reports, use_container_width=True, hide_index=True)


def generate_executive_summary(loader):
    """Generate executive summary report"""
    tables = loader.get_available_tables()
    quality_summary = loader.get_quality_summary()
    
    summary = {
        "report_type": "Executive Summary",
        "generated_at": datetime.now().isoformat(),
        "total_tables": len(tables),
        "quality_summary": quality_summary,
        "tables": []
    }
    
    for table_info in tables:
        report = loader.load_eda_report(table_info["source"], table_info["table"])
        if report:
            summary["tables"].append({
                "source": table_info["source"],
                "table": table_info["table"],
                "quality_score": report.get("data_quality", {}).get("overall_quality_score", 0),
                "completeness": report.get("data_quality", {}).get("completeness_score", 0),
                "row_count": report.get("basic_info", {}).get("row_count", 0)
            })
    
    return summary


if __name__ == "__main__":
    show()

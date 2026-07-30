"""
Gold Layer Report Generator

Generates comprehensive EDA reports in multiple formats:
- JSON (machine-readable)
- HTML (human-readable with visualizations)
- Markdown (documentation)
"""

import json
from pathlib import Path
from typing import Dict, Any, List
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class GoldReportGenerator:
    """
    Generates comprehensive EDA reports from analysis results.
    """
    
    def __init__(self):
        """Initialize the report generator."""
        self.logger = logger
    
    def generate_report(self, eda_results: Dict[str, Any],
                       output_dir: str = "gold/eda_reports",
                       formats: List[str] = None) -> str:
        """
        Generate EDA report in multiple formats.
        
        Args:
            eda_results: EDA analysis results
            output_dir: Directory to save reports
            formats: List of formats ['json', 'html', 'md']
            
        Returns:
            Path to main report file
        """
        if formats is None:
            formats = ['json', 'html', 'md']
        
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        
        table_name = eda_results['table_name']
        source_system = eda_results['source_system']
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        base_filename = f"{source_system}_{table_name}_eda_{timestamp}"
        
        reports = {}
        
        # Generate JSON report
        if 'json' in formats:
            json_path = self._generate_json_report(
                eda_results, output_path, base_filename
            )
            reports['json'] = json_path
        
        # Generate HTML report
        if 'html' in formats:
            html_path = self._generate_html_report(
                eda_results, output_path, base_filename
            )
            reports['html'] = html_path
        
        # Generate Markdown report
        if 'md' in formats:
            md_path = self._generate_markdown_report(
                eda_results, output_path, base_filename
            )
            reports['md'] = md_path
        
        return reports.get('json', reports.get('html', reports.get('md', '')))
    
    def _generate_json_report(self, eda_results: Dict[str, Any],
                             output_dir: Path, base_filename: str) -> str:
        """Generate JSON report."""
        json_file = output_dir / f"{base_filename}.json"
        
        with open(json_file, 'w') as f:
            json.dump(eda_results, f, indent=2, default=str)
        
        self.logger.info(f"JSON report generated: {json_file}")
        return str(json_file)
    
    def _generate_html_report(self, eda_results: Dict[str, Any],
                             output_dir: Path, base_filename: str) -> str:
        """Generate HTML report with styled tables and charts."""
        html_file = output_dir / f"{base_filename}.html"
        
        html_content = self._build_html_content(eda_results)
        
        with open(html_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        
        self.logger.info(f"HTML report generated: {html_file}")
        return str(html_file)
    
    def _generate_markdown_report(self, eda_results: Dict[str, Any],
                                  output_dir: Path, base_filename: str) -> str:
        """Generate Markdown report."""
        md_file = output_dir / f"{base_filename}.md"
        
        md_content = self._build_markdown_content(eda_results)
        
        with open(md_file, 'w', encoding='utf-8') as f:
            f.write(md_content)
        
        self.logger.info(f"Markdown report generated: {md_file}")
        return str(md_file)
    
    def _build_html_content(self, eda_results: Dict[str, Any]) -> str:
        """Build HTML content for the report."""
        table_name = eda_results['table_name']
        source_system = eda_results['source_system']
        timestamp = eda_results['analysis_timestamp']
        
        html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>EDA Report - {source_system}.{table_name}</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            line-height: 1.6;
            color: #333;
            max-width: 1200px;
            margin: 0 auto;
            padding: 20px;
            background-color: #f5f5f5;
        }}
        .header {{
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            padding: 30px;
            border-radius: 10px;
            margin-bottom: 30px;
            box-shadow: 0 4px 6px rgba(0,0,0,0.1);
        }}
        .header h1 {{
            margin: 0;
            font-size: 2.5em;
        }}
        .header p {{
            margin: 10px 0 0 0;
            opacity: 0.9;
        }}
        .section {{
            background: white;
            padding: 25px;
            margin-bottom: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .section h2 {{
            color: #667eea;
            border-bottom: 3px solid #667eea;
            padding-bottom: 10px;
            margin-top: 0;
        }}
        .metric-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
            gap: 15px;
            margin: 20px 0;
        }}
        .metric-card {{
            background: #f8f9fa;
            padding: 20px;
            border-radius: 8px;
            border-left: 4px solid #667eea;
        }}
        .metric-card .label {{
            font-size: 0.9em;
            color: #666;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}
        .metric-card .value {{
            font-size: 2em;
            font-weight: bold;
            color: #333;
            margin-top: 5px;
        }}
        .quality-score {{
            font-size: 3em;
            font-weight: bold;
            text-align: center;
            margin: 20px 0;
        }}
        .quality-excellent {{ color: #28a745; }}
        .quality-good {{ color: #17a2b8; }}
        .quality-fair {{ color: #ffc107; }}
        .quality-poor {{ color: #dc3545; }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin: 15px 0;
        }}
        th, td {{
            padding: 12px;
            text-align: left;
            border-bottom: 1px solid #ddd;
        }}
        th {{
            background-color: #667eea;
            color: white;
            font-weight: 600;
        }}
        tr:hover {{
            background-color: #f5f5f5;
        }}
        .insight {{
            background: #e7f3ff;
            border-left: 4px solid #2196F3;
            padding: 15px;
            margin: 10px 0;
            border-radius: 4px;
        }}
        .correlation-item {{
            padding: 10px;
            margin: 5px 0;
            background: #f8f9fa;
            border-radius: 4px;
        }}
        .strong-positive {{ border-left: 4px solid #28a745; }}
        .strong-negative {{ border-left: 4px solid #dc3545; }}
        .footer {{
            text-align: center;
            color: #666;
            margin-top: 30px;
            padding-top: 20px;
            border-top: 1px solid #ddd;
        }}
    </style>
</head>
<body>
    <div class="header">
        <h1>📊 Exploratory Data Analysis Report</h1>
        <p><strong>Table:</strong> {source_system}.{table_name}</p>
        <p><strong>Analysis Date:</strong> {timestamp}</p>
    </div>
"""
        
        # Basic Information Section
        basic_info = eda_results['basic_info']
        html += f"""
    <div class="section">
        <h2>📋 Basic Information</h2>
        <div class="metric-grid">
            <div class="metric-card">
                <div class="label">Total Rows</div>
                <div class="value">{basic_info['row_count']:,}</div>
            </div>
            <div class="metric-card">
                <div class="label">Total Columns</div>
                <div class="value">{basic_info['column_count']}</div>
            </div>
            <div class="metric-card">
                <div class="label">Memory Usage</div>
                <div class="value">{basic_info['memory_usage_mb']:.2f} MB</div>
            </div>
            <div class="metric-card">
                <div class="label">Duplicate Rows</div>
                <div class="value">{basic_info['duplicates']:,}</div>
            </div>
        </div>
    </div>
"""
        
        # Data Quality Section
        quality = eda_results['data_quality']
        quality_score = quality['overall_quality_score']
        quality_class = (
            'quality-excellent' if quality_score >= 90 else
            'quality-good' if quality_score >= 75 else
            'quality-fair' if quality_score >= 60 else
            'quality-poor'
        )
        
        html += f"""
    <div class="section">
        <h2>✅ Data Quality Assessment</h2>
        <div class="quality-score {quality_class}">
            {quality_score:.1f}/100
        </div>
        <div class="metric-grid">
            <div class="metric-card">
                <div class="label">Completeness</div>
                <div class="value">{quality['completeness_score']:.1f}%</div>
            </div>
            <div class="metric-card">
                <div class="label">Missing Values</div>
                <div class="value">{quality['missing_value_count']:,}</div>
            </div>
            <div class="metric-card">
                <div class="label">Missing %</div>
                <div class="value">{quality['missing_percentage']:.2f}%</div>
            </div>
            <div class="metric-card">
                <div class="label">Duplicates %</div>
                <div class="value">{quality['duplicate_percentage']:.2f}%</div>
            </div>
        </div>
    </div>
"""
        
        # Column Analysis Section
        html += """
    <div class="section">
        <h2>📊 Column Analysis</h2>
        <table>
            <thead>
                <tr>
                    <th>Column Name</th>
                    <th>Type</th>
                    <th>Non-Null</th>
                    <th>Null %</th>
                    <th>Unique</th>
                    <th>Key Statistics</th>
                </tr>
            </thead>
            <tbody>
"""
        
        for col_name, col_info in eda_results['column_analysis'].items():
            stats = ""
            if col_info.get('type') == 'numeric':
                stats = f"Mean: {col_info.get('mean', 0):.2f}, Std: {col_info.get('std', 0):.2f}"
            elif col_info.get('type') == 'categorical':
                stats = f"Top: {col_info.get('most_frequent', 'N/A')}"
            
            html += f"""
                <tr>
                    <td><strong>{col_name}</strong></td>
                    <td>{col_info['dtype']}</td>
                    <td>{col_info['non_null_count']:,}</td>
                    <td>{col_info['null_percentage']:.1f}%</td>
                    <td>{col_info['unique_count']}</td>
                    <td>{stats}</td>
                </tr>
"""
        
        html += """
            </tbody>
        </table>
    </div>
"""
        
        # Correlations Section
        if 'strong_correlations' in eda_results['correlations']:
            strong_corrs = eda_results['correlations']['strong_correlations']
            if strong_corrs:
                html += """
    <div class="section">
        <h2>🔗 Strong Correlations</h2>
"""
                for corr in strong_corrs:
                    corr_class = 'strong-positive' if corr['correlation'] > 0 else 'strong-negative'
                    html += f"""
        <div class="correlation-item {corr_class}">
            <strong>{corr['column1']}</strong> ↔️ <strong>{corr['column2']}</strong>: 
            {corr['correlation']:.3f}
        </div>
"""
                html += """
    </div>
"""
        
        # Outliers Section
        outliers = eda_results.get('outliers', {})
        if outliers and not outliers.get('message'):
            html += """
    <div class="section">
        <h2>⚠️ Outliers Detected</h2>
        <table>
            <thead>
                <tr>
                    <th>Column</th>
                    <th>Outlier Count</th>
                    <th>Percentage</th>
                    <th>Bounds</th>
                </tr>
            </thead>
            <tbody>
"""
            for col, outlier_info in outliers.items():
                html += f"""
                <tr>
                    <td><strong>{col}</strong></td>
                    <td>{outlier_info['count']:,}</td>
                    <td>{outlier_info['percentage']:.2f}%</td>
                    <td>[{outlier_info['lower_bound']:.2f}, {outlier_info['upper_bound']:.2f}]</td>
                </tr>
"""
            html += """
            </tbody>
        </table>
    </div>
"""
        
        # Insights Section
        insights = eda_results.get('insights', [])
        if insights:
            html += """
    <div class="section">
        <h2>💡 Key Insights</h2>
"""
            for insight in insights:
                html += f"""
        <div class="insight">
            {insight}
        </div>
"""
            html += """
    </div>
"""
        
        # Business Metrics Section
        business_metrics = eda_results.get('business_metrics', {})
        if business_metrics:
            html += """
    <div class="section">
        <h2>💼 Business Metrics</h2>
        <div class="metric-grid">
"""
            for metric_name, metric_value in business_metrics.items():
                if isinstance(metric_value, (int, float)):
                    html += f"""
            <div class="metric-card">
                <div class="label">{metric_name.replace('_', ' ').title()}</div>
                <div class="value">{metric_value:,.2f}</div>
            </div>
"""
            html += """
        </div>
    </div>
"""
        
        html += """
    <div class="footer">
        <p>Generated by Syniq Data Lakehouse - Gold Layer EDA Engine</p>
        <p>© 2026 All rights reserved</p>
    </div>
</body>
</html>
"""
        
        return html
    
    def _build_markdown_content(self, eda_results: Dict[str, Any]) -> str:
        """Build Markdown content for the report."""
        table_name = eda_results['table_name']
        source_system = eda_results['source_system']
        timestamp = eda_results['analysis_timestamp']
        
        md = f"""# Exploratory Data Analysis Report

**Table:** `{source_system}.{table_name}`  
**Analysis Date:** {timestamp}

---

## 📋 Basic Information

| Metric | Value |
|--------|-------|
| Total Rows | {eda_results['basic_info']['row_count']:,} |
| Total Columns | {eda_results['basic_info']['column_count']} |
| Memory Usage | {eda_results['basic_info']['memory_usage_mb']:.2f} MB |
| Duplicate Rows | {eda_results['basic_info']['duplicates']:,} |

---

## ✅ Data Quality Assessment

**Overall Quality Score:** {eda_results['data_quality']['overall_quality_score']:.1f}/100

| Metric | Value |
|--------|-------|
| Completeness | {eda_results['data_quality']['completeness_score']:.1f}% |
| Missing Values | {eda_results['data_quality']['missing_value_count']:,} |
| Missing Percentage | {eda_results['data_quality']['missing_percentage']:.2f}% |
| Duplicate Percentage | {eda_results['data_quality']['duplicate_percentage']:.2f}% |

---

## 📊 Column Analysis

| Column | Type | Non-Null | Null % | Unique |
|--------|------|----------|--------|--------|
"""
        
        for col_name, col_info in eda_results['column_analysis'].items():
            md += f"| {col_name} | {col_info['dtype']} | {col_info['non_null_count']:,} | "
            md += f"{col_info['null_percentage']:.1f}% | {col_info['unique_count']} |\n"
        
        md += "\n---\n\n"
        
        # Strong Correlations
        if 'strong_correlations' in eda_results['correlations']:
            strong_corrs = eda_results['correlations']['strong_correlations']
            if strong_corrs:
                md += "## 🔗 Strong Correlations\n\n"
                for corr in strong_corrs:
                    md += f"- **{corr['column1']}** ↔️ **{corr['column2']}**: {corr['correlation']:.3f}\n"
                md += "\n---\n\n"
        
        # Insights
        insights = eda_results.get('insights', [])
        if insights:
            md += "## 💡 Key Insights\n\n"
            for insight in insights:
                md += f"- {insight}\n"
            md += "\n---\n\n"
        
        # Business Metrics
        business_metrics = eda_results.get('business_metrics', {})
        if business_metrics:
            md += "## 💼 Business Metrics\n\n"
            md += "| Metric | Value |\n"
            md += "|--------|-------|\n"
            for metric_name, metric_value in business_metrics.items():
                if isinstance(metric_value, (int, float)):
                    md += f"| {metric_name.replace('_', ' ').title()} | {metric_value:,.2f} |\n"
            md += "\n---\n\n"
        
        md += "\n---\n\n*Generated by Syniq Data Lakehouse - Gold Layer EDA Engine*\n"
        
        return md


if __name__ == "__main__":
    # Example usage
    print("Gold Report Generator - Ready to generate EDA reports")

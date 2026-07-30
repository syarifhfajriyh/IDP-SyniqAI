"""
Data loading utilities for the SyniqAI dashboard
"""

import pandas as pd
import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime
import streamlit as st


class DataLoader:
    """Load data from the lakehouse layers"""
    
    def __init__(self, project_root: Path):
        self.project_root = project_root
        self.bronze_path = project_root / "bronze"
        self.silver_path = project_root / "silver"
        self.gold_path = project_root / "gold"
        self.eda_reports_path = self.gold_path / "eda_reports"
    
    @st.cache_data(ttl=300)
    def get_available_tables(_self) -> List[Dict[str, str]]:
        """Get list of all tables in Gold layer"""
        tables = []
        
        if not _self.gold_path.exists():
            return tables
        
        for source_dir in _self.gold_path.iterdir():
            if source_dir.is_dir() and source_dir.name != "eda_reports":
                for table_dir in source_dir.iterdir():
                    if table_dir.is_dir():
                        parquet_files = list(table_dir.glob("*.parquet"))
                        if parquet_files:
                            latest_file = max(parquet_files, key=lambda x: x.stat().st_mtime)
                            tables.append({
                                "source": source_dir.name,
                                "table": table_dir.name,
                                "path": str(latest_file),
                                "last_modified": datetime.fromtimestamp(
                                    latest_file.stat().st_mtime
                                ).strftime("%Y-%m-%d %H:%M:%S")
                            })
        
        return tables
    
    @st.cache_data(ttl=300)
    def load_table_data(_self, source: str, table: str) -> Optional[pd.DataFrame]:
        """Load data from Gold layer"""
        table_path = _self.gold_path / source / table
        
        if not table_path.exists():
            return None
        
        parquet_files = list(table_path.glob("*.parquet"))
        if not parquet_files:
            return None
        
        # Load most recent file
        latest_file = max(parquet_files, key=lambda x: x.stat().st_mtime)
        
        try:
            df = pd.read_parquet(latest_file)
            return df
        except Exception as e:
            st.error(f"Error loading data: {e}")
            return None
    
    @st.cache_data(ttl=300)
    def load_eda_report(_self, source: str, table: str) -> Optional[Dict]:
        """Load EDA report for a table"""
        if not _self.eda_reports_path.exists():
            return None
        
        # Find most recent report
        pattern = f"{source}_{table}_eda_*.json"
        report_files = list(_self.eda_reports_path.glob(pattern))
        
        if not report_files:
            return None
        
        latest_report = max(report_files, key=lambda x: x.stat().st_mtime)
        
        try:
            with open(latest_report, 'r') as f:
                report = json.load(f)
            return report
        except Exception as e:
            st.error(f"Error loading report: {e}")
            return None
    
    @st.cache_data(ttl=300)
    def get_master_report(_self) -> Optional[Dict]:
        """Load master report"""
        master_report_path = _self.eda_reports_path / "MASTER_REPORT.json"
        
        if not master_report_path.exists():
            return None
        
        try:
            with open(master_report_path, 'r') as f:
                report = json.load(f)
            return report
        except Exception as e:
            st.error(f"Error loading master report: {e}")
            return None
    
    def get_quality_summary(_self) -> Dict:
        """Get summary of data quality across all tables"""
        tables = _self.get_available_tables()
        summary = {
            "total_tables": len(tables),
            "excellent": 0,
            "good": 0,
            "fair": 0,
            "poor": 0,
            "avg_quality": 0,
            "avg_completeness": 0
        }
        
        total_quality = 0
        total_completeness = 0
        
        for table_info in tables:
            report = _self.load_eda_report(table_info["source"], table_info["table"])
            if report:
                quality_score = report.get("data_quality", {}).get("overall_quality_score", 0)
                completeness = report.get("data_quality", {}).get("completeness_score", 0)
                
                total_quality += quality_score
                total_completeness += completeness
                
                if quality_score >= 90:
                    summary["excellent"] += 1
                elif quality_score >= 75:
                    summary["good"] += 1
                elif quality_score >= 60:
                    summary["fair"] += 1
                else:
                    summary["poor"] += 1
        
        if len(tables) > 0:
            summary["avg_quality"] = total_quality / len(tables)
            summary["avg_completeness"] = total_completeness / len(tables)
        
        return summary


def format_quality_score(score: float) -> Tuple[str, str]:
    """Format quality score with text label and color class"""
    if score >= 90:
        return "[EXCELLENT]", "quality-excellent"
    elif score >= 75:
        return "[GOOD]", "quality-good"
    elif score >= 60:
        return "[FAIR]", "quality-fair"
    else:
        return "[POOR]", "quality-poor"


def format_number(num: float, decimals: int = 2) -> str:
    """Format number with thousands separator"""
    if isinstance(num, (int, float)):
        if decimals == 0:
            return f"{int(num):,}"
        else:
            return f"{num:,.{decimals}f}"
    return str(num)

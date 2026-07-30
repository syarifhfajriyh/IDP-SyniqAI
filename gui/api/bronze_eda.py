"""
Bronze Layer EDA - Pre-Flight Data Quality Analysis
Answers the question: "Is this data safe and sane enough to pass to Silver?"
"""

import pandas as pd
import numpy as np
from typing import Dict, List, Any, Optional
import re
from datetime import datetime
import logging

logger = logging.getLogger(__name__)


class BronzeEDAAnalyzer:
    """Lightweight EDA analyzer for Bronze layer data quality checks"""
    
    def __init__(self, df: pd.DataFrame):
        self.df = df
        self.row_count = len(df)
        self.column_count = len(df.columns)
    
    def analyze(self) -> Dict[str, Any]:
        """Run comprehensive light EDA analysis"""
        return {
            "summary": self._get_summary_stats(),
            "numeric_distributions": self._analyze_numeric_distributions(),
            "categorical_frequencies": self._analyze_categorical_frequencies(),
            "temporal_patterns": self._analyze_temporal_patterns(),
            "semantic_warnings": self._detect_semantic_issues(),
            "correlations": self._analyze_correlations(),
            "uniqueness_signals": self._analyze_uniqueness(),
            "outlier_summary": self._detect_outliers()
        }
    
    def _get_summary_stats(self) -> Dict[str, Any]:
        """Basic summary statistics"""
        return {
            "row_count": self.row_count,
            "column_count": self.column_count,
            "memory_usage_mb": round(self.df.memory_usage(deep=True).sum() / 1024 / 1024, 2),
            "duplicate_rows": int(self.df.duplicated().sum()),
            "duplicate_percentage": round(self.df.duplicated().sum() / self.row_count * 100, 2) if self.row_count > 0 else 0,
            "total_null_cells": int(self.df.isnull().sum().sum()),
            "completeness_score": round((1 - self.df.isnull().sum().sum() / (self.row_count * self.column_count)) * 100, 2) if self.row_count > 0 else 100
        }
    
    def _analyze_numeric_distributions(self) -> Dict[str, Any]:
        """Analyze distributions for numeric columns with histograms"""
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        distributions = {}
        
        for col in numeric_cols:
            series = self.df[col].dropna()
            if len(series) == 0:
                continue
            
            # Five-number summary + mean
            stats = {
                "min": float(series.min()),
                "q25": float(series.quantile(0.25)),
                "median": float(series.quantile(0.50)),
                "q75": float(series.quantile(0.75)),
                "max": float(series.max()),
                "mean": float(series.mean()),
                "std": float(series.std()) if len(series) > 1 else 0,
                "null_count": int(self.df[col].isnull().sum()),
                "null_percentage": round(self.df[col].isnull().sum() / self.row_count * 100, 2)
            }
            
            # Histogram (10 bins)
            try:
                hist, bin_edges = np.histogram(series, bins=10)
                stats["histogram"] = {
                    "counts": hist.tolist(),
                    "bin_edges": [round(float(x), 2) for x in bin_edges]
                }
            except:
                stats["histogram"] = None
            
            # Detect skewness
            if stats["std"] > 0:
                range_val = stats["max"] - stats["min"]
                if range_val > 0:
                    stats["skew"] = "right" if stats["mean"] > stats["median"] else "left" if stats["mean"] < stats["median"] else "symmetric"
                else:
                    stats["skew"] = "symmetric"
            else:
                stats["skew"] = "constant"
            
            distributions[col] = stats
        
        return distributions
    
    def _analyze_categorical_frequencies(self) -> Dict[str, Any]:
        """Analyze categorical/string columns with frequency breakdown"""
        categorical_cols = self.df.select_dtypes(include=['object', 'category', 'string']).columns
        frequencies = {}
        
        for col in categorical_cols:
            series = self.df[col].dropna()
            if len(series) == 0:
                continue
            
            unique_count = series.nunique()
            value_counts = series.value_counts()
            
            # Only show distributions for columns with reasonable cardinality (2-50 unique values)
            if 2 <= unique_count <= 50:
                top_10 = value_counts.head(10)
                frequencies[col] = {
                    "unique_count": int(unique_count),
                    "null_count": int(self.df[col].isnull().sum()),
                    "null_percentage": round(self.df[col].isnull().sum() / self.row_count * 100, 2),
                    "distribution": [
                        {"value": str(k), "count": int(v), "percentage": round(v / len(series) * 100, 1)}
                        for k, v in top_10.items()
                    ],
                    "is_complete": unique_count <= 10  # Show all if small enough
                }
            else:
                # High cardinality - just show summary
                frequencies[col] = {
                    "unique_count": int(unique_count),
                    "null_count": int(self.df[col].isnull().sum()),
                    "null_percentage": round(self.df[col].isnull().sum() / self.row_count * 100, 2),
                    "cardinality": "high" if unique_count > 50 else "low",
                    "sample_values": series.head(5).tolist() if unique_count > 50 else None
                }
        
        return frequencies
    
    def _analyze_temporal_patterns(self) -> Dict[str, Any]:
        """Analyze datetime columns for temporal patterns"""
        datetime_cols = self.df.select_dtypes(include=['datetime64', 'datetime']).columns
        
        # Also try to detect date/timestamp columns that are stored as strings
        string_cols = self.df.select_dtypes(include=['object']).columns
        potential_date_cols = [col for col in string_cols if any(kw in col.lower() for kw in ['date', 'time', 'timestamp', 'created', 'updated'])]
        
        patterns = {}
        
        # Analyze explicit datetime columns
        for col in datetime_cols:
            series = pd.to_datetime(self.df[col], errors='coerce').dropna()
            if len(series) == 0:
                continue
            
            patterns[col] = self._get_temporal_pattern(series, col)
        
        # Try to parse potential date columns
        for col in potential_date_cols[:3]:  # Limit to first 3 to avoid slowdown
            try:
                series = pd.to_datetime(self.df[col], errors='coerce').dropna()
                if len(series) > self.row_count * 0.5:  # If >50% parsed successfully
                    patterns[col] = self._get_temporal_pattern(series, col)
            except:
                continue
        
        return patterns
    
    def _get_temporal_pattern(self, series: pd.Series, col_name: str) -> Dict[str, Any]:
        """Extract temporal pattern from datetime series"""
        pattern = {
            "min_date": series.min().isoformat() if len(series) > 0 else None,
            "max_date": series.max().isoformat() if len(series) > 0 else None,
            "range_days": (series.max() - series.min()).days if len(series) > 0 else 0,
            "null_count": int(self.df[col_name].isnull().sum()),
            "unique_dates": int(series.nunique())
        }
        
        # Check if all values are identical (batch load indicator)
        if pattern["unique_dates"] == 1:
            pattern["warning"] = "All timestamps identical - batch load detected"
        
        # Monthly distribution (if range > 30 days)
        if pattern["range_days"] > 30:
            monthly = series.dt.to_period('M').value_counts().sort_index()
            pattern["monthly_distribution"] = [
                {"month": str(period), "count": int(count)}
                for period, count in monthly.head(12).items()
            ]
        
        return pattern
    
    def _detect_semantic_issues(self) -> List[Dict[str, Any]]:
        """Detect semantic data quality issues"""
        warnings = []
        
        for col in self.df.columns:
            # Check for leading zeros in numeric-looking strings
            if self.df[col].dtype == 'object':
                series = self.df[col].dropna().astype(str)
                if len(series) > 0:
                    sample = series.head(100)
                    leading_zeros = sample.str.match(r'^0\d+').sum()
                    if leading_zeros > len(sample) * 0.5:
                        warnings.append({
                            "column": col,
                            "type": "leading_zeros",
                            "severity": "high",
                            "message": f"Column '{col}' contains numeric strings with leading zeros - will be lost if cast to integer",
                            "affected_rows": int((series.str.match(r'^0\d+').sum()))
                        })
            
            # Check email validity
            if 'email' in col.lower():
                if self.df[col].dtype == 'object':
                    series = self.df[col].dropna().astype(str)
                    if len(series) > 0:
                        email_regex = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
                        valid_emails = series.str.match(email_regex).sum()
                        validity_rate = valid_emails / len(series) * 100
                        if validity_rate < 95:
                            warnings.append({
                                "column": col,
                                "type": "invalid_format",
                                "severity": "medium",
                                "message": f"Email validity rate: {validity_rate:.1f}% - some invalid formats detected",
                                "validity_rate": round(validity_rate, 1)
                            })
            
            # Check for suspiciously low cardinality in name fields
            if 'name' in col.lower() and 'file' not in col.lower():
                if self.df[col].dtype == 'object':
                    unique_ratio = self.df[col].nunique() / self.row_count
                    if unique_ratio < 0.1 and self.row_count > 100:
                        warnings.append({
                            "column": col,
                            "type": "synthetic_data",
                            "severity": "low",
                            "message": f"Column '{col}' has low cardinality ({self.df[col].nunique()} unique values in {self.row_count} rows) - possible synthetic data",
                            "unique_ratio": round(unique_ratio, 3)
                        })
        
        return warnings
    
    def _analyze_correlations(self) -> Dict[str, Any]:
        """Analyze cross-column correlations (numeric only)"""
        numeric_df = self.df.select_dtypes(include=[np.number])
        
        if len(numeric_df.columns) < 2:
            return {"message": "Not enough numeric columns for correlation analysis"}
        
        try:
            corr_matrix = numeric_df.corr()
            
            # Find strong correlations (>0.7 or <-0.7, excluding diagonal)
            strong_correlations = []
            for i in range(len(corr_matrix.columns)):
                for j in range(i + 1, len(corr_matrix.columns)):
                    corr_val = corr_matrix.iloc[i, j]
                    if abs(corr_val) > 0.7:
                        strong_correlations.append({
                            "column1": corr_matrix.columns[i],
                            "column2": corr_matrix.columns[j],
                            "correlation": round(float(corr_val), 3)
                        })
            
            return {
                "strong_correlations": strong_correlations,
                "num_columns_analyzed": len(numeric_df.columns)
            }
        except:
            return {"error": "Could not compute correlations"}
    
    def _analyze_uniqueness(self) -> List[Dict[str, Any]]:
        """Detect uniqueness signals and synthetic patterns"""
        signals = []
        
        for col in self.df.columns:
            unique_count = self.df[col].nunique()
            unique_ratio = unique_count / self.row_count if self.row_count > 0 else 0
            
            # Perfect uniqueness (potential primary key)
            if unique_ratio == 1.0:
                signals.append({
                    "column": col,
                    "type": "unique",
                    "message": f"Column '{col}' is 100% unique - potential primary key"
                })
                
                # Check if it's sequential integers (synthetic data signal)
                if self.df[col].dtype in ['int64', 'int32']:
                    series = self.df[col].dropna().sort_values()
                    if len(series) > 1:
                        diffs = series.diff().dropna()
                        if (diffs == 1).all():
                            signals.append({
                                "column": col,
                                "type": "sequential",
                                "message": f"Column '{col}' contains sequential integers (1, 2, 3...) - synthetic/auto-increment pattern"
                            })
            
            # Check if two columns are identical (data duplication)
            for other_col in self.df.columns:
                if col < other_col:  # Avoid duplicate checks
                    try:
                        if self.df[col].equals(self.df[other_col]):
                            signals.append({
                                "column": f"{col} == {other_col}",
                                "type": "duplicate_columns",
                                "message": f"Columns '{col}' and '{other_col}' are identical - redundant data"
                            })
                    except:
                        pass
        
        return signals
    
    def _detect_outliers(self) -> Dict[str, Any]:
        """Detect outliers using IQR method for numeric columns"""
        numeric_cols = self.df.select_dtypes(include=[np.number]).columns
        outlier_summary = {}
        
        for col in numeric_cols:
            series = self.df[col].dropna()
            if len(series) < 4:  # Need at least 4 values for IQR
                continue
            
            q1 = series.quantile(0.25)
            q3 = series.quantile(0.75)
            iqr = q3 - q1
            
            if iqr == 0:
                continue  # No variance
            
            lower_bound = q1 - 1.5 * iqr
            upper_bound = q3 + 1.5 * iqr
            
            outliers = series[(series < lower_bound) | (series > upper_bound)]
            outlier_count = len(outliers)
            
            if outlier_count > 0:
                outlier_summary[col] = {
                    "count": outlier_count,
                    "percentage": round(outlier_count / len(series) * 100, 2),
                    "lower_bound": round(float(lower_bound), 2),
                    "upper_bound": round(float(upper_bound), 2),
                    "outlier_values": sorted([round(float(x), 2) for x in outliers.head(5).tolist()])
                }
        
        return outlier_summary


def generate_bronze_eda(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Main entry point for Bronze EDA analysis.
    Returns comprehensive data quality insights.
    """
    try:
        analyzer = BronzeEDAAnalyzer(df)
        return analyzer.analyze()
    except Exception as e:
        logger.error(f"Error generating Bronze EDA: {e}")
        return {"error": str(e)}

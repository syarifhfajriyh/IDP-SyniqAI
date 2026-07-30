"""
Gold Layer EDA Engine

Performs comprehensive Exploratory Data Analysis on Silver layer data.
Generates detailed statistical analysis, distributions, correlations, and insights.
"""

import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime
import json
import logging
from scipy import stats
from collections import Counter

logger = logging.getLogger(__name__)


class GoldEDAEngine:
    """
    Comprehensive EDA Engine for Gold Layer data analysis.
    Performs statistical analysis, data profiling, and insight generation.
    """
    
    def __init__(self):
        """Initialize the EDA Engine."""
        self.logger = logger
        self.eda_results = {}
        
    def analyze_dataframe(self, df: pd.DataFrame, table_name: str, 
                         source_system: str) -> Dict[str, Any]:
        """
        Perform comprehensive EDA on a DataFrame.
        
        Args:
            df: Input DataFrame from Silver layer
            table_name: Name of the table being analyzed
            source_system: Source system identifier
            
        Returns:
            Dictionary containing all EDA results
        """
        self.logger.info(f"Starting EDA for {source_system}.{table_name}")
        
        eda_report = {
            'table_name': table_name,
            'source_system': source_system,
            'analysis_timestamp': datetime.now().isoformat(),
            'basic_info': self._get_basic_info(df),
            'data_quality': self._assess_data_quality(df),
            'column_analysis': self._analyze_columns(df),
            'statistical_summary': self._get_statistical_summary(df),
            'correlations': self._compute_correlations(df),
            'distributions': self._analyze_distributions(df),
            'outliers': self._detect_outliers(df),
            'patterns': self._detect_patterns(df),
            'insights': self._generate_insights(df),
            'business_metrics': self._calculate_business_metrics(df, table_name)
        }
        
        self.logger.info(f"EDA completed for {source_system}.{table_name}")
        return eda_report
    
    def _get_basic_info(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Get basic DataFrame information."""
        return {
            'row_count': len(df),
            'column_count': len(df.columns),
            'memory_usage_mb': df.memory_usage(deep=True).sum() / (1024 * 1024),
            'duplicates': df.duplicated().sum(),
            'columns': list(df.columns),
            'dtypes': df.dtypes.astype(str).to_dict(),
            'shape': df.shape
        }
    
    def _assess_data_quality(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Assess overall data quality metrics."""
        total_cells = df.shape[0] * df.shape[1]
        missing_cells = df.isnull().sum().sum()
        
        quality_metrics = {
            'completeness_score': ((total_cells - missing_cells) / total_cells * 100) if total_cells > 0 else 0,
            'missing_value_count': int(missing_cells),
            'missing_percentage': (missing_cells / total_cells * 100) if total_cells > 0 else 0,
            'duplicate_rows': int(df.duplicated().sum()),
            'duplicate_percentage': (df.duplicated().sum() / len(df) * 100) if len(df) > 0 else 0,
            'columns_with_missing': df.isnull().any().sum(),
            'missing_by_column': df.isnull().sum().to_dict()
        }
        
        # Data quality score (0-100)
        completeness = quality_metrics['completeness_score']
        duplicate_penalty = min(quality_metrics['duplicate_percentage'], 20)
        quality_metrics['overall_quality_score'] = max(0, completeness - duplicate_penalty)
        
        return quality_metrics
    
    def _analyze_columns(self, df: pd.DataFrame) -> Dict[str, Dict[str, Any]]:
        """Perform detailed analysis on each column."""
        column_analysis = {}
        
        for col in df.columns:
            col_data = df[col]
            analysis = {
                'dtype': str(col_data.dtype),
                'non_null_count': int(col_data.count()),
                'null_count': int(col_data.isnull().sum()),
                'null_percentage': float(col_data.isnull().sum() / len(df) * 100) if len(df) > 0 else 0,
                'unique_count': int(col_data.nunique()),
                'unique_percentage': float(col_data.nunique() / len(df) * 100) if len(df) > 0 else 0
            }
            
            # Type-specific analysis
            # Skip boolean columns for numeric analysis
            if pd.api.types.is_bool_dtype(col_data):
                analysis.update(self._analyze_categorical_column(col_data))
            elif pd.api.types.is_numeric_dtype(col_data):
                analysis.update(self._analyze_numeric_column(col_data))
            elif pd.api.types.is_datetime64_any_dtype(col_data):
                analysis.update(self._analyze_datetime_column(col_data))
            elif pd.api.types.is_string_dtype(col_data) or col_data.dtype == 'object':
                analysis.update(self._analyze_categorical_column(col_data))
            
            column_analysis[col] = analysis
        
        return column_analysis
    
    def _analyze_numeric_column(self, series: pd.Series) -> Dict[str, Any]:
        """Analyze numeric column."""
        series_clean = series.dropna()
        
        if len(series_clean) == 0:
            return {'type': 'numeric', 'error': 'No non-null values'}
        
        return {
            'type': 'numeric',
            'min': float(series_clean.min()),
            'max': float(series_clean.max()),
            'mean': float(series_clean.mean()),
            'median': float(series_clean.median()),
            'std': float(series_clean.std()),
            'variance': float(series_clean.var()),
            'q25': float(series_clean.quantile(0.25)),
            'q75': float(series_clean.quantile(0.75)),
            'iqr': float(series_clean.quantile(0.75) - series_clean.quantile(0.25)),
            'skewness': float(series_clean.skew()),
            'kurtosis': float(series_clean.kurtosis()),
            'zeros_count': int((series_clean == 0).sum()),
            'negative_count': int((series_clean < 0).sum()),
            'positive_count': int((series_clean > 0).sum())
        }
    
    def _analyze_datetime_column(self, series: pd.Series) -> Dict[str, Any]:
        """Analyze datetime column."""
        series_clean = series.dropna()
        
        if len(series_clean) == 0:
            return {'type': 'datetime', 'error': 'No non-null values'}
        
        return {
            'type': 'datetime',
            'min_date': series_clean.min().isoformat() if pd.notna(series_clean.min()) else None,
            'max_date': series_clean.max().isoformat() if pd.notna(series_clean.max()) else None,
            'date_range_days': (series_clean.max() - series_clean.min()).days if pd.notna(series_clean.max()) and pd.notna(series_clean.min()) else None
        }
    
    def _analyze_categorical_column(self, series: pd.Series) -> Dict[str, Any]:
        """Analyze categorical/text column."""
        series_clean = series.dropna()
        
        if len(series_clean) == 0:
            return {'type': 'categorical', 'error': 'No non-null values'}
        
        value_counts = series_clean.value_counts()
        
        analysis = {
            'type': 'categorical',
            'most_frequent': str(value_counts.index[0]) if len(value_counts) > 0 else None,
            'most_frequent_count': int(value_counts.iloc[0]) if len(value_counts) > 0 else 0,
            'least_frequent': str(value_counts.index[-1]) if len(value_counts) > 0 else None,
            'least_frequent_count': int(value_counts.iloc[-1]) if len(value_counts) > 0 else 0,
            'top_10_values': value_counts.head(10).to_dict()
        }
        
        # Text statistics if string
        if series_clean.dtype == 'object':
            lengths = series_clean.astype(str).str.len()
            analysis.update({
                'avg_length': float(lengths.mean()),
                'min_length': int(lengths.min()),
                'max_length': int(lengths.max())
            })
        
        return analysis
    
    def _get_statistical_summary(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Get comprehensive statistical summary."""
        # Exclude boolean columns from numeric analysis
        numeric_df = df.select_dtypes(include=[np.number]).select_dtypes(exclude=['bool'])
        
        if numeric_df.empty:
            return {'message': 'No numeric columns found'}
        
        return {
            'describe': numeric_df.describe().to_dict(),
            'numeric_column_count': len(numeric_df.columns),
            'numeric_columns': list(numeric_df.columns)
        }
    
    def _compute_correlations(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Compute correlation matrices for numeric columns."""
        # Exclude boolean columns from correlation analysis
        numeric_df = df.select_dtypes(include=[np.number]).select_dtypes(exclude=['bool'])
        
        if numeric_df.empty or len(numeric_df.columns) < 2:
            return {'message': 'Insufficient numeric columns for correlation'}
        
        corr_matrix = numeric_df.corr()
        
        # Find strong correlations (>0.7 or <-0.7)
        strong_correlations = []
        for i in range(len(corr_matrix.columns)):
            for j in range(i+1, len(corr_matrix.columns)):
                corr_value = corr_matrix.iloc[i, j]
                if abs(corr_value) > 0.7:
                    strong_correlations.append({
                        'column1': corr_matrix.columns[i],
                        'column2': corr_matrix.columns[j],
                        'correlation': float(corr_value)
                    })
        
        return {
            'correlation_matrix': corr_matrix.to_dict(),
            'strong_correlations': strong_correlations
        }
    
    def _analyze_distributions(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze distributions of numeric columns."""
        # Exclude boolean columns from distribution analysis
        numeric_df = df.select_dtypes(include=[np.number]).select_dtypes(exclude=['bool'])
        
        if numeric_df.empty:
            return {'message': 'No numeric columns found'}
        
        distributions = {}
        for col in numeric_df.columns:
            col_data = numeric_df[col].dropna()
            if len(col_data) > 0:
                distributions[col] = {
                    'distribution_type': self._identify_distribution(col_data),
                    'histogram': self._get_histogram_data(col_data)
                }
        
        return distributions
    
    def _identify_distribution(self, series: pd.Series) -> str:
        """Identify the likely distribution type."""
        if len(series) < 8:
            return 'insufficient_data'
        
        # Normality test
        try:
            _, p_value = stats.normaltest(series)
            if p_value > 0.05:
                return 'normal'
        except:
            pass
        
        # Check skewness
        skew = series.skew()
        if abs(skew) < 0.5:
            return 'symmetric'
        elif skew > 0.5:
            return 'right_skewed'
        else:
            return 'left_skewed'
    
    def _get_histogram_data(self, series: pd.Series, bins: int = 10) -> Dict[str, List]:
        """Get histogram data for visualization."""
        counts, bin_edges = np.histogram(series, bins=bins)
        return {
            'counts': counts.tolist(),
            'bin_edges': bin_edges.tolist()
        }
    
    def _detect_outliers(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Detect outliers using IQR method."""
        # Exclude boolean columns from outlier analysis
        numeric_df = df.select_dtypes(include=[np.number]).select_dtypes(exclude=['bool'])
        
        if numeric_df.empty:
            return {'message': 'No numeric columns found'}
        
        outliers = {}
        for col in numeric_df.columns:
            col_data = numeric_df[col].dropna()
            if len(col_data) > 0:
                Q1 = col_data.quantile(0.25)
                Q3 = col_data.quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                outlier_mask = (col_data < lower_bound) | (col_data > upper_bound)
                outlier_count = outlier_mask.sum()
                
                if outlier_count > 0:
                    outliers[col] = {
                        'count': int(outlier_count),
                        'percentage': float(outlier_count / len(col_data) * 100),
                        'lower_bound': float(lower_bound),
                        'upper_bound': float(upper_bound)
                    }
        
        return outliers
    
    def _detect_patterns(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Detect patterns and anomalies in the data."""
        patterns = {}
        
        # Check for constant columns
        constant_cols = [col for col in df.columns if df[col].nunique() <= 1]
        if constant_cols:
            patterns['constant_columns'] = constant_cols
        
        # Check for potential ID columns (high cardinality)
        id_cols = [col for col in df.columns 
                   if df[col].nunique() / len(df) > 0.95 and len(df) > 10]
        if id_cols:
            patterns['potential_id_columns'] = id_cols
        
        # Check for low cardinality columns
        low_card_cols = [col for col in df.columns 
                        if df[col].nunique() < 10 and len(df) > 10]
        if low_card_cols:
            patterns['low_cardinality_columns'] = low_card_cols
        
        return patterns
    
    def _generate_insights(self, df: pd.DataFrame) -> List[str]:
        """Generate automated insights from the data."""
        insights = []
        
        # Data size insights
        if len(df) > 1000000:
            insights.append(f"Large dataset with {len(df):,} rows - consider partitioning")
        
        # Data quality insights
        missing_pct = (df.isnull().sum().sum() / (df.shape[0] * df.shape[1])) * 100
        if missing_pct > 10:
            insights.append(f"High missing data rate: {missing_pct:.1f}% - review data collection")
        
        # Duplicate insights
        dup_pct = (df.duplicated().sum() / len(df)) * 100
        if dup_pct > 5:
            insights.append(f"Significant duplicates: {dup_pct:.1f}% - verify data ingestion")
        
        # Column insights
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            col_data = df[col].dropna()
            if len(col_data) > 0:
                zeros_pct = ((col_data == 0).sum() / len(col_data)) * 100
                if zeros_pct > 50:
                    insights.append(f"Column '{col}' has {zeros_pct:.1f}% zeros - verify validity")
        
        if not insights:
            insights.append("Data quality looks good - no major issues detected")
        
        return insights
    
    def _calculate_business_metrics(self, df: pd.DataFrame, 
                                    table_name: str) -> Dict[str, Any]:
        """Calculate business-specific metrics based on table type."""
        metrics = {}
        
        # Transaction metrics
        if 'transaction' in table_name.lower():
            if 'amount' in df.columns:
                metrics['total_transaction_amount'] = float(df['amount'].sum())
                metrics['avg_transaction_amount'] = float(df['amount'].mean())
                metrics['max_transaction_amount'] = float(df['amount'].max())
            
            if 'transaction_date' in df.columns or 'date' in df.columns:
                date_col = 'transaction_date' if 'transaction_date' in df.columns else 'date'
                try:
                    df[date_col] = pd.to_datetime(df[date_col])
                    metrics['date_range'] = {
                        'start': df[date_col].min().isoformat(),
                        'end': df[date_col].max().isoformat()
                    }
                except:
                    pass
        
        # Patient/record metrics
        if 'patient' in table_name.lower() or 'record' in table_name.lower():
            if 'patient_id' in df.columns:
                metrics['unique_patients'] = int(df['patient_id'].nunique())
            
            if 'age' in df.columns:
                metrics['avg_age'] = float(df['age'].mean())
                metrics['age_distribution'] = {
                    'min': float(df['age'].min()),
                    'max': float(df['age'].max()),
                    'median': float(df['age'].median())
                }
        
        return metrics

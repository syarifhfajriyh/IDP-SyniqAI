import pandas as pd
import numpy as np
import yaml
from datetime import datetime
import logging
from typing import Dict, List, Tuple
import re

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SilverTransformer:
    """
    Pandas-based data cleaning and transformation for Silver layer.
    Applies cleaning rules from config/cleaning_rules.yaml
    """
    
    def __init__(self, config_path: str = "config/cleaning_rules.yaml"):
        """
        Initialize the transformer with cleaning rules configuration.
        
        Args:
            config_path: Path to the cleaning rules YAML configuration file
        """
        self.logger = logger
        self.config = self._load_config(config_path)
        self.cleaning_summary = {}
        
        
    def _load_config(self, config_path: str) -> Dict:
        """Load cleaning rules from YAML configuration file."""
        try:
            with open(config_path, 'r') as f:
                config = yaml.safe_load(f)
            self.logger.info(f"Configuration loaded from {config_path}")
            return config
        except FileNotFoundError:
            self.logger.error(f"Configuration file not found: {config_path}")
            raise
        except yaml.YAMLError as e:
            self.logger.error(f"Error parsing YAML: {e}")
            raise
    
    def transform(self, df: pd.DataFrame, source_system: str = "postgres", 
                  bronze_location: str = None) -> pd.DataFrame:
        """
        Apply all cleaning transformations to the dataframe.
        
        Args:
            df: Input DataFrame (from Bronze layer)
            source_system: Source system name (e.g., "postgres", "mongodb")
            bronze_location: Path to the original bronze file
            
        Returns:
            Cleaned DataFrame with metadata columns
        """
        self.logger.info("Starting data transformation...")
        original_row_count = len(df)
        
        # Store initial state
        self.cleaning_summary['original_row_count'] = original_row_count
        self.cleaning_summary['original_column_count'] = len(df.columns)
        
        # 1. Missing Value Handling
        df = self._handle_missing_values(df)
        
        # 2. Duplicate Removal
        df = self._remove_duplicates(df)
        
        # 3. Outlier Treatment
        df = self._handle_outliers(df)
        
        # 4. Data Type Standardization
        df = self._standardize_types(df)
        
        # 5. Validation Rules
        df = self._apply_validation_rules(df)
        
        # 6. Add Metadata Columns
        df = self._add_metadata_columns(df, source_system, bronze_location, original_row_count)
        
        self.logger.info("Data transformation completed")
        return df
    
    def _handle_missing_values(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle missing values according to configuration.
        - Drop columns with >80% missing
        - Fill numerical: median/mean
        - Fill categorical: mode/"Unknown"
        - Time series: forward/backward fill
        """
        self.logger.info("Handling missing values...")
        missing_config = self.config.get('missing_values', {})
        threshold = missing_config.get('drop_threshold', 0.80)
        
        # Drop columns exceeding threshold
        initial_cols = len(df.columns)
        missing_pct = df.isnull().sum() / len(df)
        cols_to_drop = missing_pct[missing_pct > threshold].index.tolist()
        
        if cols_to_drop:
            self.logger.info(f"Dropping columns with >{threshold*100}% missing: {cols_to_drop}")
            df = df.drop(columns=cols_to_drop)
        
        self.cleaning_summary['cols_dropped'] = len(cols_to_drop)
        
        # Fill numerical columns
        numerical_fill = missing_config.get('numerical_fill_method', 'median')
        for col in df.select_dtypes(include=[np.number]).columns:
            if df[col].isnull().any():
                if numerical_fill == 'median':
                    df[col].fillna(df[col].median(), inplace=True)
                elif numerical_fill == 'mean':
                    df[col].fillna(df[col].mean(), inplace=True)
                else:
                    df[col].fillna(df[col].median(), inplace=True)
        
        # Fill categorical columns
        categorical_fill = missing_config.get('categorical_fill_value', 'Unknown')
        for col in df.select_dtypes(include=['object']).columns:
            if df[col].isnull().any():
                df[col].fillna(categorical_fill, inplace=True)
        
        self.logger.info(f"Missing values handled. Columns remaining: {len(df.columns)}")
        return df
    
    def _remove_duplicates(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove duplicate rows.
        - Exact duplicates: keep first
        - Priority: latest timestamp wins
        """
        self.logger.info("Removing duplicates...")
        duplicate_config = self.config.get('duplicates', {})
        initial_rows = len(df)
        
        # Remove exact duplicates (keep first occurrence)
        df = df.drop_duplicates(keep='first')
        
        exact_dups = initial_rows - len(df)
        self.cleaning_summary['exact_duplicates_removed'] = exact_dups
        
        self.logger.info(f"Removed {exact_dups} exact duplicate rows")
        return df
    
    def _handle_outliers(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Handle outliers using IQR method and capping/flooring.
        - Cap/floor at percentiles
        - Remove: beyond 3-sigma
        - Transform: log/sqrt for skewed data
        - Flag: add _has_outlier column
        """
        self.logger.info("Handling outliers...")
        outlier_config = self.config.get('outliers', {})
        method = outlier_config.get('method', 'iqr')
        
        for col in df.select_dtypes(include=[np.number]).columns:
            if col.startswith('_'):  # Skip metadata columns
                continue
                
            if method == 'iqr':
                Q1 = df[col].quantile(0.25)
                Q3 = df[col].quantile(0.75)
                IQR = Q3 - Q1
                lower_bound = Q1 - 1.5 * IQR
                upper_bound = Q3 + 1.5 * IQR
                
                # Flag outliers
                has_outliers = (df[col] < lower_bound) | (df[col] > upper_bound)
                if has_outliers.any():
                    df[f'{col}_has_outlier'] = has_outliers
                    
                    # Cap/floor
                    df[col] = df[col].clip(lower=lower_bound, upper=upper_bound)
                    self.logger.info(f"Handled {has_outliers.sum()} outliers in {col}")
        
        return df
    
    def _standardize_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Standardize data types according to configuration.
        - Dates: ISO 8601 (YYYY-MM-DD)
        - Phone: E.164 format (+60123456789)
        - Email: lowercase, trim
        - Currency: decimal(10,2)
        """
        self.logger.info("Standardizing data types...")
        type_config = self.config.get('type_standardization', {})
        
        # Process date columns
        date_columns = type_config.get('date_columns', [])
        for col in date_columns:
            if col in df.columns:
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce').dt.strftime('%Y-%m-%d')
                    self.logger.info(f"Standardized date column: {col}")
                except Exception as e:
                    self.logger.warning(f"Could not standardize {col}: {e}")
        
        # Process phone columns
        phone_columns = type_config.get('phone_columns', [])
        for col in phone_columns:
            if col in df.columns:
                df[col] = df[col].apply(self._format_phone)
        
        # Process email columns
        email_columns = type_config.get('email_columns', [])
        for col in email_columns:
            if col in df.columns:
                df[col] = df[col].str.lower().str.strip()
        
        # Process currency columns
        currency_columns = type_config.get('currency_columns', [])
        for col in currency_columns:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce').round(2)
        
        return df
    
    def _format_phone(self, phone: str) -> str:
        """Format phone number to E.164 format (+60123456789)."""
        if pd.isna(phone):
            return phone
        
        # Remove non-digits
        digits = re.sub(r'\D', '', str(phone))
        
        # Add country code if not present (assumes +60 for Malaysia)
        if len(digits) == 10:
            digits = '60' + digits[1:]
        
        return f"+{digits}" if digits else phone
    
    def _apply_validation_rules(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Apply business logic validation rules.
        - Age: 0 ≤ age ≤ 120
        - Email: regex pattern
        - Custom constraints
        """
        self.logger.info("Applying validation rules...")
        validation_config = self.config.get('validation_rules', {})
        invalid_rows = []
        
        # Age validation
        age_cols = validation_config.get('age_columns', [])
        for col in age_cols:
            if col in df.columns:
                invalid = ~df[col].between(0, 120)
                invalid_count = invalid.sum()
                if invalid_count > 0:
                    self.logger.warning(f"{invalid_count} invalid age values in {col}")
                    df.loc[invalid, col] = np.nan
        
        # Email validation
        email_cols = validation_config.get('email_columns', [])
        email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
        for col in email_cols:
            if col in df.columns:
                invalid = ~df[col].str.match(email_pattern, na=False)
                invalid_count = invalid.sum()
                if invalid_count > 0:
                    self.logger.warning(f"{invalid_count} invalid email values in {col}")
                    df.loc[invalid, col] = np.nan
        
        return df
    
    def _add_metadata_columns(self, df: pd.DataFrame, source_system: str, 
                             bronze_location: str, original_row_count: int) -> pd.DataFrame:
        """
        Add metadata columns to track data lineage and quality.
        """
        self.logger.info("Adding metadata columns...")
        now = datetime.now()
        
        df['_source_system'] = source_system
        df['_ingestion_timestamp'] = pd.Timestamp.now()
        df['_processing_timestamp'] = now
        df['_quality_score'] = self._calculate_quality_score(df)
        df['_has_anomalies'] = df.filter(regex='_has_outlier$').any(axis=1)
        df['_bronze_location'] = bronze_location or "unknown"
        df['_rows_processed'] = original_row_count
        df['_rows_remaining'] = len(df)
        
        self.cleaning_summary['final_row_count'] = len(df)
        self.cleaning_summary['rows_removed'] = original_row_count - len(df)
        
        return df
    
    def _calculate_quality_score(self, df: pd.DataFrame) -> float:
        """Calculate overall data quality score (0-100)."""
        total_cells = df.size
        null_cells = df.isnull().sum().sum()
        completeness = (1 - (null_cells / total_cells)) * 100 if total_cells > 0 else 100
        return round(completeness, 2)
    
    def get_cleaning_summary(self) -> Dict:
        """Return summary of cleaning operations performed."""
        return self.cleaning_summary
    
    def save_to_parquet(self, df: pd.DataFrame, output_path: str) -> None:
        """Save cleaned dataframe to Parquet format."""
        try:
            df.to_parquet(output_path, index=False)
            self.logger.info(f"Saved cleaned data to {output_path}")
        except Exception as e:
            self.logger.error(f"Error saving to Parquet: {e}")
            raise


# Example usage:
if __name__ == "__main__":
    # Example: Load data and transform
    # df = pd.read_parquet("bronze/postgres/hosp_raya_patient_record/data.parquet")
    # transformer = SilverTransformer(config_path="config/cleaning_rules.yaml")
    # cleaned_df = transformer.transform(df, source_system="postgres", bronze_location="bronze/postgres/...")
    # summary = transformer.get_cleaning_summary()
    # print(summary)
    pass
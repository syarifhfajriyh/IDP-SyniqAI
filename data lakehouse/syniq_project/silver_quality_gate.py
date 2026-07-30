import pandas as pd
from pandas.api.types import is_numeric_dtype

class SilverQualityGate:
    def __init__(self, dataframe):
        self.dataframe = dataframe
        self.issues = []

    def schema_validation(self, expected_schema):
        """ Check if the DataFrame matches the expected schema. """
        for column, dtype in expected_schema.items():
            if column not in self.dataframe.columns:
                self.issues.append(f"Missing column: {column}")
            elif not is_numeric_dtype(self.dataframe[column]) and dtype == 'numeric':
                self.issues.append(f"Column '{column}' should be numeric.")

    def missing_value_detection(self):
        """ Detect missing values in the DataFrame. """
        missing = self.dataframe.isnull().sum()
        for column in missing.index:
            if missing[column] > 0:
                self.issues.append(f"Missing values found in column: {column}")

    def duplicate_detection(self):
        """ Detect duplicate rows in the DataFrame. """
        duplicates = self.dataframe.duplicated().sum()
        if duplicates > 0:
            self.issues.append(f"{duplicates} duplicate rows found.")

    def outlier_detection(self):
        """ Simple outlier detection using IQR. """
        for column in self.dataframe.select_dtypes(include=['float64', 'int64']):
            Q1 = self.dataframe[column].quantile(0.25)
            Q3 = self.dataframe[column].quantile(0.75)
            IQR = Q3 - Q1
            lower_bound = Q1 - 1.5 * IQR
            upper_bound = Q3 + 1.5 * IQR
            outliers = self.dataframe[(self.dataframe[column] < lower_bound) | (self.dataframe[column] > upper_bound)]
            if not outliers.empty:
                self.issues.append(f"Outliers found in column: {column}")

    def data_volume_check(self, min_rows):
        """ Check if the DataFrame meets the minimum volume requirement. """
        if len(self.dataframe) < min_rows:
            self.issues.append(f"Data volume check failed: only {len(self.dataframe)} rows present.")

    def basic_type_consistency(self):
        """ Check basic type consistency for expected types. """
        for column in self.dataframe.columns:
            if self.dataframe[column].dtype == 'object':
                self.issues.append(f"Column '{column}' has inconsistent types.")

    def perform_quality_checks(self, expected_schema, min_rows):
        """ Perform all quality checks. """
        self.schema_validation(expected_schema)
        self.missing_value_detection()
        self.duplicate_detection()
        self.outlier_detection()
        self.data_volume_check(min_rows)
        self.basic_type_consistency()
        return self.issues

# Example usage:
# df = pd.DataFrame(...)  # Your DataFrame here
# qg = SilverQualityGate(df)
# issues = qg.perform_quality_checks(expected_schema, min_rows)

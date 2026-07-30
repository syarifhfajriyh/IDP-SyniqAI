"""
Data Quality Validation Framework
==================================
Provides comprehensive data quality validation with customizable rules.

Features:
- Built-in validation rules (null checks, type validation, range checks, pattern matching)
- Custom validation rules support
- Validation report generation
- Row-level and column-level validation
- Thresholds for warnings vs errors
- Integration with logger
"""

import re
from typing import List, Dict, Optional, Any, Callable, Union
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
import pandas as pd
import numpy as np

from utils.logger import get_logger, log_validation_results

logger = get_logger()


class ValidationSeverity(Enum):
    """Severity levels for validation results"""
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class ValidationResult:
    """Result of a validation rule"""
    
    rule_name: str
    passed: bool
    severity: ValidationSeverity
    message: str
    affected_rows: int = 0
    affected_columns: List[str] = field(default_factory=list)
    details: Dict[str, Any] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "rule_name": self.rule_name,
            "passed": self.passed,
            "severity": self.severity.value,
            "message": self.message,
            "affected_rows": self.affected_rows,
            "affected_columns": self.affected_columns,
            "details": self.details
        }


@dataclass
class ValidationReport:
    """Comprehensive validation report"""
    
    entity: str
    total_rows: int
    validation_timestamp: datetime = field(default_factory=datetime.now)
    results: List[ValidationResult] = field(default_factory=list)
    
    def add_result(self, result: ValidationResult):
        """Add validation result to report"""
        self.results.append(result)
    
    def get_failures(self) -> List[ValidationResult]:
        """Get all failed validations"""
        return [r for r in self.results if not r.passed]
    
    def get_by_severity(self, severity: ValidationSeverity) -> List[ValidationResult]:
        """Get results by severity level"""
        return [r for r in self.results if r.severity == severity]
    
    def has_errors(self) -> bool:
        """Check if report contains errors or critical issues"""
        return any(
            r.severity in (ValidationSeverity.ERROR, ValidationSeverity.CRITICAL)
            for r in self.get_failures()
        )
    
    def has_warnings(self) -> bool:
        """Check if report contains warnings"""
        return any(
            r.severity == ValidationSeverity.WARNING
            for r in self.get_failures()
        )
    
    def summary(self) -> Dict[str, Any]:
        """Get summary statistics"""
        failures = self.get_failures()
        return {
            "entity": self.entity,
            "total_rows": self.total_rows,
            "total_rules": len(self.results),
            "passed_rules": len(self.results) - len(failures),
            "failed_rules": len(failures),
            "errors": len(self.get_by_severity(ValidationSeverity.ERROR)),
            "warnings": len(self.get_by_severity(ValidationSeverity.WARNING)),
            "has_errors": self.has_errors(),
            "has_warnings": self.has_warnings(),
            "validation_timestamp": self.validation_timestamp.isoformat()
        }
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert full report to dictionary"""
        return {
            "summary": self.summary(),
            "results": [r.to_dict() for r in self.results]
        }
    
    def log_summary(self):
        """Log validation summary"""
        summary = self.summary()
        log_validation_results(
            entity=self.entity,
            total_rows=self.total_rows,
            validation_rules=summary["total_rules"],
            failures=summary["failed_rules"],
            warnings=summary["warnings"]
        )
    
    def __str__(self) -> str:
        summary = self.summary()
        status = "❌ FAILED" if self.has_errors() else ("⚠️ WARNINGS" if self.has_warnings() else "✅ PASSED")
        return (
            f"Validation Report [{status}]\n"
            f"  Entity: {self.entity}\n"
            f"  Rows: {self.total_rows:,}\n"
            f"  Rules: {summary['total_rules']} ({summary['passed_rules']} passed, {summary['failed_rules']} failed)\n"
            f"  Errors: {summary['errors']}\n"
            f"  Warnings: {summary['warnings']}"
        )


class ValidationRule:
    """Base class for validation rules"""
    
    def __init__(
        self,
        name: str,
        severity: ValidationSeverity = ValidationSeverity.ERROR
    ):
        """
        Initialize validation rule
        
        Args:
            name: Rule name
            severity: Severity level for failures
        """
        self.name = name
        self.severity = severity
    
    def validate(self, df: pd.DataFrame) -> ValidationResult:
        """
        Validate DataFrame against rule
        
        Args:
            df: Input DataFrame
        
        Returns:
            ValidationResult
        """
        raise NotImplementedError("Subclasses must implement validate()")


class NotNullRule(ValidationRule):
    """Validate that columns do not contain null values"""
    
    def __init__(
        self,
        columns: Union[str, List[str]],
        threshold: float = 0.0,
        severity: ValidationSeverity = ValidationSeverity.ERROR
    ):
        """
        Args:
            columns: Column name(s) to check
            threshold: Maximum acceptable null percentage (0.0 to 1.0)
            severity: Severity level
        """
        super().__init__(name=f"NotNull({columns})", severity=severity)
        self.columns = [columns] if isinstance(columns, str) else columns
        self.threshold = threshold
    
    def validate(self, df: pd.DataFrame) -> ValidationResult:
        missing_cols = [col for col in self.columns if col not in df.columns]
        if missing_cols:
            return ValidationResult(
                rule_name=self.name,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message=f"Columns not found: {missing_cols}",
                affected_columns=missing_cols
            )
        
        null_counts = {}
        total_nulls = 0
        for col in self.columns:
            null_count = df[col].isna().sum()
            null_counts[col] = null_count
            total_nulls += null_count
        
        null_percentage = total_nulls / (len(df) * len(self.columns)) if len(df) > 0 else 0
        passed = null_percentage <= self.threshold
        
        return ValidationResult(
            rule_name=self.name,
            passed=passed,
            severity=self.severity,
            message=f"Null check: {null_percentage*100:.2f}% nulls (threshold: {self.threshold*100:.2f}%)",
            affected_rows=total_nulls,
            affected_columns=self.columns,
            details={"null_counts": null_counts, "null_percentage": null_percentage}
        )


class TypeValidationRule(ValidationRule):
    """Validate column data types"""
    
    def __init__(
        self,
        column_types: Dict[str, Union[type, str]],
        severity: ValidationSeverity = ValidationSeverity.ERROR
    ):
        """
        Args:
            column_types: Dictionary of column -> expected type
            severity: Severity level
        """
        super().__init__(name="TypeValidation", severity=severity)
        self.column_types = column_types
    
    def validate(self, df: pd.DataFrame) -> ValidationResult:
        type_mismatches = {}
        
        for col, expected_type in self.column_types.items():
            if col not in df.columns:
                type_mismatches[col] = f"Column not found"
                continue
            
            actual_type = df[col].dtype
            
            # Handle string type specifications
            if isinstance(expected_type, str):
                if expected_type == "integer" and not pd.api.types.is_integer_dtype(actual_type):
                    type_mismatches[col] = f"Expected integer, got {actual_type}"
                elif expected_type == "float" and not pd.api.types.is_float_dtype(actual_type):
                    type_mismatches[col] = f"Expected float, got {actual_type}"
                elif expected_type == "string" and not pd.api.types.is_string_dtype(actual_type) and actual_type != object:
                    type_mismatches[col] = f"Expected string, got {actual_type}"
                elif expected_type == "boolean" and not pd.api.types.is_bool_dtype(actual_type):
                    type_mismatches[col] = f"Expected boolean, got {actual_type}"
                elif expected_type == "datetime" and not pd.api.types.is_datetime64_any_dtype(actual_type):
                    type_mismatches[col] = f"Expected datetime, got {actual_type}"
        
        passed = len(type_mismatches) == 0
        
        return ValidationResult(
            rule_name=self.name,
            passed=passed,
            severity=self.severity,
            message=f"Type validation: {len(type_mismatches)} mismatches" if not passed else "All types correct",
            affected_columns=list(type_mismatches.keys()),
            details={"type_mismatches": type_mismatches}
        )


class RangeValidationRule(ValidationRule):
    """Validate numeric columns are within specified ranges"""
    
    def __init__(
        self,
        column: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        severity: ValidationSeverity = ValidationSeverity.WARNING
    ):
        """
        Args:
            column: Column name
            min_value: Minimum acceptable value
            max_value: Maximum acceptable value
            severity: Severity level
        """
        super().__init__(name=f"Range({column})", severity=severity)
        self.column = column
        self.min_value = min_value
        self.max_value = max_value
    
    def validate(self, df: pd.DataFrame) -> ValidationResult:
        if self.column not in df.columns:
            return ValidationResult(
                rule_name=self.name,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message=f"Column '{self.column}' not found",
                affected_columns=[self.column]
            )
        
        violations = 0
        details = {}
        
        if self.min_value is not None:
            below_min = (df[self.column] < self.min_value).sum()
            violations += below_min
            details["below_min"] = int(below_min)
        
        if self.max_value is not None:
            above_max = (df[self.column] > self.max_value).sum()
            violations += above_max
            details["above_max"] = int(above_max)
        
        passed = violations == 0
        
        return ValidationResult(
            rule_name=self.name,
            passed=passed,
            severity=self.severity,
            message=f"Range check: {violations} violations" if not passed else "All values in range",
            affected_rows=violations,
            affected_columns=[self.column],
            details=details
        )


class PatternValidationRule(ValidationRule):
    """Validate string columns match regex pattern"""
    
    def __init__(
        self,
        column: str,
        pattern: str,
        severity: ValidationSeverity = ValidationSeverity.WARNING
    ):
        """
        Args:
            column: Column name
            pattern: Regex pattern to match
            severity: Severity level
        """
        super().__init__(name=f"Pattern({column})", severity=severity)
        self.column = column
        self.pattern = pattern
        self.regex = re.compile(pattern)
    
    def validate(self, df: pd.DataFrame) -> ValidationResult:
        if self.column not in df.columns:
            return ValidationResult(
                rule_name=self.name,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message=f"Column '{self.column}' not found",
                affected_columns=[self.column]
            )
        
        # Check non-null values
        non_null = df[self.column].dropna()
        if len(non_null) == 0:
            return ValidationResult(
                rule_name=self.name,
                passed=True,
                severity=self.severity,
                message="No non-null values to validate",
                affected_columns=[self.column]
            )
        
        # Count pattern matches
        matches = non_null.astype(str).str.match(self.regex)
        violations = (~matches).sum()
        
        passed = violations == 0
        
        return ValidationResult(
            rule_name=self.name,
            passed=passed,
            severity=self.severity,
            message=f"Pattern check: {violations} violations" if not passed else "All values match pattern",
            affected_rows=violations,
            affected_columns=[self.column],
            details={"pattern": self.pattern, "violations": int(violations)}
        )


class UniqueRule(ValidationRule):
    """Validate column values are unique"""
    
    def __init__(
        self,
        columns: Union[str, List[str]],
        severity: ValidationSeverity = ValidationSeverity.ERROR
    ):
        """
        Args:
            columns: Column name(s) that should be unique
            severity: Severity level
        """
        super().__init__(name=f"Unique({columns})", severity=severity)
        self.columns = [columns] if isinstance(columns, str) else columns
    
    def validate(self, df: pd.DataFrame) -> ValidationResult:
        missing_cols = [col for col in self.columns if col not in df.columns]
        if missing_cols:
            return ValidationResult(
                rule_name=self.name,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message=f"Columns not found: {missing_cols}",
                affected_columns=missing_cols
            )
        
        # Check for duplicates
        duplicates = df.duplicated(subset=self.columns, keep=False).sum()
        passed = duplicates == 0
        
        return ValidationResult(
            rule_name=self.name,
            passed=passed,
            severity=self.severity,
            message=f"Uniqueness check: {duplicates} duplicates" if not passed else "All values unique",
            affected_rows=duplicates,
            affected_columns=self.columns,
            details={"duplicate_count": int(duplicates)}
        )


class CustomRule(ValidationRule):
    """Custom validation rule with user-defined function"""
    
    def __init__(
        self,
        name: str,
        validation_func: Callable[[pd.DataFrame], bool],
        error_message: str,
        severity: ValidationSeverity = ValidationSeverity.WARNING
    ):
        """
        Args:
            name: Rule name
            validation_func: Function that takes DataFrame and returns True if valid
            error_message: Message to show if validation fails
            severity: Severity level
        """
        super().__init__(name=name, severity=severity)
        self.validation_func = validation_func
        self.error_message = error_message
    
    def validate(self, df: pd.DataFrame) -> ValidationResult:
        try:
            passed = self.validation_func(df)
            message = "Custom rule passed" if passed else self.error_message
            
            return ValidationResult(
                rule_name=self.name,
                passed=passed,
                severity=self.severity,
                message=message
            )
        except Exception as e:
            return ValidationResult(
                rule_name=self.name,
                passed=False,
                severity=ValidationSeverity.ERROR,
                message=f"Custom rule failed with exception: {e}"
            )


class DataValidator:
    """Main validator class that applies multiple validation rules"""
    
    def __init__(self, entity: str):
        """
        Initialize validator
        
        Args:
            entity: Entity name being validated
        """
        self.entity = entity
        self.rules: List[ValidationRule] = []
    
    def add_rule(self, rule: ValidationRule):
        """Add validation rule"""
        self.rules.append(rule)
        return self  # Allow chaining
    
    def add_not_null(
        self,
        columns: Union[str, List[str]],
        threshold: float = 0.0,
        severity: ValidationSeverity = ValidationSeverity.ERROR
    ):
        """Add not-null validation rule"""
        self.add_rule(NotNullRule(columns, threshold, severity))
        return self
    
    def add_type_validation(
        self,
        column_types: Dict[str, Union[type, str]],
        severity: ValidationSeverity = ValidationSeverity.ERROR
    ):
        """Add type validation rule"""
        self.add_rule(TypeValidationRule(column_types, severity))
        return self
    
    def add_range_validation(
        self,
        column: str,
        min_value: Optional[float] = None,
        max_value: Optional[float] = None,
        severity: ValidationSeverity = ValidationSeverity.WARNING
    ):
        """Add range validation rule"""
        self.add_rule(RangeValidationRule(column, min_value, max_value, severity))
        return self
    
    def add_pattern_validation(
        self,
        column: str,
        pattern: str,
        severity: ValidationSeverity = ValidationSeverity.WARNING
    ):
        """Add pattern validation rule"""
        self.add_rule(PatternValidationRule(column, pattern, severity))
        return self
    
    def add_unique(
        self,
        columns: Union[str, List[str]],
        severity: ValidationSeverity = ValidationSeverity.ERROR
    ):
        """Add uniqueness validation rule"""
        self.add_rule(UniqueRule(columns, severity))
        return self
    
    def add_custom(
        self,
        name: str,
        validation_func: Callable[[pd.DataFrame], bool],
        error_message: str,
        severity: ValidationSeverity = ValidationSeverity.WARNING
    ):
        """Add custom validation rule"""
        self.add_rule(CustomRule(name, validation_func, error_message, severity))
        return self
    
    def validate(self, df: pd.DataFrame) -> ValidationReport:
        """
        Execute all validation rules
        
        Args:
            df: DataFrame to validate
        
        Returns:
            ValidationReport with all results
        """
        logger.info(f"Running {len(self.rules)} validation rules on {self.entity}")
        
        report = ValidationReport(
            entity=self.entity,
            total_rows=len(df)
        )
        
        for rule in self.rules:
            logger.debug(f"Executing rule: {rule.name}")
            result = rule.validate(df)
            report.add_result(result)
            
            if not result.passed:
                logger.warning(f"Validation failed: {rule.name} | {result.message}")
        
        # Log summary
        report.log_summary()
        
        return report


if __name__ == "__main__":
    # Demo usage
    print("\n" + "="*60)
    print("Data Validator Demo")
    print("="*60)
    
    # Create sample DataFrame with various data quality issues
    data = {
        "customer_id": [1, 2, 3, 4, 5, 5],  # Duplicate ID
        "customer_name": ["Alice", "Bob", None, "David", "Eve", "Frank"],  # Null value
        "email": [
            "alice@example.com",
            "bob@example.com",
            "invalid-email",  # Invalid pattern
            "david@example.com",
            "eve@example.com",
            "frank@example.com"
        ],
        "age": [25, 30, 35, 150, 28, 40],  # 150 is out of range
        "premium": [1200.50, -100.00, 2000.00, 1800.25, 1350.00, 1450.00],  # Negative premium
        "is_active": [True, True, False, True, True, True]
    }
    df = pd.DataFrame(data)
    
    print("\n1. Sample DataFrame (with data quality issues):")
    print(df)
    
    # Create validator
    print("\n2. Creating Validator with Rules:")
    validator = DataValidator("customers")
    
    # Add validation rules
    validator.add_not_null(
        columns=["customer_id", "customer_name"],
        threshold=0.0,
        severity=ValidationSeverity.ERROR
    )
    
    validator.add_unique(
        columns="customer_id",
        severity=ValidationSeverity.ERROR
    )
    
    validator.add_type_validation(
        column_types={
            "customer_id": "integer",
            "customer_name": "string",
            "age": "integer",
            "is_active": "boolean"
        },
        severity=ValidationSeverity.ERROR
    )
    
    validator.add_range_validation(
        column="age",
        min_value=0,
        max_value=120,
        severity=ValidationSeverity.WARNING
    )
    
    validator.add_range_validation(
        column="premium",
        min_value=0,
        max_value=10000,
        severity=ValidationSeverity.ERROR
    )
    
    validator.add_pattern_validation(
        column="email",
        pattern=r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$',
        severity=ValidationSeverity.WARNING
    )
    
    # Custom rule
    validator.add_custom(
        name="PremiumAgeRatio",
        validation_func=lambda df: (df["premium"] / df["age"]).max() < 100,
        error_message="Premium to age ratio exceeds 100",
        severity=ValidationSeverity.WARNING
    )
    
    print(f"   Added {len(validator.rules)} validation rules")
    
    # Run validation
    print("\n3. Running Validation:")
    report = validator.validate(df)
    
    # Print results
    print("\n4. Validation Report:")
    print(report)
    
    print("\n5. Detailed Results:")
    for result in report.results:
        status = "✅ PASS" if result.passed else "❌ FAIL"
        print(f"   {status} | {result.rule_name}")
        print(f"      {result.message}")
        if result.details:
            print(f"      Details: {result.details}")
    
    print("\n6. Summary:")
    summary = report.summary()
    for key, value in summary.items():
        print(f"   {key}: {value}")
    
    print("\n7. Failed Rules:")
    for result in report.get_failures():
        print(f"   - {result.rule_name}: {result.message}")
    
    print("\n" + "="*60)
    print("Demo complete!")
    print("="*60)

// BASIC DATA QUALITY RULES CATALOGUE
// Simple, fundamental rules for Silver layer transformation

export const RULE_CATEGORIES = {
  // Basic Data Quality Rules (Universal)
  basic_completeness: {
    category: 'Completeness',
    label: '📋 Completeness Rules',
    description: 'Check for missing or null values',
    rules: [
      { 
        id: 'COMP-001', 
        label: 'No NULL Values', 
        description: 'Check for NULL values in critical columns', 
        ruleType: 'Completeness', 
        severity: 'ERROR', 
        default: true,
        pipelineStep: 'validate'
      },
      { 
        id: 'COMP-002', 
        label: 'No Empty Strings', 
        description: 'Check for empty strings in text columns', 
        ruleType: 'Completeness', 
        severity: 'WARNING', 
        default: true,
        pipelineStep: 'clean'
      }
    ]
  },
  
  basic_validity: {
    category: 'Validity',
    label: '✓ Validity Rules',
    description: 'Check if values match expected format',
    rules: [
      { 
        id: 'VAL-001', 
        label: 'Data Type Check', 
        description: 'Verify data types match schema (numeric, date, string)', 
        ruleType: 'Validity', 
        severity: 'ERROR', 
        default: true,
        pipelineStep: 'cast'
      },
      { 
        id: 'VAL-002', 
        label: 'Date Format Check', 
        description: 'Ensure dates are valid and not in the future', 
        ruleType: 'Validity', 
        severity: 'ERROR', 
        default: true,
        pipelineStep: 'validate'
      },
      { 
        id: 'VAL-003', 
        label: 'Positive Numbers', 
        description: 'Verify numeric values are positive where required', 
        ruleType: 'Validity', 
        severity: 'WARNING', 
        default: false,
        pipelineStep: 'validate'
      }
    ]
  },
  
  basic_uniqueness: {
    category: 'Uniqueness',
    label: '🔑 Uniqueness Rules',
    description: 'Check for duplicate records',
    rules: [
      { 
        id: 'UNQ-001', 
        label: 'Primary Key Uniqueness', 
        description: 'Check that primary key columns have no duplicates', 
        ruleType: 'Uniqueness', 
        severity: 'ERROR', 
        default: true,
        pipelineStep: 'validate'
      },
      { 
        id: 'UNQ-002', 
        label: 'Remove Duplicate Rows', 
        description: 'Remove exact duplicate rows based on all columns', 
        ruleType: 'Uniqueness', 
        severity: 'WARNING', 
        default: true,
        pipelineStep: 'dedup'
      }
    ]
  },
  
  basic_formatting: {
    category: 'Formatting',
    label: '✨ Formatting Rules',
    description: 'Standardize data format',
    rules: [
      { 
        id: 'FMT-001', 
        label: 'Trim Whitespace', 
        description: 'Remove leading and trailing whitespace from text columns', 
        ruleType: 'Format', 
        severity: 'WARNING', 
        default: true,
        pipelineStep: 'clean'
      },
      { 
        id: 'FMT-002', 
        label: 'Standardize Case', 
        description: 'Convert text to uppercase or lowercase as needed', 
        ruleType: 'Format', 
        severity: 'WARNING', 
        default: false,
        pipelineStep: 'clean'
      },
      { 
        id: 'FMT-003', 
        label: 'Remove Special Characters', 
        description: 'Clean special characters from text fields', 
        ruleType: 'Format', 
        severity: 'WARNING', 
        default: false,
        pipelineStep: 'clean'
      }
    ]
  },
  
  basic_consistency: {
    category: 'Consistency',
    label: '🔗 Consistency Rules',
    description: 'Check relationships between fields',
    rules: [
      { 
        id: 'CON-001', 
        label: 'Date Range Validation', 
        description: 'Ensure start dates are before end dates', 
        ruleType: 'Consistency', 
        severity: 'ERROR', 
        default: true,
        pipelineStep: 'validate'
      },
      { 
        id: 'CON-002', 
        label: 'Numeric Range Check', 
        description: 'Verify numeric values are within expected ranges', 
        ruleType: 'Consistency', 
        severity: 'WARNING', 
        default: false,
        pipelineStep: 'validate'
      }
    ]
  }
};

// Rule type to emoji mapping
export const RULE_TYPE_ICONS = {
  'Completeness': '📋',
  'Validity': '✓',
  'Uniqueness': '🔑',
  'Format': '✨',
  'Consistency': '🔗'
};

// Severity colors
export const SEVERITY_COLORS = {
  'ERROR': { bg: '#fee2e2', color: '#991b1b', border: '#fca5a5' },
  'WARNING': { bg: '#fef3c7', color: '#92400e', border: '#fde047' },
  'INFO': { bg: '#dbeafe', color: '#1e40af', border: '#93c5fd' }
};

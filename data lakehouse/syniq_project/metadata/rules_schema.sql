-- ============================================================================
-- SyniqAI Lakehouse - Rules Engine Database Schema
-- PostgreSQL 14+
-- Production-Ready Rule Catalog & Governance System
-- ============================================================================

-- Extension for UUID generation
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================================================
-- MAIN RULES CATALOG
-- Central repository for all data quality and validation rules
-- ============================================================================

CREATE TABLE IF NOT EXISTS rules_catalog (
    -- Primary Key
    rule_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Basic Information
    rule_name VARCHAR(255) NOT NULL UNIQUE,
    description TEXT,
    
    -- Domain & Classification
    domain VARCHAR(50) NOT NULL CHECK (domain IN ('finance', 'healthcare', 'general')),
    category VARCHAR(50) NOT NULL CHECK (category IN (
        'validation', 'transformation', 'masking', 
        'referential_integrity', 'anomaly_detection', 
        'compliance', 'data_quality', 'schema_validation'
    )),
    
    -- Target Definition
    target_table VARCHAR(255) NOT NULL,
    target_columns TEXT[], -- Array of column names
    
    -- Rule Logic
    rule_type VARCHAR(50) NOT NULL CHECK (rule_type IN (
        'not_null', 'range_check', 'regex_format', 
        'enum_validation', 'unique', 'foreign_key',
        'cross_column_logic', 'cross_table_validation',
        'masking_rule', 'drift_threshold', 'anomaly_detection',
        'contract_validation', 'sql_expression', 'data_type_check'
    )),
    condition_expression TEXT NOT NULL, -- SQL, JSON, or Python expression
    condition_type VARCHAR(20) CHECK (condition_type IN ('sql', 'json', 'python')),
    
    -- Severity & Action
    severity VARCHAR(20) NOT NULL CHECK (severity IN ('INFO', 'WARNING', 'HIGH', 'CRITICAL')),
    action VARCHAR(50) NOT NULL CHECK (action IN (
        'log', 'warn', 'quarantine_row', 
        'block_table', 'alert', 'auto_fix'
    )),
    
    -- Lifecycle Management
    status VARCHAR(20) NOT NULL DEFAULT 'draft' CHECK (status IN (
        'draft', 'pending_approval', 'approved', 
        'active', 'deprecated', 'archived'
    )),
    version INT NOT NULL DEFAULT 1,
    is_active BOOLEAN DEFAULT FALSE,
    effective_date DATE,
    expiry_date DATE,
    
    -- Ownership & Audit
    created_by VARCHAR(255) NOT NULL,
    approved_by VARCHAR(255),
    owner_team VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    approved_at TIMESTAMP,
    
    -- Performance & Configuration
    execution_priority INT DEFAULT 5 CHECK (execution_priority BETWEEN 1 AND 10),
    retry_on_failure BOOLEAN DEFAULT FALSE,
    timeout_seconds INT DEFAULT 300,
    
    -- Metadata
    tags TEXT[],
    parent_rule_id UUID REFERENCES rules_catalog(rule_id), -- For rule versioning
    
    -- Constraints
    CONSTRAINT unique_active_rule UNIQUE (rule_name, domain, version),
    CONSTRAINT valid_date_range CHECK (expiry_date IS NULL OR expiry_date > effective_date)
);

-- Indexes for performance
CREATE INDEX idx_rules_domain ON rules_catalog(domain);
CREATE INDEX idx_rules_status ON rules_catalog(status, is_active);
CREATE INDEX idx_rules_table ON rules_catalog(target_table);
CREATE INDEX idx_rules_category ON rules_catalog(category);
CREATE INDEX idx_rules_created_at ON rules_catalog(created_at DESC);

-- ============================================================================
-- RULE EXECUTION LOG
-- Track every rule execution for audit and performance monitoring
-- ============================================================================

CREATE TABLE IF NOT EXISTS rule_execution_log (
    -- Primary Key
    execution_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Rule Reference
    rule_id UUID NOT NULL REFERENCES rules_catalog(rule_id) ON DELETE CASCADE,
    rule_version INT NOT NULL,
    rule_name VARCHAR(255) NOT NULL,
    
    -- Execution Context
    table_name VARCHAR(255) NOT NULL,
    domain VARCHAR(50) NOT NULL,
    batch_id UUID,
    layer VARCHAR(20) CHECK (layer IN ('bronze', 'silver', 'gold')),
    execution_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Results
    total_rows_processed BIGINT,
    rows_passed BIGINT,
    rows_failed BIGINT,
    failure_rate DECIMAL(5,2),
    processing_time_ms INT,
    
    -- Failure Details
    sample_failures JSONB, -- Store first 10-20 failures as JSON
    quarantine_path TEXT,
    error_count INT DEFAULT 0,
    
    -- Status
    execution_status VARCHAR(20) CHECK (execution_status IN (
        'running', 'completed', 'failed', 'cancelled', 'timeout'
    )),
    error_message TEXT,
    
    -- Metadata
    executed_by VARCHAR(255),
    execution_host VARCHAR(255)
);

-- Indexes
CREATE INDEX idx_execution_rule ON rule_execution_log(rule_id);
CREATE INDEX idx_execution_timestamp ON rule_execution_log(execution_timestamp DESC);
CREATE INDEX idx_execution_status ON rule_execution_log(execution_status);
CREATE INDEX idx_execution_batch ON rule_execution_log(batch_id);

-- ============================================================================
-- QUARANTINE RECORDS METADATA
-- Detailed tracking of quarantined data
-- ============================================================================

CREATE TABLE IF NOT EXISTS quarantine_records (
    -- Primary Key
    quarantine_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Source Reference
    source_table VARCHAR(255) NOT NULL,
    source_row_id VARCHAR(255), -- Original row identifier
    domain VARCHAR(50) NOT NULL,
    layer VARCHAR(20) CHECK (layer IN ('bronze', 'silver', 'gold')),
    
    -- Rule Failure Details
    failed_rule_id UUID NOT NULL REFERENCES rules_catalog(rule_id),
    failed_rule_name VARCHAR(255) NOT NULL,
    failure_reason TEXT NOT NULL,
    severity VARCHAR(20) NOT NULL,
    
    -- Record Data
    quarantined_data JSONB, -- Store actual failed row (be mindful of size)
    quarantine_path TEXT, -- File path in storage (MinIO/S3)
    column_name VARCHAR(255), -- Specific column that failed (if applicable)
    failed_value TEXT, -- The specific value that caused failure
    
    -- Lifecycle
    quarantined_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    reviewed_at TIMESTAMP,
    reviewed_by VARCHAR(255),
    resolution_status VARCHAR(20) DEFAULT 'pending' CHECK (resolution_status IN (
        'pending', 'fixed', 'accepted', 'rejected', 'reprocessed', 'ignored'
    )),
    resolution_notes TEXT,
    reprocessed_at TIMESTAMP,
    
    -- Analytics
    is_recurring BOOLEAN DEFAULT FALSE,
    occurrence_count INT DEFAULT 1,
    first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Indexes
CREATE INDEX idx_quarantine_table ON quarantine_records(source_table);
CREATE INDEX idx_quarantine_status ON quarantine_records(resolution_status);
CREATE INDEX idx_quarantine_rule ON quarantine_records(failed_rule_id);
CREATE INDEX idx_quarantine_date ON quarantine_records(quarantined_at DESC);
CREATE INDEX idx_quarantine_domain ON quarantine_records(domain);

-- ============================================================================
-- RULE APPROVAL WORKFLOW
-- Multi-level approval system for rule changes
-- ============================================================================

CREATE TABLE IF NOT EXISTS rule_approval_workflow (
    -- Primary Key
    approval_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Rule Reference
    rule_id UUID NOT NULL REFERENCES rules_catalog(rule_id) ON DELETE CASCADE,
    rule_version INT NOT NULL,
    
    -- Workflow
    submitted_by VARCHAR(255) NOT NULL,
    submitted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    approval_status VARCHAR(20) DEFAULT 'pending' CHECK (approval_status IN (
        'pending', 'approved', 'rejected', 'changes_requested', 'withdrawn'
    )),
    
    -- Approvers
    required_approvers TEXT[], -- List of roles: ['data_governance_lead', 'domain_owner', 'compliance_officer']
    approved_by TEXT[], -- List of actual approvers
    rejected_by VARCHAR(255),
    
    -- Comments
    submission_notes TEXT,
    rejection_reason TEXT,
    change_requests TEXT,
    approved_at TIMESTAMP,
    rejected_at TIMESTAMP,
    
    -- Impact Analysis
    estimated_quarantine_rate DECIMAL(5,2),
    estimated_processing_time_ms INT,
    affected_tables TEXT[],
    affected_row_count BIGINT,
    risk_level VARCHAR(20) CHECK (risk_level IN ('low', 'medium', 'high', 'critical'))
);

-- Indexes
CREATE INDEX idx_approval_rule ON rule_approval_workflow(rule_id);
CREATE INDEX idx_approval_status ON rule_approval_workflow(approval_status);
CREATE INDEX idx_approval_submitted ON rule_approval_workflow(submitted_at DESC);

-- ============================================================================
-- DOMAIN RULE TEMPLATES
-- Pre-configured rule sets for each domain (Finance, Healthcare, General)
-- ============================================================================

CREATE TABLE IF NOT EXISTS domain_rule_templates (
    -- Primary Key
    template_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Template Info
    template_name VARCHAR(255) NOT NULL,
    domain VARCHAR(50) NOT NULL CHECK (domain IN ('finance', 'healthcare', 'general')),
    version VARCHAR(20) NOT NULL,
    
    -- Template Configuration
    default_rules JSONB NOT NULL, -- Array of rule configurations
    description TEXT,
    compliance_standards TEXT[], -- ['PCI-DSS', 'HIPAA', 'SOC2', 'GDPR']
    
    -- Lifecycle
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_by VARCHAR(255),
    
    -- Metadata
    tags TEXT[],
    
    CONSTRAINT unique_template UNIQUE (domain, version)
);

-- Indexes
CREATE INDEX idx_template_domain ON domain_rule_templates(domain);
CREATE INDEX idx_template_active ON domain_rule_templates(is_active);

-- ============================================================================
-- RULE PERFORMANCE METRICS
-- Aggregate performance statistics for rules
-- ============================================================================

CREATE TABLE IF NOT EXISTS rule_performance_metrics (
    -- Primary Key
    metric_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Rule Reference
    rule_id UUID NOT NULL REFERENCES rules_catalog(rule_id) ON DELETE CASCADE,
    
    -- Time Period
    metric_date DATE NOT NULL,
    metric_hour INT CHECK (metric_hour BETWEEN 0 AND 23),
    
    -- Performance Stats
    avg_execution_time_ms INT,
    max_execution_time_ms INT,
    min_execution_time_ms INT,
    total_executions INT,
    total_rows_processed BIGINT,
    
    -- Quality Impact
    avg_failure_rate DECIMAL(5,2),
    total_quarantine_volume BIGINT,
    false_positive_rate DECIMAL(5,2),
    false_negative_rate DECIMAL(5,2),
    
    -- Trends
    trend_direction VARCHAR(10) CHECK (trend_direction IN ('improving', 'degrading', 'stable')),
    
    -- Metadata
    calculated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    UNIQUE (rule_id, metric_date, metric_hour)
);

-- Indexes
CREATE INDEX idx_metrics_rule ON rule_performance_metrics(rule_id);
CREATE INDEX idx_metrics_date ON rule_performance_metrics(metric_date DESC);

-- ============================================================================
-- DATA LINEAGE MAPPING
-- Track column-level transformations across layers
-- ============================================================================

CREATE TABLE IF NOT EXISTS data_lineage (
    -- Primary Key
    lineage_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Source
    source_layer VARCHAR(20) NOT NULL CHECK (source_layer IN ('bronze', 'silver', 'gold')),
    source_table VARCHAR(255) NOT NULL,
    source_column VARCHAR(255),
    
    -- Target
    target_layer VARCHAR(20) NOT NULL CHECK (target_layer IN ('bronze', 'silver', 'gold')),
    target_table VARCHAR(255) NOT NULL,
    target_column VARCHAR(255),
    
    -- Transformation
    transformation_type VARCHAR(50), -- 'cast', 'mask', 'aggregate', 'join', etc.
    transformation_logic TEXT,
    rule_id UUID REFERENCES rules_catalog(rule_id),
    
    -- Domain & Metadata
    domain VARCHAR(50) NOT NULL,
    batch_id UUID,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Validation
    is_valid BOOLEAN DEFAULT TRUE,
    validated_at TIMESTAMP
);

-- Indexes
CREATE INDEX idx_lineage_source ON data_lineage(source_table, source_column);
CREATE INDEX idx_lineage_target ON data_lineage(target_table, target_column);
CREATE INDEX idx_lineage_domain ON data_lineage(domain);

-- ============================================================================
-- AUDIT LOG
-- Comprehensive audit trail for all system actions
-- ============================================================================

CREATE TABLE IF NOT EXISTS audit_log (
    -- Primary Key
    audit_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    
    -- Event Details
    event_type VARCHAR(50) NOT NULL, -- 'rule_created', 'rule_modified', 'rule_executed', etc.
    event_category VARCHAR(50) CHECK (event_category IN (
        'rule_management', 'data_processing', 'access_control', 
        'configuration', 'compliance', 'security'
    )),
    
    -- Actor
    user_id VARCHAR(255) NOT NULL,
    user_role VARCHAR(100),
    ip_address INET,
    
    -- Target
    resource_type VARCHAR(50), -- 'rule', 'table', 'user', etc.
    resource_id VARCHAR(255),
    resource_name VARCHAR(255),
    
    -- Action
    action VARCHAR(50) NOT NULL, -- 'create', 'update', 'delete', 'read', 'execute'
    action_details JSONB,
    
    -- Result
    status VARCHAR(20) CHECK (status IN ('success', 'failure', 'partial')),
    error_message TEXT,
    
    -- Context
    domain VARCHAR(50),
    session_id VARCHAR(255),
    request_id VARCHAR(255),
    
    -- Timestamp
    event_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Compliance
    retention_period_days INT DEFAULT 2555, -- 7 years for financial compliance
    is_sensitive BOOLEAN DEFAULT FALSE
);

-- Indexes
CREATE INDEX idx_audit_timestamp ON audit_log(event_timestamp DESC);
CREATE INDEX idx_audit_user ON audit_log(user_id);
CREATE INDEX idx_audit_type ON audit_log(event_type);
CREATE INDEX idx_audit_domain ON audit_log(domain);

-- ============================================================================
-- VIEWS FOR COMMON QUERIES
-- ============================================================================

-- Active Rules Summary
CREATE OR REPLACE VIEW vw_active_rules AS
SELECT 
    r.rule_id,
    r.rule_name,
    r.domain,
    r.category,
    r.target_table,
    r.severity,
    r.status,
    r.created_at,
    r.created_by,
    r.owner_team,
    COUNT(DISTINCT e.execution_id) as total_executions,
    AVG(e.processing_time_ms) as avg_execution_time_ms,
    AVG(e.failure_rate) as avg_failure_rate
FROM rules_catalog r
LEFT JOIN rule_execution_log e ON r.rule_id = e.rule_id
WHERE r.is_active = TRUE
GROUP BY r.rule_id, r.rule_name, r.domain, r.category, r.target_table, 
         r.severity, r.status, r.created_at, r.created_by, r.owner_team;

-- Quarantine Summary by Table
CREATE OR REPLACE VIEW vw_quarantine_summary AS
SELECT 
    source_table,
    domain,
    COUNT(*) as total_quarantined,
    COUNT(CASE WHEN resolution_status = 'pending' THEN 1 END) as pending,
    COUNT(CASE WHEN resolution_status = 'fixed' THEN 1 END) as fixed,
    COUNT(CASE WHEN resolution_status = 'reprocessed' THEN 1 END) as reprocessed,
    MAX(quarantined_at) as last_quarantine_date
FROM quarantine_records
GROUP BY source_table, domain;

-- Rule Approval Pending
CREATE OR REPLACE VIEW vw_pending_approvals AS
SELECT 
    a.approval_id,
    r.rule_name,
    r.domain,
    r.severity,
    a.submitted_by,
    a.submitted_at,
    a.required_approvers,
    a.risk_level,
    a.estimated_quarantine_rate
FROM rule_approval_workflow a
JOIN rules_catalog r ON a.rule_id = r.rule_id
WHERE a.approval_status = 'pending'
ORDER BY a.submitted_at DESC;

-- ============================================================================
-- FUNCTIONS & TRIGGERS
-- ============================================================================

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Trigger for rules_catalog
CREATE TRIGGER update_rules_catalog_updated_at BEFORE UPDATE ON rules_catalog
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- Trigger for domain_rule_templates
CREATE TRIGGER update_templates_updated_at BEFORE UPDATE ON domain_rule_templates
FOR EACH ROW EXECUTE FUNCTION update_updated_at_column();

-- ============================================================================
-- INITIAL DATA SEEDING (Optional)
-- ============================================================================

-- Insert default domain templates
INSERT INTO domain_rule_templates (template_name, domain, version, default_rules, description, compliance_standards)
VALUES 
    (
        'Finance Standard Rules v1',
        'finance',
        '1.0.0',
        '[
            {"rule_type": "not_null", "columns": ["transaction_id", "customer_id", "amount"]},
            {"rule_type": "range_check", "column": "amount", "min": 0, "max": 1000000},
            {"rule_type": "enum_validation", "column": "currency", "allowed": ["USD", "EUR", "GBP"]},
            {"rule_type": "masking_rule", "columns": ["account_number", "customer_id"], "method": "hash"}
        ]'::JSONB,
        'Standard rule set for financial data - includes AML, KYC, and PCI-DSS compliance checks',
        ARRAY['PCI-DSS', 'SOC2', 'AML']
    ),
    (
        'Healthcare Standard Rules v1',
        'healthcare',
        '1.0.0',
        '[
            {"rule_type": "not_null", "columns": ["patient_id", "admission_date"]},
            {"rule_type": "regex_format", "column": "icd_code", "pattern": "^[A-Z][0-9]{2}(\\\\.[0-9]{1,2})?$"},
            {"rule_type": "masking_rule", "columns": ["patient_id", "ssn", "medical_record_number"], "method": "encrypt"}
        ]'::JSONB,
        'Standard rule set for healthcare data - includes HIPAA compliance and PHI protection',
        ARRAY['HIPAA', 'HL7', 'HITECH']
    ),
    (
        'General Standard Rules v1',
        'general',
        '1.0.0',
        '[
            {"rule_type": "data_type_check", "enforce_types": true},
            {"rule_type": "not_null", "critical_columns_only": true},
            {"rule_type": "unique", "column": "id"}
        ]'::JSONB,
        'Basic rule set for general purpose data quality',
        ARRAY['ISO27001']
    )
ON CONFLICT (domain, version) DO NOTHING;

-- ============================================================================
-- COMMENTS FOR DOCUMENTATION
-- ============================================================================

COMMENT ON TABLE rules_catalog IS 'Central repository for all data quality and validation rules';
COMMENT ON TABLE rule_execution_log IS 'Audit trail of rule executions with performance metrics';
COMMENT ON TABLE quarantine_records IS 'Detailed tracking of quarantined data records';
COMMENT ON TABLE rule_approval_workflow IS 'Multi-level approval workflow for rule changes';
COMMENT ON TABLE domain_rule_templates IS 'Pre-configured rule templates per domain';
COMMENT ON TABLE rule_performance_metrics IS 'Aggregated performance statistics for rule optimization';
COMMENT ON TABLE data_lineage IS 'Column-level data lineage tracking across layers';
COMMENT ON TABLE audit_log IS 'Comprehensive audit trail for compliance and security';

-- ============================================================================
-- END OF SCHEMA
-- ============================================================================

-- Grant permissions (adjust as needed for your environment)
-- GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO syniq_admin;
-- GRANT SELECT ON ALL TABLES IN SCHEMA public TO syniq_reader;

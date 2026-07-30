-- SyniqAI Silver Layer - PostgreSQL Schema
-- Date: February 27, 2026
-- Version: 2.0
-- Description: Complete database schema for Phase 2 Production Hardening

-- =============================================================================
-- APPROVAL WORKFLOW TABLES
-- =============================================================================

-- Main approval requests table
CREATE TABLE IF NOT EXISTS approval_requests (
    id VARCHAR(50) PRIMARY KEY,
    rule_name VARCHAR(255) NOT NULL,
    submitted_by VARCHAR(255) NOT NULL,
    submitted_at TIMESTAMP NOT NULL DEFAULT NOW(),
    status VARCHAR(50) NOT NULL DEFAULT 'PENDING_APPROVAL',
    changes_summary TEXT,
    impact_analysis JSONB,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    CONSTRAINT chk_approval_status CHECK (status IN (
        'PENDING_APPROVAL', 'APPROVED', 'REJECTED', 'CANCELLED'
    ))
);

-- Approval chain (multi-level approvers)
CREATE TABLE IF NOT EXISTS approval_approvers (
    id SERIAL PRIMARY KEY,
    request_id VARCHAR(50) NOT NULL REFERENCES approval_requests(id) ON DELETE CASCADE,
    role VARCHAR(50) NOT NULL,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) NOT NULL,
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING',
    approved_at TIMESTAMP,
    comment TEXT,
    level INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    CONSTRAINT chk_approver_status CHECK (status IN (
        'PENDING', 'APPROVED', 'REJECTED'
    )),
    CONSTRAINT chk_approver_role CHECK (role IN (
        'DATA_GOVERNANCE', 'DOMAIN_OWNER', 'COMPLIANCE_OFFICER', 'SECURITY_OFFICER'
    ))
);

-- Indexes for approval workflow
CREATE INDEX idx_approval_status ON approval_requests(status);
CREATE INDEX idx_approval_submitted_by ON approval_requests(submitted_by);
CREATE INDEX idx_approval_created_at ON approval_requests(created_at DESC);
CREATE INDEX idx_approver_email ON approval_approvers(email);
CREATE INDEX idx_approver_status ON approval_approvers(status);

-- =============================================================================
-- DATA CONTRACT TABLES
-- =============================================================================

-- Main data contracts table
CREATE TABLE IF NOT EXISTS data_contracts (
    table_name VARCHAR(255) PRIMARY KEY,
    version VARCHAR(20) NOT NULL,
    owner VARCHAR(255) NOT NULL,
    schema JSONB NOT NULL,
    sla JSONB NOT NULL,
    effective_date DATE NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    archived BOOLEAN DEFAULT FALSE
);

-- Contract violations tracking
CREATE TABLE IF NOT EXISTS contract_violations (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL REFERENCES data_contracts(table_name),
    column_name VARCHAR(255) NOT NULL,
    constraint_type VARCHAR(50) NOT NULL,
    violation_count INTEGER NOT NULL,
    sample_records JSONB,
    detected_at TIMESTAMP NOT NULL DEFAULT NOW(),
    resolved_at TIMESTAMP,
    status VARCHAR(20) DEFAULT 'OPEN',
    
    CONSTRAINT chk_violation_status CHECK (status IN (
        'OPEN', 'INVESTIGATING', 'RESOLVED', 'IGNORED'
    )),
    CONSTRAINT chk_constraint_type CHECK (constraint_type IN (
        'NOT_NULL', 'UNIQUE', 'PRIMARY_KEY', 'FOREIGN_KEY',
        'RANGE', 'REGEX', 'ENUM', 'CHECK'
    ))
);

-- Contract version history
CREATE TABLE IF NOT EXISTS contract_versions (
    id SERIAL PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    version VARCHAR(20) NOT NULL,
    schema JSONB NOT NULL,
    sla JSONB NOT NULL,
    effective_date DATE NOT NULL,
    created_by VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    archived_at TIMESTAMP,
    
    UNIQUE(table_name, version)
);

-- Indexes for data contracts
CREATE INDEX idx_contract_owner ON data_contracts(owner);
CREATE INDEX idx_contract_effective_date ON data_contracts(effective_date);
CREATE INDEX idx_violation_status ON contract_violations(status);
CREATE INDEX idx_violation_detected_at ON contract_violations(detected_at DESC);
CREATE INDEX idx_violation_table ON contract_violations(table_name);

-- =============================================================================
-- PERFORMANCE METRICS TABLES
-- =============================================================================

-- Rule performance tracking
CREATE TABLE IF NOT EXISTS rule_performance (
    rule_name VARCHAR(255) PRIMARY KEY,
    avg_time DECIMAL(10,2) NOT NULL,
    min_time DECIMAL(10,2) NOT NULL,
    max_time DECIMAL(10,2) NOT NULL,
    std_dev_time DECIMAL(10,2),
    cpu_percent INTEGER NOT NULL,
    memory_mb INTEGER NOT NULL,
    runs INTEGER NOT NULL DEFAULT 0,
    failures INTEGER NOT NULL DEFAULT 0,
    last_run_at TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    CONSTRAINT chk_positive_times CHECK (
        avg_time >= 0 AND min_time >= 0 AND max_time >= 0
    ),
    CONSTRAINT chk_positive_resources CHECK (
        cpu_percent >= 0 AND memory_mb >= 0
    )
);

-- Performance history (time-series data)
CREATE TABLE IF NOT EXISTS rule_performance_history (
    id SERIAL PRIMARY KEY,
    rule_name VARCHAR(255) NOT NULL,
    execution_time DECIMAL(10,2) NOT NULL,
    cpu_percent INTEGER NOT NULL,
    memory_mb INTEGER NOT NULL,
    rows_processed BIGINT,
    success BOOLEAN NOT NULL DEFAULT TRUE,
    error_message TEXT,
    executed_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    FOREIGN KEY (rule_name) REFERENCES rule_performance(rule_name)
);

-- Optimization suggestions
CREATE TABLE IF NOT EXISTS optimization_suggestions (
    id SERIAL PRIMARY KEY,
    rule_name VARCHAR(255) NOT NULL REFERENCES rule_performance(rule_name),
    suggestion TEXT NOT NULL,
    estimated_speedup VARCHAR(10) NOT NULL,
    priority VARCHAR(20) NOT NULL DEFAULT 'MEDIUM',
    implemented BOOLEAN DEFAULT FALSE,
    implemented_at TIMESTAMP,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    CONSTRAINT chk_priority CHECK (priority IN (
        'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'
    ))
);

-- Indexes for performance metrics
CREATE INDEX idx_rule_perf_avg_time ON rule_performance(avg_time DESC);
CREATE INDEX idx_rule_perf_failures ON rule_performance(failures DESC);
CREATE INDEX idx_rule_perf_last_run ON rule_performance(last_run_at DESC);
CREATE INDEX idx_perf_history_executed_at ON rule_performance_history(executed_at DESC);
CREATE INDEX idx_perf_history_rule ON rule_performance_history(rule_name);
CREATE INDEX idx_optimization_priority ON optimization_suggestions(priority);
CREATE INDEX idx_optimization_implemented ON optimization_suggestions(implemented);

-- =============================================================================
-- AUDIT LOG TABLES (TAMPER-PROOF)
-- =============================================================================

-- Main audit logs table with hash chain
CREATE TABLE IF NOT EXISTS audit_logs (
    id VARCHAR(50) PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    event VARCHAR(100) NOT NULL,
    user_email VARCHAR(255) NOT NULL,
    ip_address VARCHAR(45),
    details JSONB NOT NULL,
    hash VARCHAR(64) NOT NULL UNIQUE,
    previous_hash VARCHAR(64),
    signature VARCHAR(20) DEFAULT 'verified',
    
    CONSTRAINT fk_previous_hash FOREIGN KEY (previous_hash) 
        REFERENCES audit_logs(hash) ON DELETE RESTRICT,
    CONSTRAINT chk_event_type CHECK (event IN (
        'TRANSFORMATION_EXECUTED',
        'TRANSFORMATION_FAILED',
        'RULE_MODIFIED',
        'RULE_APPROVED',
        'RULE_CREATED',
        'RULE_DELETED',
        'DATA_CONTRACT_VIOLATED',
        'SCHEMA_CHANGED',
        'QUARANTINE_THRESHOLD_EXCEEDED',
        'USER_ACCESS_GRANTED',
        'USER_ACCESS_REVOKED',
        'EXPORT_PERFORMED',
        'ROLLBACK_EXECUTED'
    )),
    CONSTRAINT chk_signature CHECK (signature IN (
        'verified', 'tampered', 'pending_verification'
    ))
);

-- Audit log exports tracking
CREATE TABLE IF NOT EXISTS audit_exports (
    id SERIAL PRIMARY KEY,
    exported_by VARCHAR(255) NOT NULL,
    start_date TIMESTAMP NOT NULL,
    end_date TIMESTAMP NOT NULL,
    format VARCHAR(10) NOT NULL,
    file_path TEXT,
    digital_signature TEXT NOT NULL,
    exported_at TIMESTAMP NOT NULL DEFAULT NOW(),
    
    CONSTRAINT chk_export_format CHECK (format IN ('PDF', 'CSV', 'JSON'))
);

-- Indexes for audit logs
CREATE INDEX idx_audit_timestamp ON audit_logs(timestamp DESC);
CREATE INDEX idx_audit_user ON audit_logs(user_email);
CREATE INDEX idx_audit_event ON audit_logs(event);
CREATE INDEX idx_audit_hash ON audit_logs(hash);
CREATE INDEX idx_audit_ip ON audit_logs(ip_address);

-- Hash chain integrity verification function
CREATE OR REPLACE FUNCTION verify_audit_chain() 
RETURNS TABLE(verified BOOLEAN, total_logs INTEGER, broken_at VARCHAR) AS $$
DECLARE
    log_record RECORD;
    computed_hash VARCHAR(64);
    prev_hash VARCHAR(64) := NULL;
    total INTEGER := 0;
BEGIN
    FOR log_record IN 
        SELECT * FROM audit_logs ORDER BY timestamp ASC 
    LOOP
        total := total + 1;
        
        -- Compute hash: SHA256(prev_hash + timestamp + user + event + details)
        computed_hash := encode(
            sha256(
                (COALESCE(prev_hash, '') || 
                 log_record.timestamp::text || 
                 log_record.user_email || 
                 log_record.event || 
                 log_record.details::text)::bytea
            ), 
            'hex'
        );
        
        -- Check if computed hash matches stored hash
        IF computed_hash != log_record.hash THEN
            RETURN QUERY SELECT FALSE, total, log_record.id;
            RETURN;
        END IF;
        
        prev_hash := log_record.hash;
    END LOOP;
    
    RETURN QUERY SELECT TRUE, total, NULL::VARCHAR;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- TRANSFORMATION BACKLOG TABLES
-- =============================================================================

-- Transformation jobs tracking
CREATE TABLE IF NOT EXISTS transformation_jobs (
    id VARCHAR(50) PRIMARY KEY,
    table_name VARCHAR(255) NOT NULL,
    domain VARCHAR(50) NOT NULL,
    description TEXT,
    status VARCHAR(50) NOT NULL DEFAULT 'pending',
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    duration VARCHAR(20),
    rows_input BIGINT,
    rows_output BIGINT,
    rows_quarantined INTEGER,
    rules_applied INTEGER,
    error_message TEXT,
    execution_mode VARCHAR(20),
    
    CONSTRAINT chk_job_status CHECK (status IN (
        'pending', 'in_progress', 'completed', 'failed', 'cancelled'
    )),
    CONSTRAINT chk_execution_mode CHECK (execution_mode IN (
        'full_refresh', 'incremental', 'merge'
    ))
);

-- Job execution logs
CREATE TABLE IF NOT EXISTS job_execution_logs (
    id SERIAL PRIMARY KEY,
    job_id VARCHAR(50) NOT NULL REFERENCES transformation_jobs(id) ON DELETE CASCADE,
    timestamp TIMESTAMP NOT NULL DEFAULT NOW(),
    level VARCHAR(20) NOT NULL,
    message TEXT NOT NULL,
    
    CONSTRAINT chk_log_level CHECK (level IN (
        'DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'
    ))
);

-- Indexes for transformation backlog
CREATE INDEX idx_jobs_status ON transformation_jobs(status);
CREATE INDEX idx_jobs_domain ON transformation_jobs(domain);
CREATE INDEX idx_jobs_created_at ON transformation_jobs(created_at DESC);
CREATE INDEX idx_jobs_table ON transformation_jobs(table_name);
CREATE INDEX idx_job_logs_job_id ON job_execution_logs(job_id);
CREATE INDEX idx_job_logs_timestamp ON job_execution_logs(timestamp DESC);

-- =============================================================================
-- CUSTOM RULES TABLES
-- =============================================================================

-- Custom transformation rules
CREATE TABLE IF NOT EXISTS custom_rules (
    rule_id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    description TEXT,
    category VARCHAR(50) NOT NULL,
    severity VARCHAR(20) NOT NULL,
    domain VARCHAR(50) NOT NULL,
    created_by VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    sql_logic TEXT,
    python_code TEXT,
    test_data JSONB,
    enabled BOOLEAN DEFAULT FALSE,
    approval_status VARCHAR(50) DEFAULT 'PENDING',
    approved_by VARCHAR(255),
    approved_at TIMESTAMP,
    
    CONSTRAINT chk_rule_category CHECK (category IN (
        'standard', 'finance', 'healthcare', 'retail', 'security'
    )),
    CONSTRAINT chk_rule_severity CHECK (severity IN (
        'INFO', 'WARNING', 'ERROR', 'BLOCK'
    )),
    CONSTRAINT chk_approval_status CHECK (approval_status IN (
        'DRAFT', 'PENDING', 'APPROVED', 'REJECTED', 'ARCHIVED'
    ))
);

-- Rule test results
CREATE TABLE IF NOT EXISTS rule_test_results (
    id SERIAL PRIMARY KEY,
    rule_id VARCHAR(255) NOT NULL REFERENCES custom_rules(rule_id),
    test_data JSONB NOT NULL,
    result JSONB NOT NULL,
    passed BOOLEAN NOT NULL,
    executed_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Rule usage statistics
CREATE TABLE IF NOT EXISTS rule_usage_stats (
    rule_id VARCHAR(255) PRIMARY KEY REFERENCES custom_rules(rule_id),
    times_used INTEGER DEFAULT 0,
    times_triggered INTEGER DEFAULT 0,
    last_used_at TIMESTAMP,
    avg_execution_time DECIMAL(10,2),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW()
);

-- Indexes for custom rules
CREATE INDEX idx_custom_rules_domain ON custom_rules(domain);
CREATE INDEX idx_custom_rules_category ON custom_rules(category);
CREATE INDEX idx_custom_rules_approval_status ON custom_rules(approval_status);
CREATE INDEX idx_custom_rules_enabled ON custom_rules(enabled);
CREATE INDEX idx_custom_rules_created_at ON custom_rules(created_at DESC);
CREATE INDEX idx_rule_test_results_rule ON rule_test_results(rule_id);

-- =============================================================================
-- RULE VERSIONS TABLES
-- =============================================================================

-- Rule version history
CREATE TABLE IF NOT EXISTS rule_versions (
    id SERIAL PRIMARY KEY,
    rule_id VARCHAR(255) NOT NULL,
    version VARCHAR(20) NOT NULL,
    status VARCHAR(50) NOT NULL DEFAULT 'DRAFT',
    changes TEXT NOT NULL,
    created_by VARCHAR(255) NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    can_rollback BOOLEAN DEFAULT TRUE,
    rule_code TEXT NOT NULL,
    
    UNIQUE(rule_id, version),
    CONSTRAINT chk_version_status CHECK (status IN (
        'DRAFT', 'PENDING_APPROVAL', 'APPROVED', 'ACTIVE', 
        'DEPRECATED', 'ARCHIVED'
    ))
);

-- Indexes for rule versions
CREATE INDEX idx_rule_versions_rule_id ON rule_versions(rule_id);
CREATE INDEX idx_rule_versions_status ON rule_versions(status);
CREATE INDEX idx_rule_versions_created_at ON rule_versions(created_at DESC);

-- =============================================================================
-- USER & PERMISSIONS TABLES
-- =============================================================================

-- Users table
CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    role VARCHAR(50) NOT NULL,
    domain VARCHAR(50),
    active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP NOT NULL DEFAULT NOW(),
    last_login_at TIMESTAMP,
    
    CONSTRAINT chk_user_role CHECK (role IN (
        'DATA_ENGINEER', 'DOMAIN_OWNER', 'DATA_GOVERNANCE',
        'COMPLIANCE_OFFICER', 'SECURITY_OFFICER', 'AUDITOR', 'ADMIN'
    ))
);

-- User permissions
CREATE TABLE IF NOT EXISTS user_permissions (
    id SERIAL PRIMARY KEY,
    user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    permission VARCHAR(100) NOT NULL,
    resource_type VARCHAR(50),
    resource_id VARCHAR(255),
    granted_at TIMESTAMP NOT NULL DEFAULT NOW(),
    granted_by VARCHAR(255),
    
    UNIQUE(user_id, permission, resource_type, resource_id)
);

-- Indexes for users
CREATE INDEX idx_users_email ON users(email);
CREATE INDEX idx_users_role ON users(role);
CREATE INDEX idx_users_active ON users(active);
CREATE INDEX idx_user_permissions_user_id ON user_permissions(user_id);

-- =============================================================================
-- VIEWS FOR REPORTING
-- =============================================================================

-- Active transformations summary
CREATE OR REPLACE VIEW v_active_transformations AS
SELECT 
    tj.domain,
    COUNT(*) FILTER (WHERE tj.status = 'pending') as pending_count,
    COUNT(*) FILTER (WHERE tj.status = 'in_progress') as in_progress_count,
    COUNT(*) FILTER (WHERE tj.status = 'completed') as completed_count,
    COUNT(*) FILTER (WHERE tj.status = 'failed') as failed_count,
    SUM(tj.rows_input) as total_rows_input,
    SUM(tj.rows_quarantined) as total_rows_quarantined,
    AVG(EXTRACT(EPOCH FROM (tj.completed_at - tj.started_at))) as avg_duration_seconds
FROM transformation_jobs tj
WHERE tj.created_at >= NOW() - INTERVAL '24 hours'
GROUP BY tj.domain;

-- Pending approvals summary
CREATE OR REPLACE VIEW v_pending_approvals AS
SELECT 
    ar.id,
    ar.rule_name,
    ar.submitted_by,
    ar.submitted_at,
    COUNT(aa.id) as total_approvers,
    COUNT(aa.id) FILTER (WHERE aa.status = 'APPROVED') as approved_count,
    COUNT(aa.id) FILTER (WHERE aa.status = 'PENDING') as pending_count,
    EXTRACT(EPOCH FROM (NOW() - ar.submitted_at))/3600 as hours_pending
FROM approval_requests ar
JOIN approval_approvers aa ON ar.id = aa.request_id
WHERE ar.status = 'PENDING_APPROVAL'
GROUP BY ar.id, ar.rule_name, ar.submitted_by, ar.submitted_at;

-- Contract violations summary
CREATE OR REPLACE VIEW v_contract_violations AS
SELECT 
    cv.table_name,
    COUNT(*) as total_violations,
    SUM(cv.violation_count) as total_violation_count,
    COUNT(*) FILTER (WHERE cv.status = 'OPEN') as open_violations,
    MAX(cv.detected_at) as latest_violation
FROM contract_violations cv
WHERE cv.status != 'RESOLVED'
GROUP BY cv.table_name;

-- Performance bottlenecks
CREATE OR REPLACE VIEW v_performance_bottlenecks AS
SELECT 
    rp.rule_name,
    rp.avg_time,
    rp.max_time,
    rp.cpu_percent,
    rp.memory_mb,
    rp.failures,
    CASE 
        WHEN rp.avg_time >= 60 THEN 'CRITICAL'
        WHEN rp.avg_time >= 30 THEN 'SLOW'
        WHEN rp.avg_time >= 15 THEN 'ACCEPTABLE'
        WHEN rp.avg_time >= 5 THEN 'GOOD'
        ELSE 'EXCELLENT'
    END as performance_status,
    os.suggestion,
    os.estimated_speedup
FROM rule_performance rp
LEFT JOIN optimization_suggestions os ON rp.rule_name = os.rule_name AND NOT os.implemented
WHERE rp.avg_time >= 5
ORDER BY rp.avg_time DESC;

-- =============================================================================
-- MATERIALIZED VIEWS FOR ANALYTICS
-- =============================================================================

-- Daily transformation metrics (refresh hourly)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_daily_transformation_metrics AS
SELECT 
    DATE(tj.created_at) as date,
    tj.domain,
    COUNT(*) as total_jobs,
    COUNT(*) FILTER (WHERE tj.status = 'completed') as completed_jobs,
    COUNT(*) FILTER (WHERE tj.status = 'failed') as failed_jobs,
    SUM(tj.rows_input) as total_rows_input,
    SUM(tj.rows_output) as total_rows_output,
    SUM(tj.rows_quarantined) as total_rows_quarantined,
    AVG(EXTRACT(EPOCH FROM (tj.completed_at - tj.started_at))) as avg_duration_seconds
FROM transformation_jobs tj
WHERE tj.created_at >= NOW() - INTERVAL '90 days'
GROUP BY DATE(tj.created_at), tj.domain;

CREATE UNIQUE INDEX idx_mv_daily_metrics ON mv_daily_transformation_metrics(date, domain);

-- Refresh materialized view function
CREATE OR REPLACE FUNCTION refresh_materialized_views() RETURNS void AS $$
BEGIN
    REFRESH MATERIALIZED VIEW CONCURRENTLY mv_daily_transformation_metrics;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- TRIGGERS
-- =============================================================================

-- Update timestamp trigger
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

-- Apply timestamp trigger to tables
CREATE TRIGGER trg_approval_requests_updated_at
    BEFORE UPDATE ON approval_requests
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_data_contracts_updated_at
    BEFORE UPDATE ON data_contracts
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

CREATE TRIGGER trg_users_updated_at
    BEFORE UPDATE ON users
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

-- Audit log integrity trigger (prevent updates/deletes)
CREATE OR REPLACE FUNCTION prevent_audit_log_modification()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'Audit logs are immutable. Modification not allowed.';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_prevent_audit_log_update
    BEFORE UPDATE ON audit_logs
    FOR EACH ROW
    EXECUTE FUNCTION prevent_audit_log_modification();

CREATE TRIGGER trg_prevent_audit_log_delete
    BEFORE DELETE ON audit_logs
    FOR EACH ROW
    EXECUTE FUNCTION prevent_audit_log_modification();

-- =============================================================================
-- INITIAL DATA SEEDING
-- =============================================================================

-- Insert default admin user
INSERT INTO users (email, name, role, active) VALUES
('admin@syniq.ai', 'System Administrator', 'ADMIN', TRUE)
ON CONFLICT (email) DO NOTHING;

-- Insert default performance thresholds (for reference)
COMMENT ON TABLE rule_performance IS 'Performance thresholds: EXCELLENT (<5s), GOOD (<15s), ACCEPTABLE (<30s), SLOW (<60s), CRITICAL (>60s)';

-- =============================================================================
-- MAINTENANCE FUNCTIONS
-- =============================================================================

-- Archive old transformation jobs (older than 90 days)
CREATE OR REPLACE FUNCTION archive_old_transformation_jobs() RETURNS INTEGER AS $$
DECLARE
    archived_count INTEGER;
BEGIN
    WITH archived AS (
        DELETE FROM transformation_jobs 
        WHERE status IN ('completed', 'failed') 
        AND created_at < NOW() - INTERVAL '90 days'
        RETURNING *
    )
    SELECT COUNT(*) INTO archived_count FROM archived;
    
    RETURN archived_count;
END;
$$ LANGUAGE plpgsql;

-- Clean up resolved contract violations (older than 30 days)
CREATE OR REPLACE FUNCTION cleanup_resolved_violations() RETURNS INTEGER AS $$
DECLARE
    cleaned_count INTEGER;
BEGIN
    WITH cleaned AS (
        DELETE FROM contract_violations 
        WHERE status = 'RESOLVED' 
        AND resolved_at < NOW() - INTERVAL '30 days'
        RETURNING *
    )
    SELECT COUNT(*) INTO cleaned_count FROM cleaned;
    
    RETURN cleaned_count;
END;
$$ LANGUAGE plpgsql;

-- =============================================================================
-- GRANTS & PERMISSIONS
-- =============================================================================

-- Create read-only role for analysts
CREATE ROLE syniq_analyst;
GRANT SELECT ON ALL TABLES IN SCHEMA public TO syniq_analyst;
GRANT SELECT ON ALL SEQUENCES IN SCHEMA public TO syniq_analyst;

-- Create write role for data engineers
CREATE ROLE syniq_engineer;
GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO syniq_engineer;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO syniq_engineer;
REVOKE UPDATE, DELETE ON audit_logs FROM syniq_engineer;

-- Create admin role
CREATE ROLE syniq_admin;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO syniq_admin;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO syniq_admin;

-- =============================================================================
-- END OF SCHEMA
-- =============================================================================

-- Verify installation
SELECT 
    'Schema installation complete!' as status,
    COUNT(*) as total_tables
FROM information_schema.tables 
WHERE table_schema = 'public' 
AND table_type = 'BASE TABLE';

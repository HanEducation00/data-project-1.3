-- ============================================================================
-- MODEL REGISTRY TABLES
-- Dynamic model management for seasonal ML models
-- ============================================================================

-- Model Registry Table
CREATE TABLE IF NOT EXISTS active_models (
    id SERIAL PRIMARY KEY,
    model_type VARCHAR(50) NOT NULL,  -- 'general', 'winter', 'spring', 'summer', 'fall'
    model_name VARCHAR(255) NOT NULL,
    model_version INTEGER NOT NULL,
    mlflow_model_uri TEXT NOT NULL,
    is_active BOOLEAN DEFAULT false,
    accuracy DECIMAL(5,4),
    rmse DECIMAL(10,4),
    training_records INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    activated_at TIMESTAMP,
    deactivated_at TIMESTAMP,
    created_by VARCHAR(100),
    description TEXT
);

-- Model Performance History Table
CREATE TABLE IF NOT EXISTS model_performance_history (
    id SERIAL PRIMARY KEY,
    model_type VARCHAR(50) NOT NULL,
    model_name VARCHAR(255) NOT NULL,
    model_version INTEGER,
    date DATE NOT NULL,
    accuracy DECIMAL(5,4),
    prediction_count INTEGER,
    avg_error DECIMAL(5,4),
    rmse DECIMAL(10,4),
    max_error DECIMAL(5,4),
    min_error DECIMAL(5,4),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Model Deployment Log Table
CREATE TABLE IF NOT EXISTS model_deployment_log (
    id SERIAL PRIMARY KEY,
    model_type VARCHAR(50) NOT NULL,
    old_model_name VARCHAR(255),
    new_model_name VARCHAR(255) NOT NULL,
    deployment_type VARCHAR(50), -- 'initial', 'upgrade', 'rollback', 'seasonal'
    deployed_by VARCHAR(100),
    deployment_reason TEXT,
    deployed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    rollback_model_id INTEGER REFERENCES active_models(id)
);

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- Active Models Indexes
CREATE INDEX IF NOT EXISTS idx_active_models_type_active 
    ON active_models(model_type, is_active);
CREATE INDEX IF NOT EXISTS idx_active_models_created 
    ON active_models(created_at);
CREATE INDEX IF NOT EXISTS idx_active_models_name_version 
    ON active_models(model_name, model_version);

-- Performance History Indexes  
CREATE INDEX IF NOT EXISTS idx_performance_history_date 
    ON model_performance_history(date);
CREATE INDEX IF NOT EXISTS idx_performance_history_model_date 
    ON model_performance_history(model_type, date);

-- Deployment Log Indexes
CREATE INDEX IF NOT EXISTS idx_deployment_log_date 
    ON model_deployment_log(deployed_at);
CREATE INDEX IF NOT EXISTS idx_deployment_log_type 
    ON model_deployment_log(model_type);

-- ============================================================================
-- CONSTRAINTS
-- ============================================================================

-- Unique constraint: One active model per type
CREATE UNIQUE INDEX IF NOT EXISTS unique_active_model_per_type 
    ON active_models(model_type) 
    WHERE is_active = true;

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE active_models IS 'Registry of all ML models with version control and activation status';
COMMENT ON TABLE model_performance_history IS 'Daily performance metrics for all models';
COMMENT ON TABLE model_deployment_log IS 'Audit trail of model deployments and rollbacks';

COMMENT ON COLUMN active_models.model_type IS 'Type: general, winter, spring, summer, fall';
COMMENT ON COLUMN active_models.is_active IS 'Only one model per type can be active';
COMMENT ON COLUMN model_performance_history.accuracy IS 'Daily accuracy percentage (0-1)';
COMMENT ON COLUMN model_deployment_log.deployment_type IS 'Type: initial, upgrade, rollback, seasonal';

-- ============================================================================
-- SUCCESS MESSAGE
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '✅ MODEL REGISTRY TABLES CREATED SUCCESSFULLY!';
    RAISE NOTICE '📊 Tables: active_models, model_performance_history, model_deployment_log';
    RAISE NOTICE '🔧 Indexes and constraints applied';
END $$;

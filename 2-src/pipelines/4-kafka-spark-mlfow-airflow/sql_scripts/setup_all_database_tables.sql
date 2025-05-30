-- ============================================================================
-- COMPLETE DATABASE SETUP SCRIPT
-- Run this script to set up all tables, data, and configurations
-- ============================================================================

\echo '🚀 STARTING COMPLETE DATABASE SETUP...'
\echo '============================================================================'

-- ============================================================================
-- 1. CREATE MODEL REGISTRY TABLES
-- ============================================================================

\echo '1️⃣ Creating Model Registry Tables...'
\i create_model_registry_tables.sql

-- ============================================================================
-- 2. CREATE ENHANCED PREDICTION TABLES  
-- ============================================================================

\echo '2️⃣ Creating Enhanced Prediction Tables...'
\i create_enhanced_prediction_tables.sql

-- ============================================================================
-- 3. INSERT INITIAL DATA
-- ============================================================================

\echo '3️⃣ Inserting Initial Data...'

-- Insert the existing general model
INSERT INTO active_models (
    model_type, 
    model_name, 
    model_version, 
    mlflow_model_uri, 
    is_active, 
    accuracy, 
    rmse,
    training_records,
    activated_at, 
    created_by,
    description
) VALUES (
    'general', 
    'Ultimate_6M_Records_20250528_162124', 
    1, 
    'models:/Ultimate_6M_Records_20250528_162124/1', 
    true, 
    0.9873,
    15.2,
    6340608,
    CURRENT_TIMESTAMP, 
    'initial_setup',
    'Initial general model trained on 6.3M records with 98.73% accuracy'
) ON CONFLICT DO NOTHING;

-- Insert initial deployment log
INSERT INTO model_deployment_log (
    model_type,
    new_model_name,
    deployment_type,
    deployed_by,
    deployment_reason
) VALUES (
    'general',
    'Ultimate_6M_Records_20250528_162124',
    'initial',
    'initial_setup',
    'Initial deployment of the general energy forecasting model'
) ON CONFLICT DO NOTHING;

-- ============================================================================
-- 4. CREATE UTILITY FUNCTIONS
-- ============================================================================

\echo '4️⃣ Creating Utility Functions...'

-- Function to get active model for a season
CREATE OR REPLACE FUNCTION get_active_seasonal_model(season_type VARCHAR(50))
RETURNS TABLE(
    model_name VARCHAR(255),
    model_version INTEGER,
    mlflow_uri TEXT,
    accuracy DECIMAL(5,4)
) AS $$
BEGIN
    RETURN QUERY
    SELECT 
        am.model_name,
        am.model_version,
        am.mlflow_model_uri,
        am.accuracy
    FROM active_models am
    WHERE am.model_type = season_type 
      AND am.is_active = true
    LIMIT 1;
END;
$$ LANGUAGE plpgsql;

-- Function to deactivate old model and activate new one
CREATE OR REPLACE FUNCTION deploy_new_model(
    p_model_type VARCHAR(50),
    p_model_name VARCHAR(255),
    p_model_version INTEGER,
    p_mlflow_uri TEXT,
    p_accuracy DECIMAL(5,4),
    p_deployed_by VARCHAR(100),
    p_reason TEXT DEFAULT 'Model upgrade'
)
RETURNS BOOLEAN AS $$
DECLARE
    old_model_name VARCHAR(255);
BEGIN
    -- Get current active model
    SELECT model_name INTO old_model_name
    FROM active_models
    WHERE model_type = p_model_type AND is_active = true;
    
    -- Deactivate old model
    UPDATE active_models 
    SET is_active = false, deactivated_at = CURRENT_TIMESTAMP
    WHERE model_type = p_model_type AND is_active = true;
    
    -- Insert new model as active
    INSERT INTO active_models (
        model_type, model_name, model_version, mlflow_model_uri,
        is_active, accuracy, activated_at, created_by
    ) VALUES (
        p_model_type, p_model_name, p_model_version, p_mlflow_uri,
        true, p_accuracy, CURRENT_TIMESTAMP, p_deployed_by
    );
    
    -- Log deployment
    INSERT INTO model_deployment_log (
        model_type, old_model_name, new_model_name,
        deployment_type, deployed_by, deployment_reason
    ) VALUES (
        p_model_type, old_model_name, p_model_name,
        'upgrade', p_deployed_by, p_reason
    );
    
    RETURN true;
EXCEPTION
    WHEN OTHERS THEN
        RETURN false;
END;
$$ LANGUAGE plpgsql;

-- Function to log model alerts
CREATE OR REPLACE FUNCTION log_model_alert(
    p_alert_type VARCHAR(50),
    p_severity VARCHAR(20),
    p_model_type VARCHAR(50),
    p_model_name VARCHAR(255),
    p_message TEXT,
    p_alert_value DECIMAL(10,4) DEFAULT NULL,
    p_threshold_value DECIMAL(10,4) DEFAULT NULL
)
RETURNS INTEGER AS $$
DECLARE
    alert_id INTEGER;
BEGIN
    INSERT INTO model_alert_log (
        alert_type, severity, model_type, model_name,
        message, alert_value, threshold_value
    ) VALUES (
        p_alert_type, p_severity, p_model_type, p_model_name,
        p_message, p_alert_value, p_threshold_value
    ) RETURNING id INTO alert_id;
    
    RETURN alert_id;
END;
$$ LANGUAGE plpgsql;

-- ============================================================================
-- 5. CREATE MONITORING VIEWS
-- ============================================================================

\echo '5️⃣ Creating Monitoring Views...'

-- System Health Dashboard
CREATE OR REPLACE VIEW system_health_dashboard AS
SELECT 
    'Active Models' as metric,
    COUNT(*)::TEXT as value,
    'models' as unit
FROM active_models WHERE is_active = true
UNION ALL
SELECT 
    'Predictions Today' as metric,
    COUNT(*)::TEXT as value,
    'predictions' as unit
FROM enhanced_daily_predictions 
WHERE DATE(prediction_time) = CURRENT_DATE
UNION ALL
SELECT 
    'Open Alerts' as metric,
    COUNT(*)::TEXT as value,
    'alerts' as unit
FROM model_alert_log 
WHERE resolved = false
UNION ALL
SELECT 
    'Avg Accuracy Today' as metric,
    ROUND(AVG(100 - ABS(error_percentage)), 2)::TEXT as value,
    '%' as unit
FROM enhanced_daily_predictions 
WHERE DATE(prediction_time) = CURRENT_DATE;

-- ============================================================================
-- 6. PERMISSIONS (if needed)
-- ============================================================================

\echo '6️⃣ Setting up permissions...'

-- Grant permissions to datauser
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO datauser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO datauser;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA public TO datauser;

-- ============================================================================
-- 7. FINAL VERIFICATION
-- ============================================================================

\echo '7️⃣ Verifying setup...'

-- Check tables exist
DO $$
DECLARE
    table_count INTEGER;
BEGIN
    SELECT COUNT(*) INTO table_count
    FROM information_schema.tables 
    WHERE table_schema = 'public' 
      AND table_name IN (
          'active_models', 
          'model_performance_history',
          'model_deployment_log',
          'enhanced_daily_predictions',
          'model_alert_log',
          'daily_prediction_summary'
      );
    
    IF table_count = 6 THEN
        RAISE NOTICE '✅ All 6 tables created successfully!';
    ELSE
        RAISE NOTICE '❌ Only % out of 6 tables created!', table_count;
    END IF;
END $$;

-- Show current active models
\echo 'Current Active Models:'
SELECT 
    model_type,
    model_name,
    model_version,
    accuracy,
    activated_at
FROM active_models 
WHERE is_active = true
ORDER BY model_type;

-- ============================================================================
-- SUCCESS MESSAGE
-- ============================================================================

\echo '============================================================================'
\echo '🎉 DATABASE SETUP COMPLETED SUCCESSFULLY!'
\echo '📊 Model Registry: Ready'
\echo '📈 Enhanced Predictions: Ready'  
\echo '🚨 Alert System: Ready'
\echo '📋 Monitoring Views: Ready'
\echo '🔧 Utility Functions: Ready'
\echo '============================================================================'
\echo '🚀 System is ready for enhanced ML pipeline!'

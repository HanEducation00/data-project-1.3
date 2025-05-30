-- ============================================================================
-- ENHANCED PREDICTION TABLES
-- Real-time prediction storage with model tracking
-- ============================================================================

-- Enhanced Daily Predictions Table (with model tracking)
CREATE TABLE IF NOT EXISTS enhanced_daily_predictions (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(255) NOT NULL,
    date TIMESTAMP NOT NULL,
    daily_energy DOUBLE PRECISION NOT NULL,
    predicted_daily_energy DOUBLE PRECISION NOT NULL,
    error_percentage DOUBLE PRECISION,
    absolute_error DOUBLE PRECISION,
    prediction_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    batch_id INTEGER,
    model_type VARCHAR(50) NOT NULL,      -- 'winter', 'spring', 'summer', 'fall', 'general'
    model_name VARCHAR(255) NOT NULL,     -- MLflow model name
    model_version INTEGER NOT NULL,       -- MLflow model version
    confidence_score DECIMAL(3,2),        -- Model confidence (if available)
    processing_time_ms INTEGER            -- Prediction processing time
);

-- Model Alert Log Table
CREATE TABLE IF NOT EXISTS model_alert_log (
    id SERIAL PRIMARY KEY,
    alert_type VARCHAR(50) NOT NULL,      -- 'accuracy_drop', 'freshness', 'error_spike', 'model_failure'
    severity VARCHAR(20) NOT NULL,        -- 'LOW', 'MEDIUM', 'HIGH', 'CRITICAL'
    model_type VARCHAR(50),
    model_name VARCHAR(255),
    message TEXT NOT NULL,
    alert_value DECIMAL(10,4),            -- The value that triggered alert (accuracy, hours, etc.)
    threshold_value DECIMAL(10,4),        -- The threshold that was violated
    resolved BOOLEAN DEFAULT false,
    resolved_at TIMESTAMP,
    resolved_by VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    email_sent BOOLEAN DEFAULT false,
    email_sent_at TIMESTAMP
);

-- Daily Prediction Summary Table (for quick analytics)
CREATE TABLE IF NOT EXISTS daily_prediction_summary (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    model_type VARCHAR(50) NOT NULL,
    model_name VARCHAR(255) NOT NULL,
    total_predictions INTEGER NOT NULL,
    avg_accuracy DECIMAL(5,4),
    avg_error DECIMAL(5,4),
    max_error DECIMAL(5,4),
    min_error DECIMAL(5,4),
    total_energy_actual DOUBLE PRECISION,
    total_energy_predicted DOUBLE PRECISION,
    avg_processing_time_ms DECIMAL(8,2),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- Enhanced Predictions Indexes
CREATE INDEX IF NOT EXISTS idx_enhanced_predictions_date 
    ON enhanced_daily_predictions(date);
CREATE INDEX IF NOT EXISTS idx_enhanced_predictions_customer 
    ON enhanced_daily_predictions(customer_id);
CREATE INDEX IF NOT EXISTS idx_enhanced_predictions_model 
    ON enhanced_daily_predictions(model_type, model_name);
CREATE INDEX IF NOT EXISTS idx_enhanced_predictions_batch 
    ON enhanced_daily_predictions(batch_id);
CREATE INDEX IF NOT EXISTS idx_enhanced_predictions_prediction_time 
    ON enhanced_daily_predictions(prediction_time);

-- Composite index for analytics
CREATE INDEX IF NOT EXISTS idx_enhanced_predictions_analytics 
    ON enhanced_daily_predictions(date, model_type, customer_id);

-- Alert Log Indexes
CREATE INDEX IF NOT EXISTS idx_alert_log_created 
    ON model_alert_log(created_at);
CREATE INDEX IF NOT EXISTS idx_alert_log_type_severity 
    ON model_alert_log(alert_type, severity);
CREATE INDEX IF NOT EXISTS idx_alert_log_resolved 
    ON model_alert_log(resolved);

-- Summary Indexes
CREATE INDEX IF NOT EXISTS idx_summary_date_model 
    ON daily_prediction_summary(date, model_type);

-- ============================================================================
-- CONSTRAINTS
-- ============================================================================

-- Ensure positive values
ALTER TABLE enhanced_daily_predictions 
    ADD CONSTRAINT chk_positive_energy 
    CHECK (daily_energy >= 0 AND predicted_daily_energy >= 0);

-- Ensure valid error percentage
ALTER TABLE enhanced_daily_predictions 
    ADD CONSTRAINT chk_valid_error_percentage 
    CHECK (error_percentage >= -100 AND error_percentage <= 1000);

-- Ensure valid severity levels
ALTER TABLE model_alert_log 
    ADD CONSTRAINT chk_valid_severity 
    CHECK (severity IN ('LOW', 'MEDIUM', 'HIGH', 'CRITICAL'));

-- Unique constraint for daily summary
ALTER TABLE daily_prediction_summary 
    ADD CONSTRAINT unique_daily_summary 
    UNIQUE (date, model_type, model_name);

-- ============================================================================
-- TRIGGERS FOR AUTO-CALCULATIONS
-- ============================================================================

-- Auto-calculate absolute error
CREATE OR REPLACE FUNCTION calculate_absolute_error()
RETURNS TRIGGER AS $$
BEGIN
    NEW.absolute_error := ABS(NEW.predicted_daily_energy - NEW.daily_energy);
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trigger_calculate_absolute_error
    BEFORE INSERT OR UPDATE ON enhanced_daily_predictions
    FOR EACH ROW
    EXECUTE FUNCTION calculate_absolute_error();

-- ============================================================================
-- VIEWS FOR QUICK ANALYTICS
-- ============================================================================

-- Latest Predictions View
CREATE OR REPLACE VIEW latest_predictions AS
SELECT 
    customer_id,
    date,
    daily_energy,
    predicted_daily_energy,
    error_percentage,
    model_type,
    model_name,
    prediction_time
FROM enhanced_daily_predictions
WHERE prediction_time >= CURRENT_DATE
ORDER BY prediction_time DESC;

-- Model Performance View
CREATE OR REPLACE VIEW model_performance_today AS
SELECT 
    model_type,
    model_name,
    COUNT(*) as prediction_count,
    AVG(error_percentage) as avg_error,
    AVG(ABS(error_percentage)) as avg_absolute_error,
    MIN(error_percentage) as min_error,
    MAX(error_percentage) as max_error,
    STDDEV(error_percentage) as error_stddev
FROM enhanced_daily_predictions
WHERE DATE(prediction_time) = CURRENT_DATE
GROUP BY model_type, model_name;

-- ============================================================================
-- COMMENTS
-- ============================================================================

COMMENT ON TABLE enhanced_daily_predictions IS 'Enhanced daily predictions with full model tracking and performance metrics';
COMMENT ON TABLE model_alert_log IS 'Log of all model-related alerts and their resolution status';
COMMENT ON TABLE daily_prediction_summary IS 'Daily aggregated prediction metrics for quick analytics';

COMMENT ON COLUMN enhanced_daily_predictions.error_percentage IS 'Prediction error as percentage: (predicted - actual) / actual * 100';
COMMENT ON COLUMN enhanced_daily_predictions.model_type IS 'Seasonal model type used for this prediction';
COMMENT ON COLUMN model_alert_log.alert_type IS 'Type of alert: accuracy_drop, freshness, error_spike, model_failure';

-- ============================================================================
-- SUCCESS MESSAGE
-- ============================================================================

DO $$
BEGIN
    RAISE NOTICE '✅ ENHANCED PREDICTION TABLES CREATED SUCCESSFULLY!';
    RAISE NOTICE '📊 Tables: enhanced_daily_predictions, model_alert_log, daily_prediction_summary';
    RAISE NOTICE '🔧 Indexes, constraints, triggers, and views created';
    RAISE NOTICE '📈 Views: latest_predictions, model_performance_today';
END $$;

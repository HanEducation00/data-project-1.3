-- ============================================================================
-- UPSERT DAILY PREDICTIONS
-- Insert or update daily predictions with conflict resolution
-- Parameters: {temp_table}, {model_type}, {model_name}, {model_version}
-- ============================================================================

INSERT INTO enhanced_daily_predictions (
    customer_id,
    date, 
    daily_energy, 
    predicted_daily_energy,
    error_percentage,
    absolute_error,
    prediction_time,
    batch_id,
    model_type,
    model_name,
    model_version,
    confidence_score,
    processing_time_ms
)
SELECT 
    'DAILY_TOTAL' as customer_id,
    date,
    actual_daily_total as daily_energy,
    predicted_daily_total as predicted_daily_energy,
    CASE 
        WHEN actual_daily_total > 0 THEN 
            ((predicted_daily_total - actual_daily_total) / actual_daily_total * 100)
        ELSE 0 
    END as error_percentage,
    ABS(predicted_daily_total - actual_daily_total) as absolute_error,
    CURRENT_TIMESTAMP as prediction_time,
    {batch_id} as batch_id,
    '{model_type}' as model_type,
    '{model_name}' as model_name,
    {model_version} as model_version,
    0.95 as confidence_score,
    0 as processing_time_ms
FROM {temp_table}

ON CONFLICT (date, model_type, model_name) 
DO UPDATE SET 
    daily_energy = EXCLUDED.daily_energy,
    predicted_daily_energy = EXCLUDED.predicted_daily_energy,
    error_percentage = EXCLUDED.error_percentage,
    absolute_error = EXCLUDED.absolute_error,
    prediction_time = EXCLUDED.prediction_time,
    batch_id = EXCLUDED.batch_id,
    model_version = EXCLUDED.model_version,
    confidence_score = EXCLUDED.confidence_score;

-- Clean up temp table
DROP TABLE IF EXISTS {temp_table};

-- Success message
DO $$
BEGIN
    RAISE NOTICE '✅ DAILY PREDICTIONS UPSERTED SUCCESSFULLY!';
    RAISE NOTICE '🔄 Conflict resolution: UPDATE on duplicate (date, model_type, model_name)';
    RAISE NOTICE '🧹 Temporary table cleaned up';
END $$;

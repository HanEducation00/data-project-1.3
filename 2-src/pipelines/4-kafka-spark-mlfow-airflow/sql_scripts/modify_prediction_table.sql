-- ============================================================================
-- MODIFY PREDICTION TABLE FOR DAILY TOTALS
-- Remove customer_id requirement for daily aggregated predictions
-- ============================================================================

-- Make customer_id nullable since we're doing daily totals
ALTER TABLE enhanced_daily_predictions 
ALTER COLUMN customer_id DROP NOT NULL;

-- Add unique constraint to prevent duplicate daily predictions per model
ALTER TABLE enhanced_daily_predictions 
ADD CONSTRAINT unique_daily_prediction 
UNIQUE (date, model_type, model_name);

-- Add default value for customer_id
UPDATE enhanced_daily_predictions 
SET customer_id = 'DAILY_TOTAL' 
WHERE customer_id IS NULL;

-- Success message
DO $$
BEGIN
    RAISE NOTICE '✅ PREDICTION TABLE MODIFIED FOR DAILY TOTALS!';
    RAISE NOTICE '📊 customer_id is now nullable';
    RAISE NOTICE '🔒 UNIQUE constraint added on (date, model_type, model_name)';
END $$;

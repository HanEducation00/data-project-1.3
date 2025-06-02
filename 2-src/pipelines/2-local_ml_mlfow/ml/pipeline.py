import time
from pyspark.ml import Pipeline
from pyspark.ml.feature import VectorAssembler, StandardScaler
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.evaluation import RegressionEvaluator

def create_ml_pipeline(feature_columns):
    """
    Create ML pipeline with feature scaling and GBT regressor
    """
    print("🤖 CREATING ML PIPELINE...")
    
    # Vector assembler
    assembler = VectorAssembler(
        inputCols=feature_columns,
        outputCol="raw_features"
    )
    
    # Feature scaler
    scaler = StandardScaler(
        inputCol="raw_features",
        outputCol="scaled_features",
        withStd=True,
        withMean=True
    )
    
    # GBT Regressor
    gbt = GBTRegressor(
        featuresCol="scaled_features",
        labelCol="total_daily_energy",
        predictionCol="prediction",
        maxDepth=6,
        maxBins=32,
        maxIter=100,
        stepSize=0.1,
        subsamplingRate=0.8,
        featureSubsetStrategy="sqrt",
        seed=42
    )
    
    # Create pipeline
    pipeline = Pipeline(stages=[assembler, scaler, gbt])
    
    print(f"✅ Pipeline created with {len(feature_columns)} features")
    print(f"🎯 Target: total_daily_energy")
    print(f"🌳 Algorithm: Gradient Boosted Trees")
    
    return pipeline

def train_and_evaluate_model(pipeline, train_df, val_df, test_df):
    """
    Train model and evaluate performance
    """
    print("🚀 TRAINING MODEL...")
    train_start = time.time()
    
    # Train the pipeline
    model = pipeline.fit(train_df)
    
    training_time = time.time() - train_start
    print(f"✅ Training completed in {training_time:.1f}s")
    
    # Make predictions
    print("📊 EVALUATING MODEL...")
    
    train_predictions = model.transform(train_df)
    val_predictions = model.transform(val_df)
    test_predictions = model.transform(test_df)
    
    # Evaluator
    evaluator = RegressionEvaluator(
        labelCol="total_daily_energy",
        predictionCol="prediction"
    )
    
    # Calculate metrics
    def calculate_metrics(predictions_df, dataset_name):
        r2 = evaluator.evaluate(predictions_df, {evaluator.metricName: "r2"})
        rmse = evaluator.evaluate(predictions_df, {evaluator.metricName: "rmse"})
        mae = evaluator.evaluate(predictions_df, {evaluator.metricName: "mae"})
        
        return {
            f"{dataset_name}_r2": r2,
            f"{dataset_name}_rmse": rmse,
            f"{dataset_name}_mae": mae
        }
    
    # Get metrics for all sets
    train_metrics = calculate_metrics(train_predictions, "train")
    val_metrics = calculate_metrics(val_predictions, "val")
    test_metrics = calculate_metrics(test_predictions, "test")
    
    # Combine all metrics
    all_metrics = {**train_metrics, **val_metrics, **test_metrics, "training_time": training_time}
    
    # Print results
    print(f"🎯 MODEL PERFORMANCE:")
    print(f"   📚 Train  R²: {train_metrics['train_r2']:.4f} | RMSE: {train_metrics['train_rmse']:.2f} | MAE: {train_metrics['train_mae']:.2f}")
    print(f"   🔍 Val    R²: {val_metrics['val_r2']:.4f} | RMSE: {val_metrics['val_rmse']:.2f} | MAE: {val_metrics['val_mae']:.2f}")
    print(f"   🧪 Test   R²: {test_metrics['test_r2']:.4f} | RMSE: {test_metrics['test_rmse']:.2f} | MAE: {test_metrics['test_mae']:.2f}")
    
    # Overfitting analysis
    train_test_gap = train_metrics['train_r2'] - test_metrics['test_r2']
    print(f"   🔬 Overfitting Gap: {train_test_gap:.4f}")
    
    if train_test_gap > 0.1:
        print("   ⚠️  Model might be overfitting")
    else:
        print("   ✅ Good generalization")
    
    return model, all_metrics, test_predictions

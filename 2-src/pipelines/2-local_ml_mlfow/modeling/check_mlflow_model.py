import mlflow
import mlflow.spark

# Set tracking URI
mlflow.set_tracking_uri("file:///workspace/mlruns")

# List all experiments
experiments = mlflow.search_experiments()
print("🧪 EXPERIMENTS:")
for exp in experiments:
    print(f"   {exp.name} (ID: {exp.experiment_id})")

# List registered models
try:
    models = mlflow.search_registered_models()
    print("\n🏷️ REGISTERED MODELS:")
    for model in models:
        print(f"   {model.name}")
        for version in model.latest_versions:
            print(f"     Version {version.version}: {version.current_stage}")
except Exception as e:
    print(f"❌ Model registry error: {e}")

# Search runs in Ultimate experiment
try:
    runs = mlflow.search_runs(experiment_names=["Ultimate_6M_Energy_Forecasting"])
    print(f"\n📊 RUNS: {len(runs)} found")
    if len(runs) > 0:
        latest_run = runs.iloc[0]
        print(f"   Latest run: {latest_run['run_id']}")
        print(f"   Test R²: {latest_run.get('metrics.test_r2', 'N/A')}")
except Exception as e:
    print(f"❌ Runs search error: {e}")

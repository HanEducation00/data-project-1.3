import mlflow
mlflow.set_tracking_uri("http://localhost:5000")
client = mlflow.tracking.MlflowClient()

# Tüm experiments
experiments = client.search_experiments()
for exp in experiments:
    print(f"Experiment: {exp.name} (ID: {exp.experiment_id})")

# Tüm registered models  
models = client.search_registered_models()
for model in models:
    print(f"Model: {model.name}")

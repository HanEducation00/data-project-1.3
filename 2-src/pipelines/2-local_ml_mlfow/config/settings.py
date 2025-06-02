import mlflow
from mlflow.tracking import MlflowClient

def setup_mlflow():
    """Setup MLflow tracking"""
    tracking_uri = "http://mlflow-server:5000"
    experiment_name = "System_Total_Energy_Forecasting"
    
    mlflow.set_tracking_uri(tracking_uri)
    
    try:
        experiment_id = mlflow.create_experiment(experiment_name)
        print(f"✅ Created new experiment: {experiment_name}")
    except:
        experiment = mlflow.get_experiment_by_name(experiment_name)
        experiment_id = experiment.experiment_id
        print(f"✅ Using existing experiment: {experiment_name}")
    
    mlflow.set_experiment(experiment_name)
    
    print(f"📁 MLflow tracking URI: {tracking_uri}")
    print(f"🧪 Experiment: {experiment_name}")
    print(f"🆔 Experiment ID: {experiment_id}")
    
    return tracking_uri, experiment_name, experiment_id

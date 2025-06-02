#!/usr/bin/env python3
import requests
import sys
import os

mlflow_host = os.environ.get("MLFLOW_HOST", "localhost")
mlflow_port = os.environ.get("MLFLOW_PORT", "5000")

try:
    response = requests.get(f"http://{mlflow_host}:{mlflow_port}/api/2.0/mlflow/experiments/list")
    if response.status_code == 200:
        sys.exit(0)
    else:
        print(f"MLflow API returned status code {response.status_code}")
        sys.exit(1)
except Exception as e:
    print(f"Error connecting to MLflow: {e}")
    sys.exit(1)

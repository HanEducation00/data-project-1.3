### 1
python3 -c "
try:
    from config.settings import setup_mlflow
    print('✅ config.settings - OK')
except Exception as e:
    print(f'❌ config.settings - ERROR: {e}')
"


### 2
python3 -c "
try:
    from utils.spark_utils import create_spark_session
    print('✅ utils.spark_utils - OK')
except Exception as e:
    print(f'❌ utils.spark_utils - ERROR: {e}')
"

### 3
python3 -c "
try:
    from utils.mlflow_utils import log_to_mlflow
    print('✅ utils.mlflow_utils - OK')
except Exception as e:
    print(f'❌ utils.mlflow_utils - ERROR: {e}')
"

### 4
python3 -c "
try:
    from data.loader import load_smart_meter_data, load_local_files
    print('✅ data.loader - OK')
except Exception as e:
    print(f'❌ data.loader - ERROR: {e}')
"

### 5
python3 -c "
try:
    from data.generator import generate_full_year_data
    print('✅ data.generator - OK')
except Exception as e:
    print(f'❌ data.generator - ERROR: {e}')
"


### 6
python3 -c "
try:
    from processing.aggregation import create_system_level_aggregation, create_daily_total_energy
    print('✅ processing.aggregation - OK')
except Exception as e:
    print(f'❌ processing.aggregation - ERROR: {e}')
"


### 7
python3 -c "
try:
    from processing.features import create_advanced_features, prepare_ml_dataset
    print('✅ processing.features - OK')
except Exception as e:
    print(f'❌ processing.features - ERROR: {e}')
"


### 8
python3 -c "
try:
    from ml.pipeline import create_ml_pipeline, train_and_evaluate_model
    print('✅ ml.pipeline - OK')
except Exception as e:
    print(f'❌ ml.pipeline - ERROR: {e}')
"



### 9
python3 -c "
try:
    from ml.evaluation import get_feature_importance
    print('✅ ml.evaluation - OK')
except Exception as e:
    print(f'❌ ml.evaluation - ERROR: {e}')
"



### 10
python3 -c "
try:
    from system_energy_forecasting import main
    print('✅ Ana dosya import - OK')
    print('🚀 Şimdi main() fonksiyonunu test edebiliriz!')
except Exception as e:
    print(f'❌ Ana dosya import - ERROR: {e}')
"



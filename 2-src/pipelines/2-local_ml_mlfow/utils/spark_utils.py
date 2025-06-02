from pyspark.sql import SparkSession

def create_spark_session():
    """Create optimized Spark session"""
    print("⚡ CREATING SPARK SESSION...")
    
    spark = SparkSession.builder \
        .appName("System_Total_Energy_Forecasting") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "256MB") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.cores", "2") \
        .config("spark.default.parallelism", "8") \
        .master("local[*]") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print(f"✅ Spark session created: {spark.version}")
    return spark

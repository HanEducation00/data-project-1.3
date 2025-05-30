#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Raw Load Data Pipeline Şemaları

Bu dosya ham yük verisi için PySpark ve PostgreSQL şemalarını tanımlar.
"""

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType

# Spark DataFrame Schema
RAW_LOAD_SCHEMA = StructType([
    StructField("customer_id", StringType(), False),
    StructField("profile_type", StringType(), True),
    StructField("day_num", IntegerType(), False),
    StructField("hour", IntegerType(), False),
    StructField("minute", IntegerType(), False),
    StructField("interval_idx", IntegerType(), False),
    StructField("load_percentage", FloatType(), True),
    StructField("full_timestamp", TimestampType(), False)
])

# PostgreSQL Tablo Schema (INDEX'ler kaldırıldı)
POSTGRES_TABLE_SCHEMA = """
CREATE TABLE IF NOT EXISTS raw_load_data (
    id SERIAL PRIMARY KEY,
    customer_id VARCHAR(50) NOT NULL,
    profile_type VARCHAR(50),
    day_num INTEGER NOT NULL,
    hour INTEGER NOT NULL,
    minute INTEGER NOT NULL,
    interval_idx INTEGER NOT NULL,
    load_percentage FLOAT,
    full_timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
"""

# Index'ler ayrı komutlarla
POSTGRES_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_full_timestamp ON raw_load_data (full_timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_customer_timestamp ON raw_load_data (customer_id, full_timestamp)",
    "CREATE INDEX IF NOT EXISTS idx_customer_id ON raw_load_data (customer_id)"
]


# Dosya format bilgisi (header satırı)
FILE_HEADER_FORMAT = "FORMAT=ID,PROFILETYPE,INTERVALFORMAT,TIMEINTERVAL,GLOBALUNIT,NETWORKID,YEAR,MONTH,DAY,UNIT,PHASE,VALUES"

# Veri satırı kolon sayısı (ID + meta veriler + 96 değer)
EXPECTED_COLUMN_COUNT = 11 + 96  # 107 kolon

# Günlük interval sayısı (15 dakikalık periyotlar)
INTERVALS_PER_DAY = 96
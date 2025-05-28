#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Database explorer - MEVCUTdata_loader kullan
"""

import sys
import os

sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'data_processing'))

def explore_database_simple():
    """Mevcut data_loader ile explore et"""
    
    from data_loader import load_electricity_data
    
    print("🔍 Exploring database with existing data_loader...")
    
    # Çok büyük bir limit ile tüm veriyi yüklemeye çalış
    try:
        print("📊 Loading large sample to estimate total size...")
        
        # 1 milyon kayıt limit - database'in max'ini görmek için
        raw_df, loader = load_electricity_data(
            start_date="2010-01-01",  # Çok eski tarih
            end_date="2030-12-31",    # Çok gelecek tarih
            limit=1000000             # 1M limit
        )
        
        total_loaded = raw_df.count()
        print(f"📊 Loaded records: {total_loaded:,}")
        
        if total_loaded == 1000000:
            print("⚠️ Hit limit! Database has MORE than 1M records")
            print("🔍 Let's check date range...")
        
        # Date range check
        from pyspark.sql.functions import min as spark_min, max as spark_max, count, countDistinct
        
        date_stats = raw_df.agg(
            spark_min("full_timestamp").alias("min_date"),
            spark_max("full_timestamp").alias("max_date"),
            count("*").alias("total_records"),
            countDistinct("customer_id").alias("unique_customers"),
            countDistinct("full_timestamp").alias("unique_timestamps")
        ).collect()[0]
        
        min_date = date_stats["min_date"]
        max_date = date_stats["max_date"] 
        total_records = date_stats["total_records"]
        unique_customers = date_stats["unique_customers"]
        unique_timestamps = date_stats["unique_timestamps"]
        
        print(f"\n📅 DATE RANGE:")
        print(f"   🟢 Start: {min_date}")
        print(f"   🔴 End: {max_date}")
        
        print(f"\n📊 STATS:")
        print(f"   📈 Total records: {total_records:,}")
        print(f"   👥 Unique customers: {unique_customers:,}")
        print(f"   ⏰ Unique timestamps: {unique_timestamps:,}")
        
        # Calculate duration
        from datetime import datetime
        if isinstance(min_date, str):
            min_date = datetime.strptime(min_date, '%Y-%m-%d %H:%M:%S')
        if isinstance(max_date, str):
            max_date = datetime.strptime(max_date, '%Y-%m-%d %H:%M:%S')
            
        duration = max_date - min_date
        total_hours = int(duration.total_seconds() / 3600)
        total_days = int(total_hours / 24)
        
        print(f"   📆 Duration: {total_days:,} days ({total_hours:,} hours)")
        print(f"   📈 Avg records/hour: {total_records / total_hours:.1f}")
        print(f"   📈 Avg customers/hour: {total_records / unique_customers / total_hours * unique_customers:.1f}")
        
        # Year breakdown
        print(f"\n📅 SAMPLING YEARS...")
        raw_df.createOrReplaceTempView("raw_data")
        
        year_stats = loader.spark.sql("""
            SELECT 
                YEAR(full_timestamp) as year,
                COUNT(*) as records,
                COUNT(DISTINCT customer_id) as customers
            FROM raw_data 
            GROUP BY YEAR(full_timestamp)
            ORDER BY year
        """).collect()
        
        print("📊 YEAR BREAKDOWN (from sample):")
        for row in year_stats:
            print(f"   📅 {row['year']}: {row['records']:,} records, {row['customers']:,} customers")
        
        # Strategy recommendation
        print(f"\n🎯 STRATEGY RECOMMENDATION:")
        
        if total_hours > 8760:  # 1+ year
            print("✅ EXCELLENT! 1+ year data!")
            print("🚀 FULL DATA TRAINING RECOMMENDED:")
            print(f"   📈 Expected aggregated hours: {total_hours:,}")
            print(f"   📊 Suggested train/val/test split:")
            print(f"      🏋️ Train: {int(total_hours * 0.7):,} hours")
            print(f"      📊 Validation: {int(total_hours * 0.15):,} hours") 
            print(f"      🧪 Test: {int(total_hours * 0.15):,} hours")
            
            print(f"\n🔥 ADVANCED FEATURES TO ADD:")
            print("   • Lag features: [1h, 6h, 24h, 168h]")
            print("   • Rolling features: [24h, 168h, 720h]") 
            print("   • Seasonal: month_sin/cos, hour_sin/cos")
            print("   • Calendar: is_weekend, is_holiday")
            
        elif total_hours > 2160:  # 3+ months
            print("✅ GOOD! 3+ months data")
            print("📊 Can capture weekly patterns")
        else:
            print("⚠️ Limited data")
        
        loader.close()
        
        return {
            'sample_records': total_records,
            'total_hours': total_hours,
            'total_days': total_days,
            'unique_customers': unique_customers,
            'date_range': (min_date, max_date),
            'hit_limit': total_loaded == 1000000
        }
        
    except Exception as e:
        print(f"❌ Exploration failed: {e}")
        return None

def estimate_full_size():
    """Tam boyutu tahmin et"""
    
    print("\n🔍 ESTIMATING FULL DATABASE SIZE...")
    
    # Küçük sample al
    from data_loader import load_electricity_data
    
    raw_df, loader = load_electricity_data(
        start_date="2016-01-01",
        end_date="2016-01-02", 
        limit=10000
    )
    
    sample_count = raw_df.count()
    
    # 1 günlük sample'dan tahmin
    if sample_count > 0:
        print(f"📊 1-day sample: {sample_count:,} records")
        
        # Date range al
        from pyspark.sql.functions import min as spark_min, max as spark_max
        date_range = raw_df.agg(
            spark_min("full_timestamp").alias("min_date"),
            spark_max("full_timestamp").alias("max_date")
        ).collect()[0]
        
        min_date = date_range["min_date"]
        max_date = date_range["max_date"]
        
        from datetime import datetime
        if isinstance(min_date, str):
            min_date = datetime.strptime(min_date, '%Y-%m-%d %H:%M:%S')
        if isinstance(max_date, str):
            max_date = datetime.strptime(max_date, '%Y-%m-%d %H:%M:%S')
            
        sample_hours = (max_date - min_date).total_seconds() / 3600
        records_per_hour = sample_count / sample_hours if sample_hours > 0 else 0
        
        print(f"📊 Sample duration: {sample_hours:.1f} hours")
        print(f"📊 Records per hour: {records_per_hour:.1f}")
        
        # Estimate for different periods
        estimates = {
            "1 week": int(records_per_hour * 24 * 7),
            "1 month": int(records_per_hour * 24 * 30),
            "1 year": int(records_per_hour * 24 * 365)
        }
        
        print(f"\n📊 SIZE ESTIMATES:")
        for period, estimate in estimates.items():
            print(f"   {period}: ~{estimate:,} records")
    
    loader.close()

if __name__ == "__main__":
    print("🧪 DATABASE EXPLORATION")
    
    # İlk büyük sample
    result = explore_database_simple()
    
    if result and result.get('hit_limit'):
        print("\n" + "="*50)
        estimate_full_size()
    
    print("\n🎯 READY FOR FULL DATA TRAINING!")

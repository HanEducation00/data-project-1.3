#!/usr/bin/env python3
"""
SQL Utilities for Enhanced ML Pipeline
Execute SQL scripts with parameter substitution
"""
import os
import psycopg2
import logging

logger = logging.getLogger(__name__)

def execute_sql_script(script_name, **params):
    """Execute SQL script from sql_scripts directory with parameter substitution"""
    
    # Build script path
    current_dir = os.path.dirname(os.path.abspath(__file__))
    script_path = os.path.join(current_dir, "sql_scripts", script_name)
    
    if not os.path.exists(script_path):
        logger.error(f"❌ SQL script not found: {script_path}")
        return False
    
    try:
        # Read SQL script
        with open(script_path, 'r') as file:
            query = file.read()
        
        # Replace parameters
        for key, value in params.items():
            placeholder = f"{{{key}}}"
            query = query.replace(placeholder, str(value))
        
        # Execute SQL
        conn = psycopg2.connect(
            host="postgres",
            database="datawarehouse", 
            user="datauser",
            password="datapass"
        )
        
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"✅ SQL script executed successfully: {script_name}")
        return True
        
    except Exception as e:
        logger.error(f"❌ SQL script execution failed ({script_name}): {e}")
        return False

def setup_database():
    """Setup database tables and constraints"""
    logger.info("🔧 Setting up database...")
    
    # 1. First create tables (IF NOT EXISTS)
    if execute_sql_script("create_enhanced_prediction_tables.sql"):
        logger.info("✅ Tables created/verified")
    else:
        logger.error("❌ Failed to create tables")
        return False
    
    # 2. Then modify the prediction table
    if execute_sql_script("modify_prediction_table.sql"):
        logger.info("✅ Prediction table modified")
    else:
        logger.error("❌ Failed to modify prediction table")
        return False
    
    return True

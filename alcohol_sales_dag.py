from datetime import datetime, timedelta
from pathlib import Path
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

# Import our custom scripts
import sys

sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))
from Real_Time_Data_Processor import simulate_kafka_method, load_real_data
from data_cleaning import clean_data, transform_data, setup_directories
from data_validation import DataValidator

# Default directories
DEFAULT_STAGING_DIR = "/opt/airflow/staging/real_time"
DEFAULT_PROCESSED_DIR = "/opt/airflow/processed/real_time"
DEFAULT_TRANSFORMED_DIR = "/opt/airflow/transformed/real_time"
DEFAULT_SQL_SCRIPTS_DIR = "/opt/airflow/sql_scripts"

# DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

import hashlib
import shutil
from pathlib import Path
from datetime import datetime
import time
import os
import json


def calculate_file_checksum(filepath):
    sha256_hash = hashlib.sha256()
    with open(filepath, "rb") as f:
        # Read the file in chunks to handle large files efficiently
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()


def ingest_data(**kwargs):
    # Input parameters with defaults
    data_file = kwargs.get('data_file', '/opt/airflow/data/Warehouse_and_Retail_Sales.csv')
    staging_dir = kwargs.get('staging_dir', DEFAULT_STAGING_DIR)
    iterations = kwargs.get('iterations', 1)

    # Ensure directories exist
    Path(staging_dir).mkdir(parents=True, exist_ok=True)

    # Original data file path
    original_data_file = '/opt/airflow/data/Warehouse_and_Retail_Sales.csv'

    # Marker directory and files
    processed_marker_dir = '/opt/airflow/processed'
    processed_marker_file = Path(f'{processed_marker_dir}/original_data_processed.json')

    # Check if original data file exists
    if not Path(original_data_file).exists():
        raise FileNotFoundError(f"Original data file not found: {original_data_file}")

    # Calculate the current file's checksum
    current_checksum = calculate_file_checksum(original_data_file)

    # Check if file has been processed before
    processed_info = {}
    if processed_marker_file.exists():
        with open(processed_marker_file, 'r') as f:
            processed_info = json.load(f)

    # Determine if we need to process the file
    needs_processing = (
            not processed_marker_file.exists() or
            processed_info.get('checksum') != current_checksum
    )

    if needs_processing:
        # Create processed marker directory if it doesn't exist
        Path(processed_marker_dir).mkdir(parents=True, exist_ok=True)

        # Create a timestamped filename in the staging directory
        staged_file = f"{staging_dir}/original_data_{int(time.time())}.csv"

        # Ensure the directory for the staged file exists
        os.makedirs(os.path.dirname(staged_file), exist_ok=True)

        # Copy the original data file to the staging directory
        shutil.copy(original_data_file, staged_file)

        print(f"Original data copied to staging area: {staged_file}")

        # Update the processed info
        processed_info = {
            'timestamp': datetime.now().isoformat(),
            'checksum': current_checksum,
            'staged_file': staged_file
        }

        # Save the processed marker with more detailed information
        with open(processed_marker_file, 'w') as f:
            json.dump(processed_info, f)

        # Update the data_file to use the newly staged file
        kwargs['data_file'] = staged_file
        staged_file_to_use = staged_file
    else:
        # If already processed and no changes, use the previous staged file
        print("Original data has already been processed and is unchanged.")
        staged_file_to_use = processed_info.get('staged_file', data_file)

    # Generate and save synthetic data using the staged file
    success = simulate_kafka_method(staging_dir, staged_file_to_use, iterations)
    if not success:
        raise Exception("Data ingestion failed")

    # Return the staging directory for the next task
    return staging_dir


# Function to process CSV files in the staging directory
def process_staged_data(**kwargs):
    import pandas as pd
    import glob

    staging_dir = kwargs.get('staging_dir', DEFAULT_STAGING_DIR)
    processed_dir = kwargs.get('processed_dir', DEFAULT_PROCESSED_DIR)
    transformed_dir = kwargs.get('transformed_dir', DEFAULT_TRANSFORMED_DIR)

    # Ensure directories exist
    for dir_path in [staging_dir, processed_dir, transformed_dir]:
        Path(dir_path).mkdir(parents=True, exist_ok=True)

    # Get all CSV files in staging directory
    csv_files = glob.glob(f"{staging_dir}/*.csv")
    if not csv_files:
        print(f"No CSV files found in {staging_dir}")
        return []

    processed_files = []
    for file_path in csv_files:
        try:
            # Read the CSV file
            df = pd.read_csv(file_path)
            print(f"Processing {file_path}: {len(df)} rows")

            # Clean and transform the data
            cleaned_df = clean_data(df)
            transformed_df = transform_data(cleaned_df)

            # Save transformed data
            file_name = Path(file_path).name
            output_file = Path(transformed_dir) / f"{Path(file_name).stem}_transformed.csv"
            transformed_df.to_csv(output_file, index=False)
            print(f"Saved transformed data to {output_file}")

            # Mark as processed by moving to processed directory
            processed_file = Path(processed_dir) / file_name
            # Save a copy to the processed directory
            transformed_df.to_csv(processed_file, index=False)
            processed_files.append(str(output_file))

        except Exception as e:
            print(f"Error processing {file_path}: {e}")

    return processed_files


# Function to validate transformed data
def validate_data(**kwargs):
    import glob

    transformed_dir = kwargs.get('transformed_dir', DEFAULT_TRANSFORMED_DIR)

    # Get all transformed CSV files
    csv_files = glob.glob(f"{transformed_dir}/*_transformed.csv")
    if not csv_files:
        print(f"No transformed CSV files found in {transformed_dir}")
        return False

    # Get the latest file
    latest_file = max(csv_files, key=os.path.getctime)
    print(f"Validating latest file: {latest_file}")

    # Create validator and run validation
    validator = DataValidator()
    results = validator.validate(latest_file)

    # If validation fails, raise an exception to fail the task
    if results['status'] == "FAIL":
        raise Exception(f"Data validation failed: {results['summary']['failed_checks']} failed checks")

    return results['status'] == "PASS"


# Function to load data into the data warehouse
def load_data_to_warehouse(**kwargs):
    """Load transformed data into the data warehouse"""
    import pandas as pd
    import mysql.connector
    from mysql.connector import Error

    transformed_dir = kwargs.get('transformed_dir', DEFAULT_TRANSFORMED_DIR)
    ti = kwargs['ti']
    processed_files = ti.xcom_pull(task_ids='process_data')

    if not processed_files:
        print("No processed files to load into warehouse")
        return []

    # Database connection parameters
    db_config = {
        'host': Variable.get('mysql_host', 'mysql'),
        'database': Variable.get('mysql_db', 'alcohol_sales_dw'),
        'user': Variable.get('mysql_user', 'airflow'),
        'password': Variable.get('mysql_password', 'airflow')
    }

    loaded_files = []
    for file_path in processed_files:
        try:
            df = pd.read_csv(file_path)
            print(f"Loading {file_path} into warehouse: {len(df)} rows")

            # Connect to the database
            conn = mysql.connector.connect(**db_config)
            if conn.is_connected():
                cursor = conn.cursor()

                # For each row, insert data into dimension tables first, then fact table
                for _, row in df.iterrows():
                    # Insert into dim_product if not exists
                    cursor.execute("""
                        INSERT IGNORE INTO dim_product (product_id, product_name, product_type)
                        VALUES (%s, %s, %s)
                    """, (row['product_id'], row['product_name'], row['product_type']))

                    # Get product_key
                    cursor.execute("""
                        SELECT product_key FROM dim_product WHERE product_id = %s
                    """, (row['product_id'],))
                    product_key = cursor.fetchone()[0]

                    # Insert into dim_supplier if not exists
                    cursor.execute("""
                        INSERT IGNORE INTO dim_supplier (supplier_name)
                        VALUES (%s)
                    """, (row['supplier'],))

                    # Get supplier_key
                    cursor.execute("""
                        SELECT supplier_key FROM dim_supplier WHERE supplier_name = %s
                    """, (row['supplier'],))
                    supplier_key = cursor.fetchone()[0]

                    # Insert into fact_sales
                    cursor.execute("""
                        INSERT IGNORE INTO fact_sales 
                        (transaction_id, product_key, supplier_key, year, month, 
                         retail_sales, retail_transfers, warehouse_sales, total_sales)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
                        row['transaction_id'], product_key, supplier_key,
                        row['year'], row['month'],
                        row['retail_sales'], row['retail_transfers'],
                        row['warehouse_sales'], row['total_sales']
                    ))

                conn.commit()
                loaded_files.append(file_path)
                print(f"Successfully loaded {file_path} into warehouse")

                cursor.close()
                conn.close()

        except Exception as e:
            print(f"Error loading {file_path} into warehouse: {e}")

    return loaded_files


# Create the DAG
with DAG(
        'alcohol_sales_pipeline',
        default_args=default_args,
        description='Alcohol Sales Data Pipeline',
        schedule_interval=timedelta(hours=1),
        start_date=days_ago(1),
        catchup=False,
        tags=['alcohol_sales'],
) as dag:
    # Task to create directories if they don't exist
    init_directories = BashOperator(
        task_id='init_directories',
        bash_command=f'mkdir -p {DEFAULT_STAGING_DIR} {DEFAULT_PROCESSED_DIR} {DEFAULT_TRANSFORMED_DIR} {DEFAULT_SQL_SCRIPTS_DIR}'
    )

    # Task to create the database schema if it doesn't exist
    init_schema = MySqlOperator(
        task_id='init_schema',
        mysql_conn_id='mysql_default',
        sql="""
        -- Create the data warehouse database
        CREATE DATABASE IF NOT EXISTS alcohol_sales_dw;

        -- Use the data warehouse database
        USE alcohol_sales_dw;

        -- Create dimension tables first
        CREATE TABLE IF NOT EXISTS dim_product (
            product_key INT AUTO_INCREMENT PRIMARY KEY,
            product_id INT UNIQUE NOT NULL,
            product_name VARCHAR(100) NOT NULL,
            product_type VARCHAR(50)
        );

        CREATE TABLE IF NOT EXISTS dim_supplier (
            supplier_key INT AUTO_INCREMENT PRIMARY KEY,
            supplier_name VARCHAR(100) UNIQUE NOT NULL
        );

        -- Create Fact table
        CREATE TABLE IF NOT EXISTS fact_sales (
            sales_key INT AUTO_INCREMENT PRIMARY KEY,
            transaction_id VARCHAR(50) UNIQUE NOT NULL,
            product_key INT,
            supplier_key INT,
            year INT NOT NULL,
            month INT NOT NULL,
            retail_sales DECIMAL(10, 2) DEFAULT 0,
            retail_transfers DECIMAL(10, 2) DEFAULT 0,
            warehouse_sales DECIMAL(10, 2) DEFAULT 0,
            total_sales DECIMAL(10, 2) NOT NULL,
            FOREIGN KEY (product_key) REFERENCES dim_product(product_key),
            FOREIGN KEY (supplier_key) REFERENCES dim_supplier(supplier_key)
        );
        """,
        database='airflow'
    )

    # Task to ingest data
    ingest_task = PythonOperator(
        task_id='ingest_data',
        python_callable=ingest_data,
        op_kwargs={
            'data_file': '/opt/airflow/data/Warehouse_and_Retail_Sales.csv',
            'staging_dir': DEFAULT_STAGING_DIR,
            'iterations': 2  # Generate more data for testing
        }
    )

    # Task to process data
    process_task = PythonOperator(
        task_id='process_data',
        python_callable=process_staged_data,
        op_kwargs={
            'staging_dir': DEFAULT_STAGING_DIR,
            'processed_dir': DEFAULT_PROCESSED_DIR,
            'transformed_dir': DEFAULT_TRANSFORMED_DIR
        }
    )

    # Task to validate data
    validate_task = PythonOperator(
        task_id='validate_data',
        python_callable=validate_data,
        op_kwargs={
            'transformed_dir': DEFAULT_TRANSFORMED_DIR
        }
    )

    # Task to load data to warehouse
    load_task = PythonOperator(
        task_id='load_data',
        python_callable=load_data_to_warehouse,
        op_kwargs={
            'transformed_dir': DEFAULT_TRANSFORMED_DIR
        }
    )

    # Optional: Task to clean up staging files older than 7 days
    cleanup_task = BashOperator(
        task_id='cleanup_old_files',
        bash_command=f'find {DEFAULT_STAGING_DIR} -name "*.csv" -mtime +7 -delete'
    )

    # Set up dependencies
    init_directories >> init_schema >> ingest_task >> process_task >> validate_task >> load_task >> cleanup_task
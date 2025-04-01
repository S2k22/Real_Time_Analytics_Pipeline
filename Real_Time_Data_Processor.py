#!/usr/bin/env python3
"""
Simplified Real-Time Data Ingestion Script for Alcohol Sales Transactions
This version doesn't require Kafka

This script:
1. Generates transactions based on real sales data
2. Saves them directly to staging files
3. Ensures data integrity with proper error handling and logging
"""

import os
import sys
import logging
import argparse
import json
import time
import random
import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Default directories and settings
DEFAULT_STAGING_DIR = "staging/real_time"
DEFAULT_PROCESSED_DIR = "processed/real_time"
DEFAULT_TRANSFORMED_DIR = "transformed/real_time"
DEFAULT_BATCH_SIZE = 10  # Process in batches of 10 records
DEFAULT_INTERVAL = 30  # Process every 30 seconds

# Path to your real dataset
REAL_DATA_PATH = "Warehouse_and_Retail_Sales.csv"


# Load your real dataset
def load_real_data(data_path):
    """Load the real alcohol sales dataset"""
    try:
        # Load your actual dataset
        df = pd.read_csv(data_path)
        logger.info(f"Successfully loaded data with {len(df)} records")
        return df
    except Exception as e:
        logger.error(f"Error loading real data: {e}")
        sys.exit(1)


def generate_random_sales_value(mean, std, max_val, base_value=None):
    """
    Generate a random sales value that follows a realistic distribution:
    - Values near the mean are more likely
    - Values far from the mean are less likely
    - Will never exceed the maximum value

    Uses a truncated normal distribution
    """
    if base_value is not None:
        # If we have a base value, use it to influence the generated value
        # but still maintain the distribution properties
        weight = random.uniform(0.3, 0.7)  # How much to weight the base value vs. the distribution

        # Generate from normal distribution
        from_distribution = np.random.normal(mean, std)
        # Combine with base value
        value = (base_value * weight) + (from_distribution * (1 - weight))
    else:
        # Generate purely from the distribution
        value = np.random.normal(mean, std)

    # Ensure non-negative and below max
    return min(max(1, value), max_val)


def generate_transaction(real_data_df):
    """
    Generate a random sales transaction based on real data.
    Preserves valid product combinations by using the complete set of
    product attributes (item_code, supplier, description, type) together.
    """
    # Select a random row from the real dataset to get a valid product combination
    random_row = real_data_df.sample(1).iloc[0]

    # Use the actual year from the dataset or restrict to 2017-2020
    year = min(max(int(random_row["YEAR"]), 2017), 2020)

    # Use the actual month from the dataset, ensure it's an integer between 1-12
    month = min(max(int(random_row["MONTH"]), 1), 12)

    # Get product information as a set to preserve relationship
    product_info = {
        "supplier": random_row["SUPPLIER"],
        "item_code": int(random_row["ITEM CODE"]),
        "item_description": random_row["ITEM DESCRIPTION"],
        "item_type": random_row["ITEM TYPE"]
    }

    # Generate new sales values only
    retail_sales = generate_random_sales_value(
        mean=7.0, std=30, max_val=2700,
        base_value=float(random_row["RETAIL SALES"])
    )

    retail_transfers = generate_random_sales_value(
        mean=6, std=30, max_val=1990,
        base_value=float(random_row["RETAIL TRANSFERS"])
    )

    warehouse_sales = generate_random_sales_value(
        mean=25, std=249, max_val=1817,
        base_value=float(random_row["WAREHOUSE SALES"])
    )

    # Calculate proper total sales
    total_sales = retail_sales + warehouse_sales

    # Generate a transaction ID with timestamp to ensure uniqueness
    transaction_id = f"TX-{int(time.time())}-{random.randint(1000, 9999)}"

    # Create the transaction object that maintains data integrity
    transaction = {
        "transaction_id": transaction_id,
        "timestamp": datetime.now().isoformat(),
        "year": year,
        "month": month,
        # Keep product information together as a unit
        "supplier": product_info["supplier"],
        "item_code": product_info["item_code"],
        "item_description": product_info["item_description"],
        "item_type": product_info["item_type"],
        # Only randomize the sales figures
        "retail_sales": round(retail_sales, 2),
        "retail_transfers": round(retail_transfers, 2),
        "warehouse_sales": round(warehouse_sales, 2),
        "total_sales": round(total_sales, 2)
    }

    return transaction


def simulate_kafka_method(staging_dir, data_path, iterations=1):
    """Simulate Kafka by directly writing data to files"""
    try:
        # Load the real data
        real_data_df = load_real_data(data_path)

        # Generate transactions
        for i in range(iterations):
            # Generate 1-5 transactions per batch
            num_transactions = random.randint(1, 5)
            batch = []

            for _ in range(num_transactions):
                # Generate transaction
                transaction = generate_transaction(real_data_df)
                batch.append(transaction)
                logger.info(f"Generated transaction: {transaction['transaction_id']}")

            # Create timestamp for filename
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")

            # Create directory if it doesn't exist
            os.makedirs(Path(staging_dir), exist_ok=True)

            # Save to JSON file
            filename = f"{staging_dir}/sales_data_{timestamp}.json"
            with open(filename, 'w') as f:
                json.dump(batch, f, indent=2)

            # Also save to CSV for easier processing
            df = pd.DataFrame(batch)
            csv_filename = f"{staging_dir}/sales_data_{timestamp}.csv"
            df.to_csv(csv_filename, index=False)

            logger.info(f"Saved {len(batch)} transactions to {csv_filename}")

            # Simulate delay between batches
            if i < iterations - 1:
                time.sleep(1)

        return True

    except Exception as e:
        logger.error(f"Error in simulate_kafka_method: {e}")
        return False


def main():
    """Main function to handle command line arguments and run simulated Kafka"""
    parser = argparse.ArgumentParser(description='Real-time data ingestion for alcohol sales transactions')
    parser.add_argument('--iterations', type=int, default=1,
                        help='Number of iterations to run (default: 1)')
    parser.add_argument('--output-dir', type=str, default=DEFAULT_STAGING_DIR,
                        help=f'Output directory for staging data (default: {DEFAULT_STAGING_DIR})')
    parser.add_argument('--data-file', type=str, default=REAL_DATA_PATH,
                        help='Path to your real data file')
    parser.add_argument('--method', type=str, default='kafka',
                        help='Method to use (only used for compatibility, always uses simulation)')

    args = parser.parse_args()

    # Create staging directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)

    try:
        logger.info(f"Starting data ingestion with simulated Kafka...")
        if simulate_kafka_method(args.output_dir, args.data_file, args.iterations):
            logger.info("Data ingestion completed successfully")
            return 0
        else:
            logger.error("Data ingestion failed")
            return 1

    except KeyboardInterrupt:
        logger.info("Program terminated by user")
        return 0

    except Exception as e:
        logger.error(f"Error: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())
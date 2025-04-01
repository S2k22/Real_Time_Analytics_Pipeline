

import os
import sys
import glob
import logging
import argparse
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Default directories
DEFAULT_STAGING_DIR = "staging"
DEFAULT_PROCESSED_DIR = "processed"
DEFAULT_OUTPUT_DIR = "transformed"


def setup_directories(staging_dir, processed_dir, output_dir):
    # Create full paths
    staging_path = Path(staging_dir)
    processed_path = Path(processed_dir)
    output_path = Path(output_dir)

    # Create directories if they don't exist
    for path in [staging_path, processed_path, output_path]:
        if not path.exists():
            path.mkdir(parents=True, exist_ok=True)
            logger.info(f"Created directory: {path}")

    return staging_path, processed_path, output_path


def find_new_files(staging_path, processed_path, file_pattern="*.csv"):
    # Get all CSV files in staging directory
    all_files = list(staging_path.glob(file_pattern))

    # Get list of already processed files
    processed_marker = processed_path / "processed_files.txt"

    processed_files = set()
    if processed_marker.exists():
        with open(processed_marker, 'r') as f:
            processed_files = set(line.strip() for line in f)

    # Filter new files
    new_files = [file for file in all_files if str(file) not in processed_files]
    logger.info(f"Found {len(new_files)} new files to process")

    return new_files, processed_marker


def mark_as_processed(file_paths, processed_marker):
    with open(processed_marker, 'a') as f:
        for file_path in file_paths:
            f.write(f"{file_path}\n")
    logger.info(f"Marked {len(file_paths)} files as processed")


def clean_data(df):
    logger.info("Cleaning data...")
    original_count = len(df)

    # Make a copy to avoid modification warnings
    cleaned_df = df.copy()

    # Check if we have the transaction_id field
    if 'transaction_id' not in cleaned_df.columns:
        # This might be the original Warehouse_and_Retail_Sales.csv
        logger.info("Original dataset detected - adapting cleaning process")

        # Create a transaction_id if it doesn't exist
        if 'transaction_id' not in cleaned_df.columns:
            cleaned_df['transaction_id'] = [f"TX-ORIG-{i:06d}" for i in range(len(cleaned_df))]

        # Handle missing values
        # Fill missing numeric values with zeros
        numeric_cols = ['RETAIL SALES', 'RETAIL TRANSFERS', 'WAREHOUSE SALES']
        for col in numeric_cols:
            if col in cleaned_df.columns:
                cleaned_df[col] = pd.to_numeric(cleaned_df[col], errors='coerce').fillna(0)

        # Fill missing categorical values with "Unknown"
        categorical_cols = ['SUPPLIER', 'ITEM DESCRIPTION', 'ITEM TYPE']
        for col in categorical_cols:
            if col in cleaned_df.columns:
                cleaned_df[col] = cleaned_df[col].fillna('Unknown')

        # Ensure data type consistency
        # Convert ITEM CODE to integer if it exists
        if 'ITEM CODE' in cleaned_df.columns:
            cleaned_df['ITEM CODE'] = pd.to_numeric(cleaned_df['ITEM CODE'], errors='coerce').fillna(0).astype(int)

        # Data validation
        # Check for anomalies in numeric fields and fix negative values
        for col in numeric_cols:
            if col in cleaned_df.columns:
                cleaned_df.loc[cleaned_df[col] < 0, col] = 0

        # Add total sales column if it doesn't exist
        if 'RETAIL SALES' in cleaned_df.columns and 'WAREHOUSE SALES' in cleaned_df.columns:
            if 'TOTAL SALES' not in cleaned_df.columns:
                cleaned_df['TOTAL SALES'] = cleaned_df['RETAIL SALES'] + cleaned_df['WAREHOUSE SALES']

    else:
        cleaned_df = cleaned_df.drop_duplicates(subset=['transaction_id'])
        logger.info(f"Removed {original_count - len(cleaned_df)} duplicate transactions")

        # Handle missing values for critical fields
        critical_fields = ['transaction_id', 'item_code', 'supplier']
        cleaned_df = cleaned_df.dropna(subset=critical_fields)
        logger.info(f"Removed rows with missing critical fields, remaining: {len(cleaned_df)}")

        # Fill missing numeric values with zeros
        numeric_cols = ['retail_sales', 'retail_transfers', 'warehouse_sales', 'total_sales']
        for col in numeric_cols:
            if col in cleaned_df.columns:
                cleaned_df[col] = pd.to_numeric(cleaned_df[col], errors='coerce').fillna(0)

        # Fill missing categorical values with "Unknown"
        categorical_cols = ['item_description', 'item_type']
        for col in categorical_cols:
            if col in cleaned_df.columns:
                cleaned_df[col] = cleaned_df[col].fillna('Unknown')

        # Ensure data type consistency
        if 'item_code' in cleaned_df.columns:
            cleaned_df['item_code'] = pd.to_numeric(cleaned_df['item_code'], errors='coerce').fillna(0).astype(int)

        # Data validation - fix negative values
        for col in numeric_cols:
            if col in cleaned_df.columns:
                cleaned_df.loc[cleaned_df[col] < 0, col] = 0

        # Verify total_sales calculation
        if all(col in cleaned_df.columns for col in ['retail_sales', 'warehouse_sales', 'total_sales']):
            # Check if there's a mismatch between calculated and stored total_sales
            calculated_total = cleaned_df['retail_sales'] + cleaned_df['warehouse_sales']
            mask = abs(cleaned_df['total_sales'] - calculated_total) > 0.01
            if mask.sum() > 0:
                logger.info(f"Fixing {mask.sum()} rows with incorrect total_sales")
                cleaned_df.loc[mask, 'total_sales'] = calculated_total[mask]

    return cleaned_df

def transform_data(df):
    logger.info("Transforming data...")

    transformed_df = df.copy()

    # Map for column standardization (include YEAR and MONTH)
    column_mapping = {
        'SUPPLIER': 'supplier',
        'ITEM CODE': 'product_id',
        'ITEM DESCRIPTION': 'product_name',
        'ITEM TYPE': 'product_type',
        'RETAIL SALES': 'retail_sales',
        'RETAIL TRANSFERS': 'retail_transfers',
        'WAREHOUSE SALES': 'warehouse_sales',
        'TOTAL SALES': 'total_sales',
        'YEAR': 'year',
        'MONTH': 'month'
    }

    # Rename columns that exist in the dataframe
    rename_dict = {old: new for old, new in column_mapping.items() if old in transformed_df.columns}
    if rename_dict:
        transformed_df = transformed_df.rename(columns=rename_dict)

    # If there's already an item_code column, rename it to product_id
    if 'item_code' in transformed_df.columns and 'product_id' not in transformed_df.columns:
        transformed_df = transformed_df.rename(columns={'item_code': 'product_id'})

    # If there's already an item_description column, rename it to product_name
    if 'item_description' in transformed_df.columns and 'product_name' not in transformed_df.columns:
        transformed_df = transformed_df.rename(columns={'item_description': 'product_name'})

    # If there's already an item_type column, rename it to product_type
    if 'item_type' in transformed_df.columns and 'product_type' not in transformed_df.columns:
        transformed_df = transformed_df.rename(columns={'item_type': 'product_type'})

    # Drop rows where product_type is "Unknown"
    if 'product_type' in transformed_df.columns:
        transformed_df = transformed_df[transformed_df['product_type'] != 'Unknown']

    return transformed_df



def process_file(file_path, output_path):
    """Process a single file: read, clean, transform, and save"""
    logger.info(f"Processing file: {file_path}")

    try:
        # Read the file
        df = pd.read_csv(file_path)
        logger.info(f"Read {len(df)} rows from {file_path}")

        # Clean the data
        cleaned_df = clean_data(df)

        # Transform the data
        transformed_df = transform_data(cleaned_df)

        # Create output filename
        file_stem = Path(file_path).stem
        output_file = output_path / f"{file_stem}_transformed.csv"

        # Save transformed data
        transformed_df.to_csv(output_file, index=False)
        logger.info(f"Saved transformed data to {output_file}")

        return True
    except Exception as e:
        logger.error(f"Error processing {file_path}: {e}")
        return False


def main():
    # Parse command line arguments
    parser = argparse.ArgumentParser(description='Clean and transform alcohol sales data')
    parser.add_argument('--staging-dir', default=DEFAULT_STAGING_DIR, help='Directory containing input CSV files')
    parser.add_argument('--processed-dir', default=DEFAULT_PROCESSED_DIR, help='Directory to track processed files')
    parser.add_argument('--output-dir', default=DEFAULT_OUTPUT_DIR, help='Directory for output files')
    parser.add_argument('--single-file',
                        help='Process a single file directly instead of scanning the staging directory')
    args = parser.parse_args()

    logger.info("Starting data cleaning and transformation process")

    # Setup directories
    staging_path, processed_path, output_path = setup_directories(
        args.staging_dir, args.processed_dir, args.output_dir
    )

    # Check if we're processing a single file
    if args.single_file:
        logger.info(f"Processing single file: {args.single_file}")
        single_file_path = Path(args.single_file)
        if not single_file_path.exists():
            logger.error(f"File not found: {args.single_file}")
            return

        # Process the single file
        if process_file(single_file_path, output_path):
            logger.info(f"Successfully processed {single_file_path}")
        else:
            logger.error(f"Failed to process {single_file_path}")
    else:
        # Find new files to process
        new_files, processed_marker = find_new_files(staging_path, processed_path)

        if not new_files:
            logger.info("No new files to process. Exiting.")
            return

        # Process each file
        successfully_processed = []
        for file_path in new_files:
            if process_file(file_path, output_path):
                successfully_processed.append(file_path)

        # Mark files as processed
        if successfully_processed:
            mark_as_processed(successfully_processed, processed_marker)
            logger.info(f"Successfully processed {len(successfully_processed)} files")

    logger.info("Data cleaning and transformation process completed")


if __name__ == "__main__":
    main()
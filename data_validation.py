#!/usr/bin/env python3
"""
Data Quality & Validation Script for Alcohol Sales Data

This script performs essential validation checks on alcohol sales data:
1. Detects missing values in critical fields
2. Identifies duplicate transactions
3. Finds outliers in sales values 
4. Validates data integrity
5. Logs validation results
"""

import os
import sys
import logging
import argparse
import numpy as np
import pandas as pd
from datetime import datetime
from pathlib import Path
from scipy import stats

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("data_validation.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class DataValidator:
    """Data validation class for alcohol sales data"""
    
    def __init__(self):
        """Initialize the DataValidator with default thresholds"""
        self.thresholds = {
            "missing_values_pct": 5.0,
            "duplicate_rows_pct": 1.0,
            "outlier_z_score": 3.0
        }
        
        self.validation_results = {
            "timestamp": datetime.now().isoformat(),
            "checks": [],
            "summary": {
                "total_checks": 0,
                "passed_checks": 0, 
                "warning_checks": 0,
                "failed_checks": 0
            }
        }
    
    def validate(self, data_path):
        """Perform validation checks on the data"""
        # Load data
        df = self.load_data(data_path)
        
        # Perform checks
        self.check_missing_values(df)
        self.check_duplicate_rows(df)
        self.check_outliers(df)
        self.check_data_types(df)
        self.check_business_rules(df)
        
        # Calculate summary
        self.validation_results["summary"]["total_checks"] = len(self.validation_results["checks"])
        self.validation_results["summary"]["passed_checks"] = sum(
            1 for check in self.validation_results["checks"] if check["status"] == "PASS"
        )
        self.validation_results["summary"]["warning_checks"] = sum(
            1 for check in self.validation_results["checks"] if check["status"] == "WARNING"
        )
        self.validation_results["summary"]["failed_checks"] = sum(
            1 for check in self.validation_results["checks"] if check["status"] == "FAIL"
        )
        
        # Determine overall status
        if self.validation_results["summary"]["failed_checks"] > 0:
            self.validation_results["status"] = "FAIL"
        elif self.validation_results["summary"]["warning_checks"] > 0:
            self.validation_results["status"] = "WARNING"
        else:
            self.validation_results["status"] = "PASS"
        
        # Print summary to console
        self.print_summary()
        
        return self.validation_results
    
    def load_data(self, data_path):
        """Load data from CSV file"""
        try:
            df = pd.read_csv(data_path)
            logger.info(f"Loaded CSV data from {data_path}, {len(df)} rows")
            return df
        except Exception as e:
            logger.error(f"Error loading data: {e}")
            sys.exit(1)
    
    def check_missing_values(self, df):
        """Check for missing values in each column"""
        threshold = self.thresholds["missing_values_pct"]
        
        # Check each column
        for column in df.columns:
            missing_count = df[column].isna().sum()
            missing_pct = (missing_count / len(df)) * 100
            
            # Determine status
            if missing_pct >= threshold:
                status = "FAIL"
            elif missing_pct > 0:
                status = "WARNING"
            else:
                status = "PASS"
            
            # Add to results
            self.validation_results["checks"].append({
                "check_name": f"missing_values_{column}",
                "check_type": "missing_values",
                "column": column,
                "count": int(missing_count),
                "percentage": float(missing_pct),
                "threshold": float(threshold),
                "status": status
            })
    
    def check_duplicate_rows(self, df):
        """Check for duplicate rows"""
        threshold = self.thresholds["duplicate_rows_pct"]
        
        # Check for duplicates
        duplicate_count = df.duplicated().sum()
        duplicate_pct = (duplicate_count / len(df)) * 100
        
        # Determine status
        if duplicate_pct >= threshold:
            status = "FAIL"
        elif duplicate_pct > 0:
            status = "WARNING"
        else:
            status = "PASS"
        
        # Add to results
        self.validation_results["checks"].append({
            "check_name": "duplicate_rows",
            "check_type": "duplicate_rows",
            "count": int(duplicate_count),
            "percentage": float(duplicate_pct),
            "threshold": float(threshold),
            "status": status
        })
        
        # Check for duplicate transaction IDs if column exists
        if 'transaction_id' in df.columns:
            dup_txn_count = df.duplicated(subset=['transaction_id']).sum()
            dup_txn_pct = (dup_txn_count / len(df)) * 100
            
            # Determine status
            if dup_txn_pct > 0:  # Any duplicate transaction IDs are a failure
                status = "FAIL"
            else:
                status = "PASS"
            
            # Add to results
            self.validation_results["checks"].append({
                "check_name": "duplicate_transaction_ids",
                "check_type": "duplicate_rows",
                "count": int(dup_txn_count),
                "percentage": float(dup_txn_pct),
                "threshold": 0.0,
                "status": status
            })
    
    def check_outliers(self, df):
        """Check for outliers in numeric columns"""
        z_threshold = self.thresholds["outlier_z_score"]
        
        # Numeric columns to check
        numeric_columns = df.select_dtypes(include=['number']).columns.tolist()
        
        # Focus on sales columns if available
        sales_columns = [col for col in ['retail_sales', 'warehouse_sales', 'total_sales'] 
                         if col in numeric_columns]
        if sales_columns:
            numeric_columns = sales_columns
        
        # Check each numeric column
        for column in numeric_columns:
            # Skip columns with all missing values
            if df[column].isna().all():
                continue
            
            # Get non-null values
            values = df[column].dropna()
            
            # Skip if not enough values
            if len(values) < 10:
                continue
            
            # Calculate Z-scores
            z_scores = np.abs(stats.zscore(values))
            
            # Identify outliers
            outliers = z_scores > z_threshold
            outlier_count = outliers.sum()
            outlier_pct = (outlier_count / len(values)) * 100
            
            # Get outlier details
            if outlier_count > 0:
                outlier_indices = np.where(outliers)[0]
                outlier_values = values.iloc[outlier_indices]
                min_outlier = float(outlier_values.min())
                max_outlier = float(outlier_values.max())
            else:
                min_outlier = None
                max_outlier = None
            
            # Determine status
            if outlier_pct >= 5.0:  # More than 5% outliers is a failure
                status = "FAIL"
            elif outlier_pct > 0:
                status = "WARNING"
            else:
                status = "PASS"
            
            # Add to results
            self.validation_results["checks"].append({
                "check_name": f"outliers_{column}",
                "check_type": "outliers",
                "column": column,
                "count": int(outlier_count),
                "percentage": float(outlier_pct),
                "threshold": float(z_threshold),
                "min_value": float(values.min()),
                "max_value": float(values.max()),
                "mean": float(values.mean()),
                "std": float(values.std()),
                "min_outlier": min_outlier,
                "max_outlier": max_outlier,
                "status": status
            })
    
    def check_data_types(self, df):
        """Check data types and formats"""
        # Check for key columns relevant to alcohol sales
        expected_columns = ['product_id', 'supplier', 'retail_sales', 'warehouse_sales', 'total_sales']
        missing_columns = [col for col in expected_columns if col not in df.columns]
        
        if missing_columns:
            self.validation_results["checks"].append({
                "check_name": "missing_key_columns",
                "check_type": "data_type",
                "missing_columns": missing_columns,
                "status": "FAIL"
            })
        
        # Check timestamp format if present
        if 'timestamp' in df.columns:
            try:
                # Convert to datetime
                timestamps = pd.to_datetime(df['timestamp'], errors='coerce')
                invalid_count = timestamps.isna().sum()
                invalid_pct = (invalid_count / len(df)) * 100
                
                # Determine status
                if invalid_pct >= 5.0:
                    status = "FAIL"
                elif invalid_pct > 0:
                    status = "WARNING"
                else:
                    status = "PASS"
                
                # Add to results
                self.validation_results["checks"].append({
                    "check_name": "invalid_timestamps",
                    "check_type": "data_type",
                    "count": int(invalid_count),
                    "percentage": float(invalid_pct),
                    "threshold": 5.0,
                    "status": status
                })
            
            except Exception as e:
                self.validation_results["checks"].append({
                    "check_name": "timestamp_format_error",
                    "check_type": "data_type",
                    "error": str(e),
                    "status": "FAIL"
                })
    
    def check_business_rules(self, df):
        """Check business rules and constraints"""
        # Check if total_sales = retail_sales + warehouse_sales
        if all(col in df.columns for col in ['total_sales', 'retail_sales', 'warehouse_sales']):
            # Calculate expected total
            expected_total = df['retail_sales'] + df['warehouse_sales']
            
            # Allow for small floating-point differences
            mismatch = (abs(df['total_sales'] - expected_total) > 0.01)
            mismatch_count = mismatch.sum()
            mismatch_pct = (mismatch_count / len(df)) * 100
            
            # Determine status
            if mismatch_pct >= 5.0:
                status = "FAIL"
            elif mismatch_pct > 0:
                status = "WARNING"
            else:
                status = "PASS"
            
            # Add to results
            self.validation_results["checks"].append({
                "check_name": "total_sales_mismatch",
                "check_type": "business_rules",
                "count": int(mismatch_count),
                "percentage": float(mismatch_pct),
                "threshold": 5.0,
                "status": status
            })
    
    def print_summary(self):
        """Print validation summary to console"""
        print("\n" + "="*50)
        print(f"DATA VALIDATION SUMMARY: {self.validation_results['status']}")
        print("="*50)
        print(f"Total Checks: {self.validation_results['summary']['total_checks']}")
        print(f"Passed: {self.validation_results['summary']['passed_checks']}")
        print(f"Warnings: {self.validation_results['summary']['warning_checks']}")
        print(f"Failed: {self.validation_results['summary']['failed_checks']}")
        print("-"*50)
        
        # Print failed checks
        if self.validation_results['summary']['failed_checks'] > 0:
            print("\nFAILED CHECKS:")
            for check in self.validation_results["checks"]:
                if check["status"] == "FAIL":
                    print(f"- {check['check_name']}")
        
        # Print warning checks
        if self.validation_results['summary']['warning_checks'] > 0:
            print("\nWARNING CHECKS:")
            for check in self.validation_results["checks"]:
                if check["status"] == "WARNING":
                    print(f"- {check['check_name']}")
        
        print("="*50 + "\n")

def main():
    """Main entry point for the script"""
    parser = argparse.ArgumentParser(description='Validate alcohol sales data')
    parser.add_argument('data_path', help='Path to the CSV file to validate')
    parser.add_argument('--output', help='Path to save validation results JSON')
    args = parser.parse_args()
    
    # Create validator and run validation
    validator = DataValidator()
    results = validator.validate(args.data_path)
    
    # Save results if output path provided
    if args.output:
        import json
        with open(args.output, 'w') as f:
            json.dump(results, f, indent=2)
        logger.info(f"Validation results saved to {args.output}")
    
    # Exit with appropriate exit code
    if results['status'] == "FAIL":
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Real-Time Data Ingestion Script for Alcohol Sales Transactions
Using Kafka for streaming data

This script:
1. Sets up a Kafka topic for alcohol sales transactions
2. Runs a producer that generates transactions based on real sales data
3. Runs a consumer that collects transactions and saves them to staging files
4. Ensures data integrity with proper error handling and logging
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
import threading
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
def load_real_data():
    """Load the real alcohol sales dataset"""
    try:
        # Load your actual dataset
        df = pd.read_csv(REAL_DATA_PATH)
        logger.info(f"Successfully loaded data with {len(df)} records")
        return df
    except Exception as e:
        logger.error(f"Error loading real data: {e}")
        sys.exit(1)


# Load the dataset at module initialization
REAL_DATA_DF = load_real_data()


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


def generate_transaction():
    """
    Generate a random sales transaction based on real data.
    Preserves valid product combinations by using the complete set of
    product attributes (item_code, supplier, description, type) together.
    """
    # Select a random row from the real dataset to get a valid product combination
    random_row = REAL_DATA_DF.sample(1).iloc[0]

    # Use the actual year from the dataset or restrict to 2017-2020
    year = min(max(int(random_row["YEAR"]), 2017), 2020)

    # Use the actual month from the dataset, ensure it's an integer between 1-12
    month = min(max(int(random_row["MONTH"]), 1), 12)

    # Generate realistic sales values using statistics and the base data
    # Statistics provided:
    # - Retail sales: mean=7.0, std=30, max=2700
    # - Retail transfers: mean=6, std=30, max=1990
    # - Warehouse sales: mean=25, std=249, max=1817

    # IMPORTANT: We're keeping the product attributes together as a single unit
    # to maintain data integrity and ensure valid combinations

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


# ======================= KAFKA METHOD =======================
def setup_kafka(topic_name='alcohol-sales-transactions'):
    """Set up Kafka topic"""
    try:
        from kafka.admin import KafkaAdminClient, NewTopic
        from kafka.errors import TopicAlreadyExistsError, NoBrokersAvailable

        bootstrap_servers = 'localhost:9092'  # Update with your Kafka broker address

        try:
            admin_client = KafkaAdminClient(
                bootstrap_servers=bootstrap_servers,
                client_id='sales-admin'
            )

            # Create topic if it doesn't exist
            topic_list = [NewTopic(
                name=topic_name,
                num_partitions=1,
                replication_factor=1
            )]

            admin_client.create_topics(new_topics=topic_list, validate_only=False)
            logger.info(f"Topic {topic_name} created successfully")
        except TopicAlreadyExistsError:
            logger.info(f"Topic {topic_name} already exists")
        except NoBrokersAvailable:
            logger.error("No Kafka brokers available. Make sure Kafka is running.")
            raise

        return bootstrap_servers, topic_name

    except ImportError:
        logger.error("Kafka-python package not found. Install it with 'pip install kafka-python'")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Error setting up Kafka: {e}")
        sys.exit(1)


def run_kafka_producer(bootstrap_servers, topic_name, interval=3, max_iterations=None):
    """Run Kafka producer to generate and send sales transaction data"""
    try:
        from kafka import KafkaProducer

        # Create producer
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )

        iteration = 0
        while max_iterations is None or iteration < max_iterations:
            # Generate 1-5 transactions per batch
            num_transactions = random.randint(1, 5)

            for _ in range(num_transactions):
                # Generate transaction
                transaction = generate_transaction()

                # Send message to Kafka topic
                producer.send(topic_name, value=transaction)
                logger.info(f"Produced transaction: {transaction['transaction_id']}")

            # Flush to ensure all messages are sent
            producer.flush()

            # Increment iteration counter
            iteration += 1

            # Sleep before next iteration
            if max_iterations is None or iteration < max_iterations:
                time.sleep(interval)

        logger.info("Finished producing messages")
        producer.close()

    except Exception as e:
        logger.error(f"Producer error: {e}")
        if 'producer' in locals():
            producer.close()


def run_kafka_consumer(bootstrap_servers, topic_name, batch_size=10, save_interval=30):
    """Run Kafka consumer to receive and save sales transaction data"""
    try:
        from kafka import KafkaConsumer

        # Create consumer
        consumer = KafkaConsumer(
            topic_name,
            bootstrap_servers=bootstrap_servers,
            auto_offset_reset='latest',
            enable_auto_commit=True,
            group_id='sales-consumer-group',
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

        batch = []
        last_save_time = time.time()

        logger.info("Consumer started, waiting for messages...")

        for message in consumer:
            transaction = message.value
            logger.info(f"Consumed transaction: {transaction['transaction_id']}")

            # Add to current batch
            batch.append(transaction)

            current_time = time.time()
            should_save = (len(batch) >= batch_size) or (current_time - last_save_time >= save_interval)

            if should_save and batch:
                # Create timestamp for filename
                timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
                filename = f"{DEFAULT_STAGING_DIR}/sales_data_{timestamp}.json"

                # Create directory if it doesn't exist
                os.makedirs(os.path.dirname(filename), exist_ok=True)

                # Save the data to the staging area
                with open(filename, 'w') as f:
                    json.dump(batch, f, indent=2)

                logger.info(f"Saved {len(batch)} transactions to {filename}")

                # Also convert to CSV for easier processing later
                df = pd.DataFrame(batch)
                csv_filename = f"{DEFAULT_STAGING_DIR}/sales_data_{timestamp}.csv"
                df.to_csv(csv_filename, index=False)
                logger.info(f"Saved data to CSV: {csv_filename}")

                # Reset batch and update last save time
                batch = []
                last_save_time = current_time

    except KeyboardInterrupt:
        logger.info("Consumer stopping...")
    except Exception as e:
        logger.error(f"Consumer error: {e}")
    finally:
        if 'consumer' in locals():
            consumer.close()


def run_kafka_method(max_iterations=None):
    """Run both Kafka producer and consumer"""
    try:
        # Set up Kafka
        bootstrap_servers, topic_name = setup_kafka()

        # Start the producer in a separate thread
        producer_thread = threading.Thread(
            target=run_kafka_producer,
            args=(bootstrap_servers, topic_name),
            kwargs={"interval": 3, "max_iterations": max_iterations},
            daemon=True
        )
        producer_thread.start()

        # Give the producer a moment to start sending messages
        time.sleep(2)

        # Run the consumer
        run_kafka_consumer(bootstrap_servers, topic_name, batch_size=5, save_interval=15)

    except Exception as e:
        logger.error(f"Error in Kafka method: {e}")
        logger.error("Make sure Kafka is running on your system or update the Kafka configuration")


def main():
    """Main function to handle command line arguments and run Kafka"""
    parser = argparse.ArgumentParser(description='Real-time data ingestion for alcohol sales transactions using Kafka')
    parser.add_argument('--iterations', type=int, default=None,
                        help='Number of iterations to run (default: runs continuously)')
    parser.add_argument('--output-dir', type=str, default=DEFAULT_STAGING_DIR,
                        help=f'Output directory for staging data (default: {DEFAULT_STAGING_DIR})')
    parser.add_argument('--data-file', type=str, default=REAL_DATA_PATH,
                        help='Path to your real data file')

    args = parser.parse_args()

    # Update the data file path if provided as an argument
    if args.data_file != REAL_DATA_PATH:
        import sys
        this_module = sys.modules[__name__]
        setattr(this_module, 'REAL_DATA_PATH', args.data_file)
        # Reload data
        setattr(this_module, 'REAL_DATA_DF', load_real_data())

    # Create staging directory if it doesn't exist
    os.makedirs(args.output_dir, exist_ok=True)

    try:
        logger.info("Starting Kafka method...")
        run_kafka_method(max_iterations=args.iterations)
    except KeyboardInterrupt:
        logger.info("Program terminated by user")
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
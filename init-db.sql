-- Create the data warehouse database
CREATE DATABASE IF NOT EXISTS alcohol_sales_dw;

-- Grant privileges to the airflow user
GRANT ALL PRIVILEGES ON alcohol_sales_dw.* TO 'airflow'@'%';
FLUSH PRIVILEGES;

-- Use the data warehouse database
USE alcohol_sales_dw;

-- Create dimension tables
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

-- Create indexes for better query performance
CREATE INDEX idx_fact_sales_year_month ON fact_sales(year, month);
CREATE INDEX idx_fact_sales_product_key ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_supplier_key ON fact_sales(supplier_key);
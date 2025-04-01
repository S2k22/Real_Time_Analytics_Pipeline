# Introduction
This project implements a data engineering pipeline for processing, analyzing, and visualizing alcohol sales data. The system follows ETL (Extract, Transform, Load) principles to ingest raw sales data, process it through a series of data quality and transformation steps, store it in a dimensional data warehouse, and present actionable insights through an interactive dashboard.

The pipeline is designed with scalability and reliability, utilizing industry-standard tools and containerization technology. The system handles data validation, deduplication, and outlier detection and provides business intelligence visualizations for sales analysis. Additionaly using Kafka and generated data I simulated a real-time data flow.

![image](https://github.com/user-attachments/assets/8180bdd6-3a03-4f70-a50d-2973791a5458)

![image](https://github.com/user-attachments/assets/e47d5136-d6be-46a2-b942-ec823735ee0e)


# Business Problems Solved

This project helps to address several critical business challenges in the industry:
- **Data integration**: The pipeline helps to integrate real-time data flow into the unified data warehouse, allowing to perform comprehensive analysis in real-time.
- **Data Quality Issues**: The robust validation and cleaning processes implemented in the pipeline address inconsistencies, missing values, and errors in sales data. The validation module performs statistical outlier detection, identifies duplicates, and verifies that data are in unified standards, ensuring the Analytical department receives trustworthy data for decision-making.
- **Operational Efficiency**: The automated ETL process replaces manual data compilation and transformation that helps raise efficiency, allowing the use of freed time for more complex business matters. The Airflow-orchestrated workflow reduces the operational overhead by automatically executing each stage of data processing on schedule without the need for manual intervention.
- **Audit and Compliance**: The pipeline maintains detailed logs  throughout the process. Each transformation is documented, and the system tracks which files have been processed using checksum validation, supporting audit requirements and business needs.
- **Scalable Data Processing**: The modular design of the pipeline allows it to handle growing data volumes as the business expands. The containerized architecture can be scaled to process larger datasets by simply adjusting resource allocations.
- **Vizualization**: Automated Plotly dashboards connected to processed and validated data allow the time spent for analytics to be reduced from hours into minutes. The vizualizations included product performance, quarterly performances, analysis of suppliers, sales trends, etc.
- **Modular design**: All parts of the program  are highly modular and can later be very easily customized to fit upcoming business needs. 

# Dataset
The dataset used contains a list of sales and movement data (Montgomery County) by item and department appended monthly. Update Frequency: Monthly. Data was taken from Data.gov, the official data portal of the US government.  Link to data: https://catalog.data.gov/dataset/warehouse-and-retail-sales

# Architecture
- **Apache Airflow**: Orchestrates the workflow execution, manages dependencies, and provides monitoring capabilities
- **MySQL Database**: Functions as the data warehouse with dimensional modeling (star schema)
- **Python Processing Scripts**: Handle the data transformation, cleaning, and validation logic
- **Dash/Plotly Dashboard**: Delivers interactive data visualizations and analytics
- **Docker & Docker Compose**: Provides containerization for consistent deployment across environments

![image](https://github.com/user-attachments/assets/a5e0a061-55bb-47df-a067-a51da50e009f)


# Methodology
## ETL Process Methodology

**The pipeline** implements an ETL methodology with the following phases:
## 1. Extraction Phase
### Data Sourcing Strategies:

- **Original Data Ingestion**: The system processes the source CSV file (Warehouse_and_Retail_Sales.csv) by copying it to a staging area with checksum validation to prevent reprocessing unchanged data.
- **Synthetic Data Generation**: For developing, testing, and augmenting limited datasets, the system can generate synthetic transactions based on patterns observed in the original data. This maintains statistical properties while providing volume.
- **Idempotent Processing**: The pipeline uses checksums to identify previously processed files, ensuring that the same data isn't processed multiple times, promoting reliability and efficiency.

### Data Source Profiling:
- **The system automatically** profiles incoming data to understand its characteristics, detecting anomalies and adapting processing strategies accordingly.
## 2. Transformation Phase
### Data Cleaning Methodology:

- **Missing Value Handling**: Critical fields (product codes, etc.) trigger row removal, while non-critical missing values are imputed with appropriate defaults.
- **Type Consistency Enforcement**: Numeric fields are converted to appropriate data types with validation.
- **Duplicate Detection**: Multi-level deduplication strategy that identifies duplicate transactions based on unique identifiers.

### Data Standardization:

- **Column Standardization**: Input columns are mapped to standardized names following a consistent naming convention.
- **Value Normalization**: Categorical values (product types, supplier names) are standardized to ensure consistency.

### Business Rule Application:

- **Total Sales Verification**: Automatically validates that total sales equals the sum of retail and warehouse sales.
- **Negative Value Correction**: Negative sales figures are identified and corrected.
- **Product Type Classification**: Non-product categories are filtered out based on business rules.

## 3. Loading Phase
### Dimensional Modeling Implementation:

- **Star Schema Design**: Implementation of a star schema with fact and dimension tables.
- **Transaction Management**: SQL transactions ensure data consistency with proper commits
- **Incremental Loading**: Data is loaded incrementally to minimize processing requirements.

### Data Warehouse Integration:

- **The schema** consists of two dimension tables (products and suppliers) and one fact table (sales)
- **Foreign Key Management**: Proper handling of relationships between fact and dimension tables.
- **Transaction Management**: SQL transactions ensure the atomicity of data-loading operations.

## 4. Validation Phase
### Multi-level Validation Strategy:

- **Field-Level Validation**: Individual fields are checked for proper formats, ranges, and values.
  ```def check_missing_values(self, df):
    """Check for missing values in each column"""
    threshold = self.thresholds["missing_values_pct"]
    
    # Check each column
    for column in df.columns:
        missing_count = df[column].isna().sum()
        missing_pct = (missing_count / len(df)) * 100
        
        # Determine status based on threshold
        if missing_pct >= threshold:
            status = "FAIL"
        elif missing_pct > 0:
            status = "WARNING"
        else:
            status = "PASS"
             ```
             
- **Record-Level Validation**: Complete records are validated for internal consistency.
- **Dataset-Level Validation**: Statistical properties of the entire dataset are checked for anomalies.

### Quality Metrics:

- **Completeness**: Percentage of missing values by field
 ```
missing_count = df[column].isna().sum()
missing_pct = (missing_count / len(df)) * 100
```
- **Uniqueness**: Duplicate detection rates
- **Consistency**: Business rule compliance rates
- **Validity**: Data type and range validation metrics
- **Outlier Detection**: Statistical outlier identification using Z-scores
```
  def check_outliers(self, df):
    """Check for outliers in numeric columns"""
    z_threshold = self.thresholds["outlier_z_score"]
    
    # Calculate Z-scores
    z_scores = np.abs(stats.zscore(values))
    
    # Identify outliers
    outliers = z_scores > z_threshold
    outlier_count = outliers.sum()
    outlier_pct = (outlier_count / len(values)) * 100
```
![image](https://github.com/user-attachments/assets/e5f210cb-ba9b-4026-b0ad-d8f8a5ed00d2)

## Dashboard Methodology
### Visualization Selection Rationale:

- **Time Series Analysis**: Line charts with range sliders for temporal trend analysis
- **Composition Analysis**: Pie charts with categorical breakdowns
- **Comparative Analysis**: Bar charts for quarter-over-quarter comparisons
- **Moving Averages**: Trend line overlays to identify underlying patterns

### Interactive Features:

- **Temporal Filtering**: Date range selectors for focusing on specific time periods
- **Real-time Updates**: Automatic refresh mechanisms to display the latest data
- **Unified Hover**: Synchronized tooltips across multiple charts
- **Responsive Design**: The Layout adapts to different screen sizes

# Technology Choices and Rationale
The technologies and methodologies in this project were carefully selected to address specific requirements:
- **Airflow's** Python-based DAG definition provides greater flexibility for complex data transformation logic compared to the visual approach of NiFi. Airflow's extensive scheduler capabilities and robust monitoring make it ideal for managing complex workflows with dependencies.
- **MySQL** for Data Warehouse. MySQL offers the right balance of performance and simplicity for this use case. For moderate data volumes in alcohol sales reporting, MySQL provides sufficient analytical capabilities without the operational complexity and cost of cloud data warehouses like Snowflake or BigQuery.
- **Dash** provides programmatic control and customization capabilities not available in commercial BI tools. The Python-based approach allows seamless integration with the data processing pipeline and enables version-controlled dashboard code.
- **Containerization** ensures consistent environments across development and production, eliminating "works on my machine" problems. Docker Compose orchestration simplifies the deployment of multiple interconnected services.

# Summary 
This project serves as a solid foundation for alcohol sales analysis and data processing, with reasonable architectural decisions for the current data volume (~300K rows) while allowing for future enhancements. 


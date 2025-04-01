FROM apache/airflow:2.6.1

USER root

# Install MySQL client and required libraries for connections
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         default-mysql-client \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

USER airflow

# Install Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create directories
RUN mkdir -p /opt/airflow/dags \
    /opt/airflow/logs \
    /opt/airflow/plugins \
    /opt/airflow/data \
    /opt/airflow/sql_scripts \
    /opt/airflow/staging/real_time \
    /opt/airflow/processed/real_time \
    /opt/airflow/transformed/real_time
# API to ADLS Pipeline

This project demonstrates a complete ingestion pipeline:
- Pulls data from REST API using PySpark
- Transforms and writes data to Azure ADLS Gen2 using Delta Lake
- Orchestrated with Apache Airflow and DatabricksOperator

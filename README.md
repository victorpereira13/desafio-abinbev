# Project Documentation

## Introduction
The aim of this project is to develop a data pipeline using various tools and technologies to fetch data from the Open Brewery DB API, process it, and store it. The project involves orchestration with Apache Airflow, data processing with Databricks notebooks using PySpark, and data storage within Azure Blob Storage.

## Architecture Overview
The data pipeline follows the medallion architecture, which consists of three layers:
- **Bronze Layer**: Raw data storage
- **Silver Layer**: Curated data storage
- **Gold Layer**: Business-ready data storage

## Data Processing
### Databricks Notebooks
Three Databricks notebooks are used for processing data:
- **Bronze Notebook**: Fetches raw data from the Open Brewery DB API and stores it in the Bronze layer.
- **Silver Notebook**: Transforms the raw data into a columnar format (Parquet) and partitions it by brewery location, storing it in the Silver layer.
- **Gold Notebook**: Aggregates the data to create business-ready views, storing it in the Gold layer.

### PySpark
PySpark is used within the Databricks notebooks for data transformation and processing.

## Data Storage
### Azure Blob Storage
Data is stored in Azure Blob Storage, organized into three layers:
- **Bronze Layer**: Raw data in its native format
- **Silver Layer**: Transformed data in a columnar format, partitioned by location
- **Gold Layer**: Aggregated data for business analysis

## Orchestration
### Apache Airflow
Apache Airflow is used to orchestrate the data pipeline. It is hosted on an AWS EC2 instance and can be accessed via a public IP.

### DAGs
Three DAGs are defined in Airflow, each corresponding to a Databricks notebook:
- **Bronze DAG**: Executes the Bronze notebook
- **Silver DAG**: Executes the Silver notebook, dependent on the completion of the Bronze DAG
- **Gold DAG**: Executes the Gold notebook, dependent on the completion of the Silver DAG

## Error Handling
Airflow’s built-in monitoring tools are used to track the status of the DAGs and alert on any failures. Additionally, Databricks sends email notifications for when a DAG fails.

## Testing
Unit tests are included for each Databricks notebook to validate the data processing logic and data quality.

## Cloud Services Deployment
### AWS EC2
Airflow is deployed on an AWS EC2 instance, providing a scalable and reliable orchestration environment.

## Trade-offs
### Orchestration
- **Apache Airflow**:
  - **Pros**: Highly customizable, extensive community support, robust scheduling and monitoring capabilities, and integration with various data sources and services.
  - **Cons**: Can be complex to set up and manage, especially for beginners.
  - **Why Chosen**: Airflow’s flexibility and powerful DAG (Directed Acyclic Graph) management make it ideal for complex workflows. Its ability to handle retries, scheduling, and error handling efficiently aligns well with the project’s requirements.
- **Alternatives**:
  - **Luigi**: Simpler to set up but less flexible and with fewer integrations compared to Airflow.
  - **Prefect**: Easier to use with a more modern interface but less mature and with a smaller community.
  - **Dagster**: Focuses on data quality and lineage but is newer and less widely adopted.

### Data Processing: Databricks Notebooks in PySpark vs. Alternatives
- **Databricks Notebooks in PySpark**:
  - **Pros**: Seamless integration with Apache Spark, collaborative environment, robust performance for large-scale data processing, and built-in support for various data formats.
  - **Cons**: Can be expensive, especially for large-scale deployments.
  - **Why Chosen**: Databricks provides a powerful and scalable platform for big data processing. Its integration with Spark and support for PySpark make it an excellent choice for handling large datasets and complex transformations.
- **Alternatives**:
  - **Jupyter Notebooks**: Great for prototyping and smaller projects but lacks the scalability and performance of Databricks.
  - **Google Colab**: Free and easy to use but limited in terms of scalability and integration with enterprise data sources.
  - **AWS Glue**: Serverless and scalable but less flexible and with a steeper learning curve for complex transformations.


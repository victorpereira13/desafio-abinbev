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
- **Bronze Notebook**: Fetches raw data from the Open Brewery DB API and stores it in the bronze layer.
- **Silver Notebook**: Transforms the raw data Parquet format and partitions it by brewery location, storing it in the silver layer.
- **Gold Notebook**: Aggregates the data to create, storing it in the gold layer.

### PySpark
PySpark is used within the Databricks notebooks for data transformation and processing.

## Data Storage
### Azure Blob Storage
Data is stored in Azure Blob Storage, organized by the three layers.

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

## Running the application
To run the application, simply open your web browser and navigate to the public IP address of the EC2 instance.
The default credentials are used to access Airflow.

## Documentation
The detailed documentation for this project is available in the docs/ directory.




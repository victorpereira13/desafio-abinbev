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
- **Gold Notebook**: Aggregates the data to create, storing it in the Gold layer.

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

## Execution Flow
Execution flow of the pipeline:
- **Bronze DAG**: Executes the job responsible for the bronze layer, and triggers Silver DAG
- **Silver DAG**: Executes the job responsible for the silver layer, and triggers Gold DAG
- **Gold DAG**: Aggregates and stores business-ready data

## Cloud Services Deployment
### AWS EC2
Airflow is deployed on an AWS EC2 instance, providing a scalable and reliable orchestration environment.

## Trade-offs
### Orchestration
- **Apache Airflow**:
  - **Pros**: Highly customizable, extensive community support, robust scheduling and monitoring capabilities, and integration with various data sources and services.
  - **Cons**: Can be complex to set up and manage, especially for beginners.
  - **Why Chosen**: Airflow’s flexibility and powerful DAG (Directed Acyclic Graph) management make it ideal for both simple and complex workflows. Its ability to handle retries, scheduling, and error handling efficiently aligns well with the project’s requirements.
- **Alternative**:
  - **Luigi**: Simpler to set up but less flexible and with fewer integrations compared to Airflow.


### Data Processing: Databricks Notebooks
- **Databricks Notebooks in PySpark**:
  - **Pros**: Seamless integration with Apache Spark, collaborative environment, robust performance for large-scale data processing, and built-in support for various data formats.
  - **Cons**: Can be expensive, especially for large-scale deployments.
  - **Why Chosen**: Databricks provides a powerful and scalable platform for big data processing. Its integration with Spark and support for PySpark make it an excellent choice for handling large datasets and complex transformations.
- **Alternative*:
  - **AWS Glue**: Serverless and scalable but less flexible and its performance may be worse than Databricks for large-scale data processing.

### Data Storage: Azure Blob Storage
- **Azure Blob Storage**:
  - **Pros**: Highly scalable, cost-effective, secure, and integrates well with other Azure services. Supports various data formats and access patterns.
  - **Cons**: Requires familiarity with Azure ecosystem and can incur costs for data egress.
  - **Why Chosen**: Azure Blob Storage offers robust scalability and security features, making it suitable for storing large volumes of data. Its integration with Databricks and other Azure services ensures seamless data flow and management.
- **Alternative**:
  - **Amazon S3**: Equally scalable and widely used but might not integrate as seamlessly with Databricks if the rest of the infrastructure is on Azure.

### Hosting: AWS EC2
- **AWS EC2**:
  - **Pros**: Highly customizable, scalable, and reliable. Wide range of instance types to suit different workloads.
  - **Cons**: Requires management of infrastructure, which can be complex and time-consuming.
  - **Why Chosen**: AWS EC2 provides the flexibility to configure the environment as needed, ensuring optimal performance for hosting Airflow. Its reliability and scalability make it a suitable choice for production workloads.
- **Alternatives**:
  - **Azure Virtual Machines**: Similar capabilities but might not be as cost-effective as EC2.

### Error Handling: Databricks Email Notifications
- **Databricks Email Notifications**:
  - **Pros**: Easy to set up, integrates well with Databricks workflows, and provides timely alerts for errors.
  - **Cons**: Limited to email notifications, which might not be sufficient for all monitoring needs.
  - **Why Chosen**: Databricks’ built-in email notification system ensures that any errors in the data processing workflows are promptly reported, allowing for quick resolution.
- **Alternatives**:
  - **Slack Notifications**: Great for team collaboration but might require custom integration.
  - **CloudWatch Alarms (AWS)**: Robust monitoring and alerting but more complex to set up and manage.

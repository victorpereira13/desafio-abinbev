from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

default_args = {
  'owner': 'airflow',
  'retries': 3, 
  'retry_delay': timedelta(minutes=5)
}

with DAG('dag_bronze',
  start_date = days_ago(2),
  schedule_interval = '5 12 * * *',
  default_args = default_args
  ) as dag:

  opr_run_now = DatabricksRunNowOperator(
    task_id = 'run_bronze_job',
    databricks_conn_id = 'databricks_default',
    job_id = 824401042013269
  )

  trigger_dag_silver = TriggerDagRunOperator(
    task_id='trigger_silver_dag',
    trigger_dag_id='dag_silver'
  )

opr_run_now >> trigger_dag_silver
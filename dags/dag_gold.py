from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.utils.dates import days_ago
from datetime import timedelta


default_args = {
  'owner': 'airflow',
  'retries': 3, 
  'retry_delay': timedelta(minutes=5)
}

with DAG('dag_gold',
  start_date = days_ago(2),
  schedule_interval = None,
  default_args = default_args
  ) as dag:

  opr_run_now = DatabricksRunNowOperator(
    task_id = 'run_gold_job',
    databricks_conn_id = 'databricks_default',
    job_id = 121292047242952
  )
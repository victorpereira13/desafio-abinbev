from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.utils.dates import days_ago

default_args = {
  'owner': 'airflow'
}

with DAG('dag_bronze',
  start_date = days_ago(2),
  schedule_interval = None,
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
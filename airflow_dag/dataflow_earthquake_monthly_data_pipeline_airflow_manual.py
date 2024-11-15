from airflow import models
from airflow import DAG
import airflow
from datetime import datetime, timedelta
from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator
from airflow.providers.apache.beam.hooks.beam import BeamRunnerType

from airflow.operators.dummy_operator import DummyOperator

default_args = {
    'owner': 'Airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=50),
    'dataflow_default_options': {
        'project': 'spark-learning-43150',
        'region': 'us-central1',
        'runner': 'DataflowRunner'
    }
}

dag = DAG(
    dag_id='dataflow_earthquake_pipeline_monthly_manual',
    default_args=default_args,
    schedule_interval=None,
    start_date=datetime(2024, 10, 4),
    catchup=False,
    description="DAG for data ingestion and transformation"
)

start = DummyOperator(
    task_id="start_task_id",
    dag=dag

)

dataflow_task = BeamRunPythonPipelineOperator(
    task_id="load_raw_data_task",
    dag=dag,
    gcp_conn_id="gcp_connection",
    runner=BeamRunnerType.DataflowRunner,
    py_file="gs://earthquake_analysis_buck/dataflow/dataflow_code/dataflow_earthquake_pipeline_parquet_monthly_withAudit.py",

)

end = DummyOperator(
    task_id="end_task_id",
    dag=dag
)


start >> dataflow_task >> end





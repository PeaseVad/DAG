import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

dag = DAG(
    dag_id = "databricks_run",
    start_date = airflow.utils.dates.days_ago(0),
    schedule_interval=None,
    tags=['azure'],
)

notebook_task_params = {
    'existing_cluster_id': '0422-081121-mixed26',
    'notebook_task': {
        'notebook_path': '/Users/v.pasechnyk@godeltech.com/airflow_test',
        'base_parameters' : {'run_date' : '2021/06/17'},
    },
}
notebook_task = DatabricksSubmitRunOperator(
    task_id = 'notebook_task', 
    json = notebook_task_params,
    dag = dag,
)

notebook_task
import airflow
import requests
import requests.exceptions as requests_exceptions
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.microsoft.azure.operators.wasb_delete_blob import WasbDeleteBlobOperator
from airflow.providers.microsoft.azure.sensors.wasb import WasbBlobSensor

dag = DAG(
    dag_id="databricks_run",
    start_date=airflow.utils.dates.days_ago(0),
    schedule_interval=None,
    tags=['azure'],
)

BLOB_NAME = "load_parametrs/zones.csv"
AZURE_CONTAINER_NAME = "v-pasechnyk"

wait_for_blob = WasbBlobSensor(
    task_id="wait_for_blob",
    wasb_conn_id="wasb_default",
    container_name=AZURE_CONTAINER_NAME,
    blob_name=BLOB_NAME,
)

notebook_task_params = {
    'existing_cluster_id': '0422-081121-mixed26',
    'notebook_task': {
        'notebook_path': '/Users/v.pasechnyk@godeltech.com/Weather-Data-Prep-Airflow',
        'base_parameters': {'run_date': '2021/07/12'},
    },
}
notebook_task = DatabricksSubmitRunOperator(
    task_id='notebook_task',
    json=notebook_task_params,
    dag=dag,
)

remove_param_file = WasbDeleteBlobOperator(
    task_id="delete_param_file",
    wasb_conn_id="wasb_default",
    container_name=AZURE_CONTAINER_NAME,
    blob_name=BLOB_NAME
)

wait_for_blob >> notebook_task >> remove_param_file
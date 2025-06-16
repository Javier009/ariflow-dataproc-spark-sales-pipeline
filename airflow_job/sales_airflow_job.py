from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.google.cloud.operators.gcs import GCSListObjectsOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocSubmitJobOperator,
    DataprocDeleteClusterOperator
)
# from airflow.utils.dates import days_ago

PROJECT_ID = 'airflow-dataproc-project'
RAW_DATA_BUCKET = 'airflow-p1-sales-data'

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'start_date': datetime(2025, 5, 17),
}

dag = DAG(
    'sales_data_DAG',
    default_args=default_args,
    description='A DAG to setup dataproc and run Spark job on that and then delete Cluster',
    schedule_interval='0 0 * * *',
    catchup=False,
    tags=['dev'],
)

# Define cluster config
CLUSTER_NAME = 'sales-cluster' 
PROJECT_ID = 'airflow-dataproc-project'
REGION = 'us-central1'

CLUSTER_CONFIG = {
    'master_config': {
        'num_instances': 1, # Master node
        'machine_type_uri': 'n1-standard-2',  # Machine type
        'disk_config': {
            'boot_disk_type': 'pd-standard',
            'boot_disk_size_gb': 30
        }
    },
    'worker_config': {
        'num_instances': 2,  # Worker nodes
        'machine_type_uri': 'n1-standard-2',  # Machine type
        'disk_config': {
            'boot_disk_type': 'pd-standard',
            'boot_disk_size_gb': 30
        }
    },
    'software_config': {
        'image_version': '2.2.26-debian12'  # Image version
    },
    'gce_cluster_config': {
        'service_account': 'run-project-tasks@airflow-dataproc-project.iam.gserviceaccount.com',
        'subnetwork_uri': 'projects/airflow-dataproc-project/regions/us-central1/subnetworks/default',
    }
}

create_cluster = DataprocCreateClusterOperator(
    task_id='create_dataproc_cluster',
    cluster_name=CLUSTER_NAME,
    project_id=PROJECT_ID,
    region=REGION,
    cluster_config=CLUSTER_CONFIG,
    use_if_exists = True,
    dag=dag,
)
list_files_in_bucket = GCSListObjectsOperator(
        task_id='list_all_files',
        bucket= RAW_DATA_BUCKET, # Replace with your actual bucket name
        prefix=None, # Example: list all files in 'input_data/'
        gcp_conn_id='google_cloud_default',
    )

def push_with_custom_key(ti, source_task_id, custom_key):
        file_list = ti.xcom_pull(task_ids=source_task_id, key='return_value')
        ti.xcom_push(key=custom_key, value=file_list)

store_files_custom_xcom = PythonOperator(
        task_id='store_files_custom_xcom',
        python_callable=push_with_custom_key,
        op_kwargs={
            'source_task_id': 'list_all_files',
            'custom_key': 'GCS_raw_data_bucket'
        },
        provide_context=True,  # needed for Airflow 2.x with PythonOperator
    )

PYSPARK_JOB = {
    "reference": {"project_id": PROJECT_ID},
    "placement": {"cluster_name": CLUSTER_NAME},
    "pyspark_job": {
        "main_python_file_uri": "gs://airflow-p1-source-code/read_and_process_sales_data.py",
        "args": [
            "{{ ti.xcom_pull(task_ids='store_files_custom_xcom', key='GCS_raw_data_bucket') }}"
        ]
        },
}

submit_pyspark_job = DataprocSubmitJobOperator(
    task_id='submit_pyspark_job_on_dataproc',
    job=PYSPARK_JOB,
    region=REGION,
    project_id=PROJECT_ID,
    dag=dag,
)

delete_cluster = DataprocDeleteClusterOperator(
    task_id='delete_dataproc_cluster',
    project_id=PROJECT_ID,
    cluster_name=CLUSTER_NAME,
    region=REGION,
    trigger_rule='all_done',  
    dag=dag,
)

create_cluster >> list_files_in_bucket >> store_files_custom_xcom >> submit_pyspark_job >> delete_cluster

# -- Copy code to Google Cloud Airflow bucket
# gsutil cp /Users/delgadonoriega/Desktop/gcp-data-eng-bootcamp/Module_3_class_2/airflow_project_1/airflow_job/sales_airflow_job.py gs://us-central1-sales-composer--ee14f9b9-bucket/dags/sales_airflow_job.py

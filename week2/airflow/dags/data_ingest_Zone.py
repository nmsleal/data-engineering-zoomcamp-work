import os
import logging

from datetime import datetime

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator

from lib.ingest_scripts import *

dagId = 'Zone_ingestion_gcs_dag'
fileNamePrefix = 'taxi+_zone_lookup'
datasetUrl = 'https://s3.amazonaws.com/nyc-tlc/misc/'
startDate = days_ago(1)
# endDate = datetime(2020, 1, 2)
boolCatchup = True
maxActiveRuns = 3

# https://s3.amazonaws.com/nyc-tlc/trip+data/yellow_tripdata_2020-01.csv
# dagId = 'NYTaxi_ingestion_gcs_dag'
# fileNamePrefix = 'yellow_tripdata'
# datasetUrl = 'https://s3.amazonaws.com/nyc-tlc/trip+data/'

# https://nyc-tlc.s3.amazonaws.com/trip+data/fhv_tripdata_2020-02.csv
# dagId = 'FHV_ingestion_gcs_dag'
# fileNamePrefix = 'fhv_tripdata'
# datasetUrl = 'https://nyc-tlc.s3.amazonaws.com/trip+data/'

# https://s3.amazonaws.com/nyc-tlc/misc/taxi+_zone_lookup.csv

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

# dataset_file = fileNamePrefix + "_{{execution_date.strftime(\'%Y-%m\')}}" + ".csv"
dataset_file = fileNamePrefix + ".csv"
dataset_url = datasetUrl + dataset_file

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")
parquet_file = dataset_file.replace('.csv', '.parquet')
# BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'trips_data_all')

default_args = {
    "owner": "airflow",
    "start_date": startDate,
    # "end_date": endDate,
    "depends_on_past": False,
    "retries": 1
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id=dagId,
    schedule_interval='@once',
    default_args=default_args,
    catchup=boolCatchup,
    max_active_runs=maxActiveRuns,
    tags=['dtc-de'],
) as dag:

    download_dataset_task = BashOperator(
        task_id="download_dataset_task",
        bash_command=f"curl -sSf {dataset_url} > {path_to_local_home}/{dataset_file}"
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{dataset_file}",
        },
    )

    # TODO: Homework - research and try XCOM to communicate output values between 2 tasks/operators
    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{parquet_file}",
            "local_file": f"{path_to_local_home}/{parquet_file}",
        },
    )

    # bigquery_external_table_task = BigQueryCreateExternalTableOperator(
    #     task_id="bigquery_external_table_task",
    #     table_resource={
    #         "tableReference": {
    #             "projectId": PROJECT_ID,
    #             "datasetId": BIGQUERY_DATASET,
    #             "tableId": "external_table",
    #         },
    #         "externalDataConfiguration": {
    #             "sourceFormat": "PARQUET",
    #             "sourceUris": [f"gs://{BUCKET}/raw/{parquet_file}"],
    #         },
    #     },
    # )

    delete_dataset_task = BashOperator(
        task_id="delete_dataset_task",
        bash_command=f"rm {path_to_local_home}/{dataset_file} {path_to_local_home}/{parquet_file}"
    )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> delete_dataset_task
    # download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task >> delete_dataset_task

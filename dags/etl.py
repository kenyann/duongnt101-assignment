from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.utils.trigger_rule import TriggerRule
from airflow.sensors.python import PythonSensor
from airflow.providers.microsoft.azure.transfers.local_to_adls import LocalFilesystemToADLSOperator
from airflow.providers.microsoft.azure.transfers.local_to_wasb import LocalFilesystemToWasbOperator
# nopep8
import sys
sys.path.insert(0, "./")
from src.crawler import Crawler  # nopep8


default_args = {
    "owner": "Duong_Nguy",
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}


@task(task_id="extract")
def extract():
    import pandas as pd
    crawl = Crawler('https://www.dienmayxanh.com/may-lanh#c=2002&o=13&pi=7')
    return crawl.extract()  # have to be json


def transform(df):
    df.to_csv("./duongnt101-assignment/may-lanh.csv", index=False)


@dag(dag_id="etl", start_date=datetime(2024, 5, 9), default_args=default_args, catchup=False)
def pipeline():

    extract_task = PythonOperator(
        task_id="crawl_data",
        python_callable=extract
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        op_kwargs={"df": "{{ti.xcom_pull('extract')}}"},
        python_callable=transform
    )

    # load = LocalFilesystemToADLSOperator(
    #     task_id="upload_task",
    #     local_path="test.csv",
    #     remote_path="https://duongnt101.blob.core.windows.net/dmx/test.csv",
    #     azure_data_lake_conn_id="adls_v2"
    # )

    load_task = LocalFilesystemToWasbOperator(
        task_id="upload_file",
        file_path="./duongnt101-assignment/may-lanh.csv",
        container_name="dmx",
        blob_name="may-lanh.csv",
        wasb_conn_id="abs"
    )
    extract_task >> transform_task >> load_task


pipeline()
# etl()

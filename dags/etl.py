from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from datetime import datetime, timedelta
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


def extract(url):
    crawl = Crawler(url)
    df = crawl.get_data()
    return df


def transform(df):
    df['ProductID'] = df['Name'].apply(lambda name: name.split(' ')[-1])
    return df


def extract_transform(ti):
    df = extract('https://www.dienmayxanh.com/may-lanh#c=2002&o=13&pi=7')
    df = transform(df)
    df_path = 'may-lanh.csv'
    df.to_csv('path', index=False)
    ti.xcom_push(key='df_path', value=df_path)


@dag(dag_id="etl", start_date=datetime(2024, 5, 9), default_args=default_args, catchup=False)
def pipeline():

    extract_transform_task = PythonOperator(
        task_id="extract_transform",
        python_callable=extract_transform
    )
    # load = LocalFilesystemToADLSOperator(
    #     task_id="upload_task",
    #     local_path="test.csv",
    #     remote_path="https://duongnt101.blob.core.windows.net/dmx/test.csv",
    #     azure_data_lake_conn_id="adls_v2"
    # )

    load_task = LocalFilesystemToWasbOperator(
        task_id="upload_file",
        file_path="{{ ti.xcom_pull(key='df_path') }}",
        container_name="dmx",
        blob_name="{{ ti.xcom_pull(key='df_path') }}",
        wasb_conn_id="abs"
    )
    extract_transform_task >> load_task


pipeline()

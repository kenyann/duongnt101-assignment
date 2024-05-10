from dotenv import load_dotenv
import os
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
load_dotenv()


def extract(url):
    crawl = Crawler(url)
    df = crawl.get_data()
    return df


def transform(df):
    product_id_list = df['Name'].values.tolist()
    df['ProductID'] = [product_id[0].split(
        " ")[-1] for product_id in product_id_list]
    return df


def extract_transform(ti):
    df = extract(os.getenv("URL") +
                 os.getenv("FILE_NAME") + os.getenv("SUFFIX"))
    df = transform(df)
    df_path = f'{os.getenv("FILE_NAME")}.csv'
    df.to_csv(df_path, index=False)
    ti.xcom_push(key='df_path', value=df_path)


@dag(dag_id="etl", start_date=datetime(2024, 5, 9), default_args=default_args, catchup=False)
def pipeline():

    extract_transform_task = PythonOperator(
        task_id="extract_transform",
        python_callable=extract_transform
    )

    load_task = LocalFilesystemToWasbOperator(
        task_id="upload_file",
        file_path="{{ ti.xcom_pull(key='df_path') }}",
        blob_name="{{ ti.xcom_pull(key='df_path') }}",
        container_name=os.getenv("FILE_NAME"),
        wasb_conn_id=os.getenv("WASB_CONN_ID")
    )
    extract_transform_task >> load_task


pipeline()

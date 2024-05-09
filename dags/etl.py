from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.decorators import dag, task
from datetime import datetime, timedelta
from airflow.utils.trigger_rule import TriggerRule
from airflow.sensors.python import PythonSensor


# nopep8
import sys
sys.path.insert(0, "./")
from src.crawler import Crawler  # nopep8


default_args = {
    "owner": "Duong_Nguy",
    "retries": 5,
    "retry_delay": timedelta(minutes=5)
}


def _extract():
    import pandas as pd
    crawl = Crawler('https://www.dienmayxanh.com/may-lanh#c=2002&o=13&pi=7')
    result = crawl.extract()
    result.to_csv("test.csv", index=False)


def _transform():
    pass


def _load():
    pass


@dag(dag_id="etl", start_date=datetime(2024, 5, 9), default_args=default_args, catchup=False)
def pipeline():

    extract = PythonOperator(
        task_id="crawl_data",
        python_callable=_extract
    )

    transform = PythonOperator(
        task_id="transform_data",
        python_callable=_transform
    )

    load = PythonOperator(
        task_id="load_data",
        python_callable=_load
    )

    # check_data = check_data_available(PATH)
    # check_data >> [check_data_success, check_data_fail]
    # check_data_success >> run_process >> [
    #     run_process_fail, run_process_success]
    extract >> transform >> load


pipeline()
# etl()

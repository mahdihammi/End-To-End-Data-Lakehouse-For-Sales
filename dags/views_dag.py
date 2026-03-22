import os
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.providers.standard.operators.python import PythonOperator

from datetime import datetime

from include.medall_arch.views import ViewsManager





LOCAL_DUCKDB_CONN_ID = os.environ.get('LOCAL_DUCKDB_CONN_ID')
DUCKLAKE_NAME = "mahdi_ducklake"


views_manager = ViewsManager(
        LOCAL_DUCKDB_CONN_ID= LOCAL_DUCKDB_CONN_ID,
        DUCKLAKE_NAME= DUCKLAKE_NAME
    )


@dag(
    dag_id="views_creation_dag",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
)


def dag_pg():

    create_views = PythonOperator(
        task_id = "create_views",
        python_callable = views_manager.creating_views,
    )

    create_views

dag_pg()








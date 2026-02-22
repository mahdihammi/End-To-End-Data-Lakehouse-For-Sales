import os
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.providers.standard.operators.python import PythonOperator

from datetime import datetime
from include.medall_arch.bronze_layer import BronzeLayerManager

import os
LOCAL_DUCKDB_CONN_ID = os.environ.get('LOCAL_DUCKDB_CONN_ID')
POSTGRES_CONN_ID = os.environ.get('POSTGRES_CONN_ID')


bronze_layer_manager = BronzeLayerManager(
    LOCAL_DUCKDB_CONN_ID= LOCAL_DUCKDB_CONN_ID,
    POSTGRES_CONN_ID = POSTGRES_CONN_ID,
    BRONZE_SCHEMA = 'bronze'
)


@dag(
    dag_id="test_pg",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
)

def dag_pg():

    

    initiate_ducklake = PythonOperator(
        task_id = "initiate_ducklake",
        python_callable = bronze_layer_manager.attach_ducklake
    )

    source_increment_load = PythonOperator(
        task_id = "source_increment_load",
        python_callable = bronze_layer_manager.increment_load_from_pg_to_minio
    )

    initiate_ducklake >> source_increment_load

dag_pg()


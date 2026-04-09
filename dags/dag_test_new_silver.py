import os
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.providers.standard.operators.python import PythonOperator

from datetime import datetime
from include.medall_arch.bronze_layer import BronzeLayerManager
from include.medall_arch.silver_layer_test import SilverLayerManager
from include.medall_arch.gold_layer import GoldTableManager
from dotenv import load_dotenv
import os
import logging
load_dotenv()


LOCAL_DUCKDB_CONN_ID = os.environ.get('LOCAL_DUCKDB_CONN_ID')
POSTGRES_CONN_ID = os.environ.get('POSTGRES_CONN_ID')
SILVER_TABLE_NAME = "orders_silver"

DUCKLAKE_NAME = os.getenv('DUCKLAKE_NAME')

silver_layer_manager = SilverLayerManager(
    LOCAL_DUCKDB_CONN_ID= LOCAL_DUCKDB_CONN_ID,
    SILVER_TABLE_NAME = SILVER_TABLE_NAME,
    DUCKLAKE_NAME = DUCKLAKE_NAME,
    SCHEMA = "silver",
)
@dag(
    dag_id="pipeline_dag_test",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
)

def dag_duckdb():

    silver_layer = PythonOperator(
            task_id = 'Silver_Layer_Manager_TEST',
            python_callable = silver_layer_manager.create_or_update_silver_table
        )
    

    silver_layer

dag_duckdb()
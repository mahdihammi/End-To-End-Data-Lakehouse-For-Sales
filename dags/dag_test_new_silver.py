import os
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from include.medall_arch import gold_layer
from include.medall_arch_test.bronze_layer_test import BronzeLayerManager
from include.medall_arch_test.silver_layer_test import SilverLayerManager
from include.medall_arch_test.gold_layer_test import GoldTableManager
from dotenv import load_dotenv
import os
import logging
load_dotenv()


LOCAL_DUCKDB_CONN_ID = os.environ.get('LOCAL_DUCKDB_CONN_ID')
POSTGRES_CONN_ID = os.environ.get('POSTGRES_CONN_ID')
SILVER_TABLE_NAME = "orders_silver"
BRONZE_TABLE_NAME = "orders_bronze"
DUCKLAKE_NAME = os.getenv('DUCKLAKE_NAME')




@dag(
    dag_id="pipeline_dag_test",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
)

def dag_duckdb():
    bronze_layer_manager = BronzeLayerManager(
        LOCAL_DUCKDB_CONN_ID= LOCAL_DUCKDB_CONN_ID,
        POSTGRES_CONN_ID = POSTGRES_CONN_ID,
        DUCKLAKE_NAME=DUCKLAKE_NAME,
        BRONZE_SCHEMA = 'bronze',
        BRONZE_TABLE_NAME = BRONZE_TABLE_NAME
        
    )


    silver_layer_manager = SilverLayerManager(
        LOCAL_DUCKDB_CONN_ID= LOCAL_DUCKDB_CONN_ID,
        SILVER_TABLE_NAME = SILVER_TABLE_NAME,
        DUCKLAKE_NAME = DUCKLAKE_NAME,
        SCHEMA = "silver",
    )


    gold_layer_manager = GoldTableManager(
        LOCAL_DUCKDB_CONN_ID= LOCAL_DUCKDB_CONN_ID,
        DUCKLAKE_NAME = DUCKLAKE_NAME,
        GOLD_SCHEMA_NAME = "gold",
    )

    incremental_load = PythonOperator(
        task_id = 'incremental_load_srouce_to_MinIO_TEST',
        python_callable = bronze_layer_manager.increment_load_from_pg_to_minio
    )

    bronze_layer = PythonOperator(
        task_id = 'bronze_Layer_Manager_TEST',
        python_callable = bronze_layer_manager.create_or_update_bronze_table
    )

    silver_layer = PythonOperator(
            task_id = 'Silver_Layer_Manager_TEST',
            python_callable = silver_layer_manager.create_or_update_silver_table
        )
    
    with TaskGroup(group_id='Gold_Layer_Manager_TEST') as gold_layer:

        t_customer_360 = PythonOperator(
            task_id='refresh_customer_360',
            python_callable=gold_layer_manager.refresh_customer_360
        )

        t_monthly_trend = PythonOperator(
            task_id='refresh_monthly_trend',
            python_callable=gold_layer_manager.refresh_monthly_trend
    )

        t_product_performance = PythonOperator(
            task_id='refresh_product_performance',
            python_callable=gold_layer_manager.refresh_product_performance
        )   

        t_sales_performance = PythonOperator(
            task_id='refresh_sales_performance',
            python_callable=gold_layer_manager.refresh_sales_performance
        )

        t_regional_performance = PythonOperator(
            task_id='refresh_regional_performance',
            python_callable=gold_layer_manager.refresh_regional_performance
        )

        t_customer_360 >> t_monthly_trend >> t_product_performance >> t_sales_performance >> t_regional_performance
    

    incremental_load >> bronze_layer >> silver_layer >> gold_layer
    

dag_duckdb()
import os
from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
from include.medall_arch.bronze_layer import BronzeLayerManager
from include.medall_arch.silver_layer import SilverLayerManager
from include.medall_arch.gold_layer import GoldTableManager
from dotenv import load_dotenv
import os
import logging
load_dotenv()


LOCAL_DUCKDB_CONN_ID = os.environ.get('LOCAL_DUCKDB_CONN_ID')
POSTGRES_CONN_ID = os.environ.get('POSTGRES_CONN_ID')
SILVER_TABLE_NAME = "orders_silver"
BRONZE_TABLE_NAME = "orders_bronze"
DUCKLAKE_NAME = os.getenv('DUCKLAKE_NAME')


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


@dag(
    dag_id="full_medallion_pipeline_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
)

def dag_duckdb():

    incremental_load = PythonOperator(
        task_id = 'incremental_load_srouce_to_MinIO',
        python_callable = bronze_layer_manager.increment_load_from_pg_to_minio
    )

    bronze_layer = PythonOperator(
        task_id = 'bronze_Layer_Manager',
        python_callable = bronze_layer_manager.create_or_update_bronze_table
    )

    silver_layer = PythonOperator(
            task_id = 'Silver_Layer_Manager',
            python_callable = silver_layer_manager.create_or_update_silver_table
        )
    
    with TaskGroup(group_id='Gold_Layer_Manager') as gold_layer:

        t_customer_360 = PythonOperator( 
            task_id='customer_kpis',
            python_callable=gold_layer_manager.refresh_customer_kpis
        )

        t_monthly_trend = PythonOperator(
            task_id='monthly_trend',
            python_callable=gold_layer_manager.refresh_sales_monthly_kpis
    )

        t_product_performance = PythonOperator(
            task_id='product_performance',
            python_callable=gold_layer_manager.refresh_product_kpis
        )   

        t_sales_performance = PythonOperator(
            task_id='sales_performance',
            python_callable=gold_layer_manager.refresh_sales_summary_kpis
        )

        t_regional_performance = PythonOperator(
            task_id='regional_performance',
            python_callable=gold_layer_manager.refresh_region_kpis
        )

        t_customer_360 >> t_monthly_trend >> t_product_performance >> t_sales_performance >> t_regional_performance
    

    incremental_load >> bronze_layer >> silver_layer >> gold_layer
    

dag_duckdb()
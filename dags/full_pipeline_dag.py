import os
from datetime import datetime

from airflow.decorators import dag
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from dotenv import load_dotenv

from include.medall_arch.bronze_layer import BronzeLayerManager
from include.medall_arch.gold_layer import GoldTableManager
from include.medall_arch.silver_layer import SilverLayerManager

load_dotenv()


LOCAL_DUCKDB_CONN_ID = os.environ.get("LOCAL_DUCKDB_CONN_ID")
POSTGRES_CONN_ID = os.environ.get("POSTGRES_CONN_ID")
SILVER_TABLE_NAME = "orders_silver"
BRONZE_TABLE_NAME = "orders_bronze"
DUCKLAKE_NAME = os.getenv("DUCKLAKE_NAME")


def get_bronze_manager():
    return BronzeLayerManager(
        LOCAL_DUCKDB_CONN_ID=LOCAL_DUCKDB_CONN_ID,
        POSTGRES_CONN_ID=POSTGRES_CONN_ID,
        DUCKLAKE_NAME=DUCKLAKE_NAME,
        BRONZE_SCHEMA="bronze",
        BRONZE_TABLE_NAME=BRONZE_TABLE_NAME,
    )


def get_silver_manager():
    return SilverLayerManager(
        LOCAL_DUCKDB_CONN_ID=LOCAL_DUCKDB_CONN_ID,
        SILVER_TABLE_NAME=SILVER_TABLE_NAME,
        DUCKLAKE_NAME=DUCKLAKE_NAME,
        SCHEMA="silver",
    )


def get_gold_manager():
    return GoldTableManager(
        LOCAL_DUCKDB_CONN_ID=LOCAL_DUCKDB_CONN_ID,
        DUCKLAKE_NAME=DUCKLAKE_NAME,
        GOLD_SCHEMA_NAME="gold",
    )


def incremental_load_task():
    return get_bronze_manager().increment_load_from_pg_to_minio()


def bronze_layer_task():
    return get_bronze_manager().create_or_update_bronze_table()


def silver_layer_task():
    return get_silver_manager().create_or_update_silver_table()


def refresh_customer_kpis_task():
    return get_gold_manager().refresh_customer_kpis()


def refresh_sales_monthly_kpis_task():
    return get_gold_manager().refresh_sales_monthly_kpis()


def refresh_product_kpis_task():
    return get_gold_manager().refresh_product_kpis()


def refresh_sales_summary_kpis_task():
    return get_gold_manager().refresh_sales_summary_kpis()


def refresh_region_kpis_task():
    return get_gold_manager().refresh_region_kpis()


@dag(
    dag_id="full_medallion_pipeline_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
)
def dag_duckdb():

    incremental_load = PythonOperator(
        task_id="incremental_load_source_to_minio",
        python_callable=incremental_load_task,
    )

    bronze_layer = PythonOperator(
        task_id="bronze_layer_manager",
        python_callable=bronze_layer_task,
    )

    silver_layer = PythonOperator(
        task_id="silver_layer_manager",
        python_callable=silver_layer_task,
    )

    with TaskGroup(group_id="Gold_Layer_Manager") as gold_layer:

        t_customer_360 = PythonOperator(
            task_id="customer_kpis",
            python_callable=refresh_customer_kpis_task,
            retries=3,
        )

        t_monthly_trend = PythonOperator(
            task_id="sales_monthly_kpis",
            python_callable=refresh_sales_monthly_kpis_task,
            retries=3,
        )

        t_product_performance = PythonOperator(
            task_id="product_kpis",
            python_callable=refresh_product_kpis_task,
            retries=3,
        )

        t_sales_performance = PythonOperator(
            task_id="sales_summary_kpis",
            python_callable=refresh_sales_summary_kpis_task,
            retries=3,
        )

        t_regional_performance = PythonOperator(
            task_id="regional_performance",
            python_callable=refresh_region_kpis_task,
            retries=3,
        )

    
    incremental_load >> bronze_layer >> silver_layer >> gold_layer


dag_duckdb()

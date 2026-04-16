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

DUCKLAKE_NAME = os.getenv('DUCKLAKE_NAME')

bronze_layer_manager = BronzeLayerManager(
    LOCAL_DUCKDB_CONN_ID= LOCAL_DUCKDB_CONN_ID,
    POSTGRES_CONN_ID = POSTGRES_CONN_ID,
    BRONZE_SCHEMA = 'bronze'
)

silver_layer_manager = SilverLayerManager(
    LOCAL_DUCKDB_CONN_ID= LOCAL_DUCKDB_CONN_ID,
    SILVER_TABLE_NAME = SILVER_TABLE_NAME,
    DUCKLAKE_NAME = DUCKLAKE_NAME,
    SCHEMA = "silver",

)

gold_table_manager = GoldTableManager(
    LOCAL_DUCKDB_CONN_ID = LOCAL_DUCKDB_CONN_ID,
    GOLD_SCHEMA_NAME = 'gold',
    DUCKLAKE_NAME = DUCKLAKE_NAME,

)

@dag(
    dag_id="pipeline_dag",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
)

def dag_pg():

    
    source_increment_load = PythonOperator(
            task_id = "source_increment_load",
            python_callable = bronze_layer_manager.increment_load_from_pg_to_minio
        ) 

    bronze_layer = PythonOperator(
        task_id = "Bronze_layer",
        python_callable = bronze_layer_manager.update_or_insert_bronze_table
    )

    silver_layer = PythonOperator(
        task_id = 'Silver_Layer_Manager',
        python_callable = silver_layer_manager.create_or_update_silver_table
    )

    with TaskGroup(group_id='Gold_Layer_Manager') as gold_layer:
        create_customer_360_table = PythonOperator(
            task_id = 'create_customer_360_gold_table',
            python_callable = gold_table_manager.create_customer_360_table,
            op_kwargs={'table_name': 'customer_360_gold_table'}
        )

        create_monthly_trend_table = PythonOperator(
            task_id = 'create_monthly_trend_gold_table',
            python_callable = gold_table_manager.create_monthly_trend_table,
            op_kwargs={'table_name': 'monthly_trend_gold_table'}
        )

        create_product_performance_table = PythonOperator(
            task_id = "create_product_performance_gold_table",
            python_callable = gold_table_manager.create_product_performance_table,
            op_kwargs={'table_name': 'product_performance_gold_table'}
        )

        create_sales_performance_table = PythonOperator(
            task_id = "create_sales_performance_gold_table",
            python_callable = gold_table_manager.create_sales_performance_table,
            op_kwargs={'table_name': 'sales_performance_gold_table'}
        )

        create_regional_performance_table = PythonOperator(
            task_id = "create_regional_performance_gold_table",
            python_callable = gold_table_manager.create_regional_performance_table,
            op_kwargs={'table_name': 'regional_performance_gold_table'}
        )


        create_customer_360_table >> create_monthly_trend_table >> create_product_performance_table >> create_sales_performance_table >> create_regional_performance_table

    source_increment_load  >> bronze_layer >> silver_layer >> gold_layer

    

dag_pg()


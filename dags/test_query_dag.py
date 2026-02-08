from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.providers.standard.operators.python import PythonOperator

from datetime import datetime

from include.medall_arch.gold_layer import GoldTableManager
from include.medall_arch.silver_layer import SilverLayerManager

@dag(
    dag_id="test_query_dag",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    catchup=False,
    tags=["test", "example"],
)
def test_query_dag():

    @task
    def run_sql_query_silver():
        print("Starting Silver layer...")

    silver_layer_manager = SilverLayerManager(
        LOCAL_DUCKDB_CONN_ID= "my_local_duckdb_conn",
        SCHEMA = "silver",
        SILVER_TABLE_NAME="orders_silver"
    )

    # ----------------------
    # SILVER layer task
    # ----------------------
    silver_layer = PythonOperator(
        task_id="silver_layer_transformation",
        python_callable=silver_layer_manager.create_or_update_silver_table,
    )

    # ----------------------
    # GOLD layer
    # ----------------------
    gold_table_manager = GoldTableManager(
        LOCAL_DUCKDB_CONN_ID="my_local_duckdb_conn",
        GOLD_SCHEMA_NAME="gold",
        SILVER_SCHEMA_NAME="silver"
    )

    with TaskGroup("gold_layer", tooltip="Gold layer sequential tasks") as gold:

        dim_customer = PythonOperator(
            task_id="create_dim_customer",
            python_callable=gold_table_manager.create_dim_customer,
        )
        dim_date = PythonOperator(
            task_id="create_dim_date",
            python_callable=gold_table_manager.create_dim_date,
        )
        dim_location = PythonOperator(
            task_id="create_dim_location",
            python_callable=gold_table_manager.create_dim_location,
        )
        dim_ship_mode = PythonOperator(
            task_id="create_dim_ship_mode",
            python_callable=gold_table_manager.create_dim_ship_mode,
        )
        dim_product = PythonOperator(
            task_id="create_dim_product",
            python_callable=gold_table_manager.create_dim_product,
        )

        # Sequential execution to avoid concurrent writes
        dim_customer >> dim_date >> dim_location >> dim_ship_mode >> dim_product

    # ----------------------
    # FACT table
    # ----------------------
    create_fact_sales = PythonOperator(
        task_id="create_fact_sales",
        python_callable=gold_table_manager.create_fact_sales,
    )

    # ----------------------
    # DAG dependencies
    # ----------------------
    run_sql_query_silver_task = run_sql_query_silver()
    run_sql_query_silver_task >> silver_layer >> gold >> create_fact_sales

# Instantiate DAG
test_query_dag()

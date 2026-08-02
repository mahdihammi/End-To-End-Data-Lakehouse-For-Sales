from __future__ import annotations

from datetime import datetime

from airflow.decorators import dag, task

from include.helpers.ingestion.append import append_to_orders_table


POSTGRES_CONN_ID = "postgres_conn"


@dag(
    dag_id="test_ingestion_orders_from_minio",
    start_date=datetime(2026, 8, 1),
    schedule=None,
    catchup=False,
    tags=["test", "minio", "postgres"],
)
def test_load_orders_from_minio():

    @task
    def test_ingest_orders() -> int:
        return append_to_orders_table(
            postgres_conn_id=POSTGRES_CONN_ID,
            bucket_name="ingest",
            object_name="tunisian_ecommerce_orders_biased.csv",
            target_table="orders",
        )

    test_ingest_orders()


test_load_orders_from_minio()
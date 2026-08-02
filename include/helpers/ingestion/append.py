from __future__ import annotations

from io import BytesIO

import pandas as pd
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import sql
from psycopg2.extras import execute_values

from include.helpers.helper import get_minio_client


######## This file contains helper functions to read a CSV file from MinIO and append its contents to a PostgreSQL table. ########

###### TEST only #############

BUCKET_NAME = "ingest"
OBJECT_NAME = "tunisian_ecommerce_orders_biased.csv"
TARGET_TABLE = "orders"


def read_csv_from_minio(
    bucket_name: str,
    object_name: str,
) -> pd.DataFrame:
    minio_client = get_minio_client()
    response = None

    try:
        response = minio_client.get_object(
            bucket_name=bucket_name,
            object_name=object_name,
        )

        return pd.read_csv(BytesIO(response.read()))

    finally:
        if response is not None:
            response.close()
            response.release_conn()


def append_to_orders_table(
    postgres_conn_id: str,
    bucket_name: str = BUCKET_NAME,
    object_name: str = OBJECT_NAME,
    target_table: str = TARGET_TABLE,
) -> int:
    df = read_csv_from_minio(
        bucket_name=bucket_name,
        object_name=object_name,
    )

    if df.empty:
        print("The CSV file is empty.")
        return 0

    # Convert pandas NaN and NaT values to PostgreSQL NULL.
    rows = [
        tuple(
            None if pd.isna(value) else value
            for value in row
        )
        for row in df.itertuples(index=False, name=None)
    ]

    columns = df.columns.tolist()

    pg_hook = PostgresHook(
        postgres_conn_id=postgres_conn_id,
    )

    connection = pg_hook.get_conn()

    insert_query = sql.SQL(
        """
        INSERT INTO {table} ({columns})
        VALUES %s
        """
    ).format(
        table=sql.Identifier(target_table),
        columns=sql.SQL(", ").join(
            sql.Identifier(column)
            for column in columns
        ),
    )

    try:
        with connection.cursor() as cursor:
            execute_values(
                cursor,
                insert_query.as_string(connection),
                rows,
                page_size=10_000,
            )

        connection.commit()

        print(
            f"Inserted {len(rows)} rows into "
            f"PostgreSQL table {target_table}"
        )

        return len(rows)

    except Exception:
        connection.rollback()
        raise

    finally:
        connection.close()
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Variable
from datetime import datetime
import logging
import pandas as pd
from sqlalchemy import create_engine
from include.helpers.helper import upload_parquet

# ----------------------------
# CONFIG
# ----------------------------
POSTGRES_CONN_ID = "postgres_conn"
TABLE_NAME = "orders"
BUCKET_NAME = "sales-lake"
BRONZE_PREFIX = "bronze/sales"
WATERMARK_VAR = "sales_last_updated_at"




def test_db_query():

    last_ts = Variable.get(WATERMARK_VAR, default_var="1970-01-01 00:00:00")

    request = f"""
        SELECT * 
        FROM orders 
        WHERE updated_at > '{last_ts}'
    """
    pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID) 
    connection = pg_hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    rows = cursor.fetchall()

    if not rows:
        return "no new data to return"
    
    columns = [desc[0] for desc in cursor.description]

    # Convert to DataFrame
    df = pd.DataFrame(rows, columns=columns)

    print(df.shape)

    load_date = datetime.utcnow().strftime("%Y-%m-%d")
    object_name = f"{BRONZE_PREFIX}/load_date={load_date}/sales_{datetime.utcnow().strftime('%H%M%S')}.parquet"

    upload_parquet(df, BUCKET_NAME, object_name)

    new_ts = df["updated_at"].max().strftime("%Y-%m-%d %H:%M:%S")
    Variable.set(WATERMARK_VAR, new_ts)
    print(f"Updated watermark to {new_ts}")


    
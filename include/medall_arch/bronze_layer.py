from airflow.providers.postgres.hooks.postgres import PostgresHook
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from airflow.models import Variable
from datetime import datetime
import logging
import pandas as pd
from sqlalchemy import create_engine
from include.helpers.helper import upload_parquet
from include.helpers.ducklake_init import attach_ducklake_and_set_secrets
from dotenv import load_dotenv
import os


from include.helpers.sql_helper import load_sql

# Load .env file
load_dotenv()
# ----------------------------
# CONFIG
# ----------------------------

TABLE_NAME = "orders"
BUCKET_NAME = "lakehouse-project"
BRONZE_PREFIX = "lakehouse-raw/sales"
WATERMARK_VAR = "sales_last_updated_at"

DBNAME = "postgres"
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

SUPABASE_HOST = os.getenv("SUPABASE_HOST")
SUPABASE_PORT = os.getenv("SUPABASE_PORT")
SUPABASE_USER = os.getenv("SUPABASE_USER")
SUPABASE_PWD = os.getenv("SUPABASE_PWD")
DUCKDB_SECRET = os.getenv('DUCKDB_SECRET')

class BronzeLayerManager:
    def __init__(self, LOCAL_DUCKDB_CONN_ID, POSTGRES_CONN_ID, BRONZE_SCHEMA):
        self.LOCAL_DUCKDB_CONN_ID = LOCAL_DUCKDB_CONN_ID
        self.my_duck_hook = DuckDBHook.get_hook(LOCAL_DUCKDB_CONN_ID)
        self.conn = self.my_duck_hook.get_conn()
        self.pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)

    def increment_load_from_pg_to_minio(self):

        last_ts = Variable.get(WATERMARK_VAR, default_var="1970-01-01 00:00:00")

        request = f"""
            SELECT * 
            FROM orders 
            WHERE updated_at > '{last_ts}'
        """
        connection = self.pg_hook.get_conn()
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


    def update_or_insert_bronze_table(self):
        """
        This method is for updating or inserting to the bronze table with a MERGE query : 
        for idempotency
        """
        
        conn = self.conn


        attach_ducklake_and_set_secrets(
            DBNAME,SUPABASE_HOST,
            SUPABASE_PORT,SUPABASE_USER,
            SUPABASE_PWD, MINIO_ENDPOINT,
            MINIO_ACCESS_KEY,MINIO_SECRET_KEY,
            conn,
            DUCKDB_SECRET
        )
        

        logging.info("loading credentials successfully")

        try:
            logging.info(f"Merge query : \n")

            bronze_query = load_sql('bronze_table.sql')
            conn.execute(bronze_query)
            count = conn.fetchone()[0]

            logging.info(f"Upsert on bronze table succeded, number of rows : {count}")

        
        except Exception as e:

            logging.error(f"Error merging bronze table: {e}")
            raise
        finally:
            conn.close()




        
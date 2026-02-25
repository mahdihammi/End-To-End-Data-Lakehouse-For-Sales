from airflow.providers.postgres.hooks.postgres import PostgresHook
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from airflow.models import Variable
from datetime import datetime
import logging
import pandas as pd
from sqlalchemy import create_engine
from include.helpers.helper import upload_parquet
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

SUPABASE_HOST = os.getenv("SUPABASE_HOST")
SUPABASE_PORT = os.getenv("SUPABASE_PORT")
SUPABASE_USER = os.getenv("SUPABASE_USER")
SUPABASE_PWD = os.getenv("SUPABASE_PWD")

class BronzeLayerManager:
    def __init__(self, LOCAL_DUCKDB_CONN_ID, POSTGRES_CONN_ID, BRONZE_SCHEMA):
        self.LOCAL_DUCKDB_CONN_ID = LOCAL_DUCKDB_CONN_ID
        self.my_duck_hook = DuckDBHook.get_hook(LOCAL_DUCKDB_CONN_ID)
        self.conn = self.my_duck_hook.get_conn()
        self.pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)


    def attach_ducklake(self):
        """
        This fct is for attaching the ducklake when starting duckdb
        """

        conn = self.conn


        conn.execute(f"""
                     
                INSTALL ducklake ;
                INSTALL postgres;

                CREATE OR REPLACE SECRET(
                    TYPE postgres,
                    HOST '{SUPABASE_HOST}',
                    PORT '{SUPABASE_PORT}',
                    DATABASE 'postgres',
                    USER '{SUPABASE_USER}',
                    PASSWORD '{SUPABASE_PWD}'
                     )
        """)

        try:
            conn.execute(
                f"""
                ATTACH 'ducklake:postgres:{DBNAME}=postgres' AS mahdi_ducklake(DATA_PATH 's3://lakehouse-project/')
                """
            )
            logging.info("Success , the ducklake has been attached succesfully")
        except Exception as e:
            logging.error(f"Cannot attach the ducklake instance {e} ")


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




        
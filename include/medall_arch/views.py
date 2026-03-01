from airflow.providers.postgres.hooks.postgres import PostgresHook
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from airflow.models import Variable
from datetime import datetime
import logging
import pandas as pd
from sqlalchemy import create_engine
from include.helpers import ducklake_init
from include.helpers.helper import upload_parquet
from include.helpers.ducklake_init import attach_ducklake_and_set_secrets
from dotenv import load_dotenv
import os


from include.helpers.sql_helper import load_sql

load_dotenv()


TABLE_NAME = "orders"
BUCKET_NAME = "lakehouse-project"
BRONZE_PREFIX = "lakehouse-raw/sales"
WATERMARK_VAR = "sales_last_updated_at"
DUCKLAKE_NAME = "mahdi_ducklake"

DBNAME = "postgres"
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

SUPABASE_HOST = os.getenv("SUPABASE_HOST")
SUPABASE_PORT = os.getenv("SUPABASE_PORT")
SUPABASE_USER = os.getenv("SUPABASE_USER")
SUPABASE_PWD = os.getenv("SUPABASE_PWD")
DUCKDB_SECRET = os.getenv('DUCKDB_SECRET')

def creating_views(LOCAL_DUCKDB_CONN_ID):
    '''
    fct for creating views
    '''

    LOCAL_DUCKDB_CONN_ID = LOCAL_DUCKDB_CONN_ID
    my_duck_hook = DuckDBHook.get_hook(LOCAL_DUCKDB_CONN_ID)
    conn = my_duck_hook.get_conn()

    try:

        attach_ducklake_and_set_secrets(
                DBNAME,SUPABASE_HOST,
                SUPABASE_PORT,SUPABASE_USER,
                SUPABASE_PWD, MINIO_ENDPOINT,
                MINIO_ACCESS_KEY,MINIO_SECRET_KEY,
                conn,
                DUCKDB_SECRET
            )
        logging.info('ducklake attached successfully')
    except Exception as e:

            logging.error(f"Error Attaching ducklake: {e}")
            raise

    try:
        bronze_query = load_sql('views/history_bronze.sql')

        bronze_query = f'''
                            CREATE VIEW IF NOT EXISTS {DUCKLAKE_NAME}.bronze.bronze_view AS \n

                            {bronze_query}
                        '''
        print(bronze_query)

        conn.execute(bronze_query)

        logging.info("bronze view created successfully")


        silver_query = load_sql('views/history_silver_transformation.sql')
        conn.execute(silver_query)

        logging.info('backfill silver view created successfully')

    except Exception as e:

            logging.error(f"Error Attaching ducklake: {e}")
            raise
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
DBNAME = "postgres"
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

SUPABASE_HOST = os.getenv("SUPABASE_HOST")
SUPABASE_PORT = os.getenv("SUPABASE_PORT")
SUPABASE_USER = os.getenv("SUPABASE_USER")
SUPABASE_PWD = os.getenv("SUPABASE_PWD")
DUCKDB_SECRET = os.getenv('DUCKDB_SECRET')



class ViewsManager:
    def __init__(self, LOCAL_DUCKDB_CONN_ID, DUCKLAKE_NAME, force_rebuild = False):
        self.LOCAL_DUCKDB_CONN_ID = LOCAL_DUCKDB_CONN_ID
        self.my_duck_hook = DuckDBHook.get_hook(LOCAL_DUCKDB_CONN_ID)
        self.conn = self.my_duck_hook.get_conn()
        self.DUCKLAKE_NAME = DUCKLAKE_NAME


    def creating_views(self):
            '''
            fct for creating views
            '''

            conn = self.conn

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
            

            logging.info("Initializing lakehouse schemas: bronze, silver, gold")
            schemas = ['bronze', 'silver', 'gold']

            for schema in schemas:
                try:

                    logging.info(f"Creating schema if not exists {schema}")

                    query = f"""
                        CREATE SCHEMA IF NOT EXISTS {self.DUCKLAKE_NAME}.{schema};
                    """

                    conn.execute(query)

                    logging.info(f"Schema {schema} created successfully")

                except Exception as e:
                    logging.error(f"Error creating schema {schema}: {e}")
                    raise


            try:

                history_bronze_query = load_sql('views/history_bronze.sql')

                history_bronze_query = f'''
                                    CREATE OR REPLACE VIEW {self.DUCKLAKE_NAME}.bronze.history_bronze_view AS \n

                                    {history_bronze_query}
                                '''

                conn.execute(history_bronze_query)
                logging.info("history bronze view created successfully")

                history_silver_query = load_sql('views/history_silver_transformation.sql')
                
                history_silver_query = f'''
                                    CREATE OR REPLACE VIEW {self.DUCKLAKE_NAME}.silver.history_silver_view AS \n

                                    {history_silver_query} '''
                
                conn.execute(history_silver_query)

                logging.info('backfill silver view created successfully')


            except Exception as e:

                    logging.error(f"Error Attaching ducklake: {e}")
                    raise
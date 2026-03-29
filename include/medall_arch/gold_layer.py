from datetime import datetime
import logging
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from include.helpers.sql_helper import load_sql
from include.helpers.ducklake_init import attach_ducklake_and_set_secrets
import os

DBNAME = "postgres"
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

SUPABASE_HOST = os.getenv("SUPABASE_HOST")
SUPABASE_PORT = os.getenv("SUPABASE_PORT")
SUPABASE_USER = os.getenv("SUPABASE_USER")
SUPABASE_PWD = os.getenv("SUPABASE_PWD")
DUCKDB_SECRET = os.getenv('DUCKDB_SECRET')


class GoldTableManager:
    
    def __init__(self, LOCAL_DUCKDB_CONN_ID, GOLD_SCHEMA_NAME, DUCKLAKE_NAME):
        self.my_duck_hook = DuckDBHook.get_hook(LOCAL_DUCKDB_CONN_ID)
        self.conn = self.my_duck_hook.get_conn()
        self.LOCAL_DUCKDB_CONN_ID = LOCAL_DUCKDB_CONN_ID
        self.GOLD_SCHEMA_NAME = GOLD_SCHEMA_NAME
        self.DUCKLAKE_NAME = DUCKLAKE_NAME


    def check_table_exists(self, table_name):
        conn = self.conn
        attach_ducklake_and_set_secrets(
            DBNAME, SUPABASE_HOST,
            SUPABASE_PORT, SUPABASE_USER,
            SUPABASE_PWD, MINIO_ENDPOINT,
            MINIO_ACCESS_KEY, MINIO_SECRET_KEY,
            conn,
            DUCKDB_SECRET
        )

        try:
            result = conn.execute(f"""
                SELECT COUNT(*) 
                FROM {self.DUCKLAKE_NAME}.{self.GOLD_SCHEMA_NAME}.{table_name}
            """).fetchone()
            
            return result[0] > 0
        except Exception as e:
            print(f"Error checking table existence: {e}")
            return False

    
    def create_customer_360_table(self, table_name):
        conn = self.conn
        result = self.check_table_exists(table_name)
        try:
            if result:
                logging.info(f'MERGING into {table_name} ...')
                query = load_sql('gold_customer_360.sql')
                conn.execute(query)

                count = conn.fetchone()[0]

                logging.info(f" {count} records affected by MERGE")
            else:
                logging.info(f"table {table_name} doesn't exist, recreating it ")
                history_query = load_sql('views/history_gold_customer_360.sql')
                history_query = f'''
                        CREATE OR REPLACE TABLE {self.DUCKLAKE_NAME}.{self.GOLD_SCHEMA_NAME}.{table_name} AS \n

                        {history_query}
                        '''
                conn.execute(history_query)
                logging.info(f"TABLE {self.DUCKLAKE_NAME}.{self.GOLD_SCHEMA_NAME}.{table_name} created successfully")


        except Exception as e:

            logging.error(f"Error merging {table_name} table: {e}")
            raise


    def create_monthly_trend_table(self, table_name):
        conn = self.conn
        result = self.check_table_exists(table_name)
        try:
            if result:
                logging.info(f'MERGING into {table_name} ...')
                query = load_sql('gold_monthly_trend.sql')
                conn.execute(query)

                count = conn.fetchone()[0]

                logging.info(f" {count} records affected by MERGE")
            else:
                logging.info(f"table {table_name} doesn't exist, recreating it ")
                history_query = load_sql('views/history_gold_monthly_trend.sql')
                history_query = f'''
                        CREATE OR REPLACE TABLE {self.DUCKLAKE_NAME}.{self.GOLD_SCHEMA_NAME}.{table_name} AS \n

                        {history_query}
                        '''
                conn.execute(history_query)
                logging.info(f"TABLE {self.DUCKLAKE_NAME}.{self.GOLD_SCHEMA_NAME}.{table_name} created successfully")


        except Exception as e:

            logging.error(f"Error merging {table_name} table: {e}")
            raise

    def create_product_performance_table(self):
        pass

    def create_sales_performance_table(self):
        pass

    def create_regional_performance_table(self):
        pass
        
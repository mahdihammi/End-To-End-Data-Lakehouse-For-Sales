from datetime import datetime
from json import load
import pandas as pd
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from include.helpers.sql_helper import load_sql
from include.helpers.ducklake_init import attach_ducklake_and_set_secrets

from dotenv import load_dotenv

import logging
import os

load_dotenv()


DBNAME = "postgres"
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

SUPABASE_HOST = os.getenv("SUPABASE_HOST")
SUPABASE_PORT = os.getenv("SUPABASE_PORT")
SUPABASE_USER = os.getenv("SUPABASE_USER")
SUPABASE_PWD = os.getenv("SUPABASE_PWD")
DUCKDB_SECRET = os.getenv('DUCKDB_SECRET')



class SilverLayerManager:
    def __init__(self, LOCAL_DUCKDB_CONN_ID, SILVER_TABLE_NAME, SCHEMA, force_rebuild = False):
        self.LOCAL_DUCKDB_CONN_ID = LOCAL_DUCKDB_CONN_ID
        self.my_duck_hook = DuckDBHook.get_hook(LOCAL_DUCKDB_CONN_ID)
        self.conn = self.my_duck_hook.get_conn()
        self.SILVER_TABLE_NAME = SILVER_TABLE_NAME
        self.SCHEMA = SCHEMA
        self.force_rebuild = force_rebuild


    def check_silver_table_exists(self, conn):
        try:
            result = conn.execute(f"""
                SELECT COUNT(*) 
                FROM mahdi_ducklake.silver.{self.SILVER_TABLE_NAME}
            """).fetchone()
            
            return result[0] > 0
        except Exception as e:
            print(f"Error checking table existence: {e}")
            return False

    
    def create_or_update_silver_table(self):
        """
        This function is to create the silver table from the sql file .sql

        using MERGE statement with incremental loading
        
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

        table_exists = self.check_silver_table_exists(conn)

        # igonore it for now 

        # if self.force_rebuild == True:
        #     logging.info(f"rebuilding the silver layer ")

        #     backfill_silver_query = load_sql('history_views/history_transformation.sql')
        #     conn.execute(backfill_silver_query)

        #     return "Silver layer backfilled"
        

        try:
            if table_exists:

                logging.info(f'table {self.SILVER_TABLE_NAME} exists, Merging table with MERGE query')

                query = load_sql('silver_transformation.sql')

                result = conn.execute(query)

                logging.info(f"{result.rowcount} records affected by MERGE")

            else:
                logging.info(f"table {self.SILVER_TABLE_NAME} doesn't exist, recreating it ")
                backfill_silver_query = load_sql('history_views/history_transformation.sql')
                backfill_silver_query = f'''
                        CREATE OR REPLACE TABLE {self.SILVER_TABLE_NAME} AS \n

                        {backfill_silver_query}
                    '''
                conn.execute(backfill_silver_query)

        
        except Exception as e:

            logging.error(f"Error merging silver table: {e}")
            raise

        

        
            



    
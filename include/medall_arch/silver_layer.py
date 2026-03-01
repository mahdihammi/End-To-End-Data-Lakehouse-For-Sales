from datetime import datetime
from json import load
import pandas as pd
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from include.helpers.sql_helper import load_sql
import logging


class SilverLayerManager:
    def __init__(self, LOCAL_DUCKDB_CONN_ID, SILVER_TABLE_NAME, SCHEMA, force_rebuild = False):
        self.LOCAL_DUCKDB_CONN_ID = LOCAL_DUCKDB_CONN_ID
        self.my_duck_hook = DuckDBHook.get_hook(LOCAL_DUCKDB_CONN_ID)
        self.conn = self.my_duck_hook.get_conn()
        self.SILVER_TABLE_NAME = SILVER_TABLE_NAME
        self.SCHEMA = SCHEMA
        self.force_rebuild = force_rebuild


    def check_silver_table_exists(self):

        conn = self.conn
        try:
            result = conn.execute(f"""
                SELECT COUNT(*) 
                FROM mahdi_ducklake.silver.orders_silver
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

        table_exists = self.check_silver_table_exists()

        if self.force_rebuild == True:
            logging.info(f"rebuilding the silver layer ")

            backfill_silver_query = load_sql('history_views/history_transformation.sql')
            conn.execute(backfill_silver_query)

            return "Silver layer backfilled"
        

        
            



    
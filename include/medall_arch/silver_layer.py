from datetime import datetime
from json import load
import pandas as pd
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from include.helpers.sql_helper import load_sql
import logging


class SilverLayerManager:
    def __init__(self, LOCAL_DUCKDB_CONN_ID, SILVER_TABLE_NAME, SCHEMA):
        self.LOCAL_DUCKDB_CONN_ID = LOCAL_DUCKDB_CONN_ID
        self.my_duck_hook = DuckDBHook.get_hook(LOCAL_DUCKDB_CONN_ID)
        self.conn = self.my_duck_hook.get_conn()
        self.SILVER_TABLE_NAME = SILVER_TABLE_NAME
        self.SCHEMA = SCHEMA


    def check_silver_table_exists(self):

        conn = self.conn
        try:
            result = conn.execute(f"""
                SELECT COUNT(*) 
                FROM information_schema.tables 
                WHERE table_schema = '{self.SCHEMA}' AND table_name = '{self.SILVER_TABLE_NAME}'
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
        logging.info("******************************************")
        logging.info(f"Creating silver table {self.SILVER_TABLE_NAME}")

        if self.check_silver_table_exists():

            query = load_sql("silver_transformation.sql")

            logging.info("Executing silver incremental MERGE")
            print(query)

            conn.execute(query)
        else:
            logging.info("table does not exist, backfilling data ")
            backfill_query = load_sql("history_views/history_silver_transformation.sql")
            logging.info("executing query backfill")
            conn.execute(backfill_query)
            



    
from datetime import datetime
from json import load
import pandas as pd
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from include.helpers.sql_helper import load_sql
from include.helpers.ducklake_init import attach_ducklake_and_set_secrets
from include.medall_arch.base import BaseLayerManager
from dotenv import load_dotenv

import logging
import os

load_dotenv()


class SilverLayerManager(BaseLayerManager):
    def __init__(self, LOCAL_DUCKDB_CONN_ID, SILVER_TABLE_NAME,DUCKLAKE_NAME, SCHEMA):
        super().__init__(LOCAL_DUCKDB_CONN_ID)
        self.SILVER_TABLE_NAME = SILVER_TABLE_NAME
        self.SCHEMA = SCHEMA
        self.DUCKLAKE_NAME = DUCKLAKE_NAME


    def check_silver_table_exists(self, conn):
        try:
            result = conn.execute(f"""
                SELECT COUNT(*) 
                FROM {self.DUCKLAKE_NAME}.{self.SCHEMA}.{self.SILVER_TABLE_NAME}
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

        self.attach_ducklake()

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

                conn.execute(query)

                count = conn.fetchone()[0]

                logging.info(f" {count} records affected by MERGE")

            else:
                logging.info(f"table {self.SILVER_TABLE_NAME} doesn't exist, recreating it ")
                backfill_silver_query = load_sql('views/history_silver_transformation.sql')
                logging.info(f"DUCKLAKE_NAME loaded: {self.DUCKLAKE_NAME}")
                backfill_silver_query = f'''
                        CREATE OR REPLACE TABLE {self.DUCKLAKE_NAME}.{self.SCHEMA}.{self.SILVER_TABLE_NAME} AS \n

                        {backfill_silver_query}
                    '''
                logging.info(f"{backfill_silver_query}")


                conn.execute(backfill_silver_query)
                logging.info(f"TABLE {self.DUCKLAKE_NAME}.{self.SCHEMA}.{self.SILVER_TABLE_NAME} created successfully")

        
        except Exception as e:

            logging.error(f"Error merging silver table: {e}")
            raise

        

        
            



    
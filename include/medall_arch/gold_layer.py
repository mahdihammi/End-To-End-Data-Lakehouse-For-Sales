from datetime import datetime
import logging
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from include.helpers.sql_helper import load_sql
from include.helpers.ducklake_init import attach_ducklake_and_set_secrets
from include.medall_arch.base import BaseLayerManager
import os



class GoldTableManager(BaseLayerManager):
    
    def __init__(self, LOCAL_DUCKDB_CONN_ID, GOLD_SCHEMA_NAME, DUCKLAKE_NAME):
        super().__init__(LOCAL_DUCKDB_CONN_ID)
        self.GOLD_SCHEMA_NAME = GOLD_SCHEMA_NAME
        self.DUCKLAKE_NAME = DUCKLAKE_NAME


    def check_table_exists(self, table_name):
        conn = self.conn
        self.attach_ducklake()

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

    def create_product_performance_table(self, table_name):
        conn = self.conn
        result = self.check_table_exists(table_name)
        try:
            if result:
                logging.info(f'MERGING into {table_name} ...')
                query = load_sql('gold_product_performance.sql')
                conn.execute(query)

                count = conn.fetchone()[0]

                logging.info(f" {count} records affected by MERGE")
            else:
                logging.info(f"table {table_name} doesn't exist, recreating it ")
                history_query = load_sql('views/history_gold_product_performance.sql')
                history_query = f'''
                        CREATE OR REPLACE TABLE {self.DUCKLAKE_NAME}.{self.GOLD_SCHEMA_NAME}.{table_name} AS \n

                        {history_query}
                        '''
                conn.execute(history_query)
                logging.info(f"TABLE {self.DUCKLAKE_NAME}.{self.GOLD_SCHEMA_NAME}.{table_name} created successfully")
        except Exception as e:

            logging.error(f"Error merging {table_name} table: {e}")
            raise

    def create_sales_performance_table(self, table_name):
        conn = self.conn
        result = self.check_table_exists(table_name)
        try:
            if result:
                logging.info(f'MERGING into {table_name} ...')
                query = load_sql('gold_sales_performance.sql')
                conn.execute(query)

                count = conn.fetchone()[0]

                logging.info(f" {count} records affected by MERGE")
            else:
                logging.info(f"table {table_name} doesn't exist, recreating it ")
                history_query = load_sql('views/history_gold_sales_performance.sql')
                history_query = f'''
                        CREATE OR REPLACE TABLE {self.DUCKLAKE_NAME}.{self.GOLD_SCHEMA_NAME}.{table_name} AS \n

                        {history_query}
                        '''
                conn.execute(history_query)
                logging.info(f"TABLE {self.DUCKLAKE_NAME}.{self.GOLD_SCHEMA_NAME}.{table_name} created successfully")
        except Exception as e:

            logging.error(f"Error merging {table_name} table: {e}")
            raise

    def create_regional_performance_table(self, table_name):
        conn = self.conn
        result = self.check_table_exists(table_name)
        try:
            if result:
                logging.info(f'MERGING into {table_name} ...')
                query = load_sql('gold_regional_performance.sql')
                conn.execute(query)

                count = conn.fetchone()[0]

                logging.info(f" {count} records affected by MERGE")
            else:
                logging.info(f"table {table_name} doesn't exist, recreating it ")
                history_query = load_sql('views/history_gold_regional_performance.sql')
                history_query = f'''
                        CREATE OR REPLACE TABLE {self.DUCKLAKE_NAME}.{self.GOLD_SCHEMA_NAME}.{table_name} AS \n

                        {history_query}
                        '''
                conn.execute(history_query)
                logging.info(f"TABLE {self.DUCKLAKE_NAME}.{self.GOLD_SCHEMA_NAME}.{table_name} created successfully")
        except Exception as e:

            logging.error(f"Error merging {table_name} table: {e}")
            raise
        
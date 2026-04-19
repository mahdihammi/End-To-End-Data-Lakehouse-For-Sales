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

    def _refresh_table(self, table_name: str, history_sql: str):
        conn = self.conn
        self.attach_ducklake()

        try:
            query = load_sql(history_sql)
            logging.info(f"Refreshing {table_name}...")
            conn.execute(f"""
                CREATE OR REPLACE TABLE {self.DUCKLAKE_NAME}.{self.GOLD_SCHEMA_NAME}.{table_name} AS
                {query}
            """)
            logging.info(f"{table_name} refreshed successfully")

        except Exception as e:
            logging.error(f"Error refreshing {table_name}: {e}")
            raise

    def refresh_customer_360(self):
        self._refresh_table("customer_360_gold_table", "gold_queries/history_gold_customer_360.sql")

    def refresh_monthly_trend(self):
        self._refresh_table("monthly_trend_gold_table", "gold_queries/history_gold_monthly_trend.sql")

    def refresh_product_performance(self):
        self._refresh_table("product_performance_gold_table", "gold_queries/history_gold_product_performance.sql")

    def refresh_sales_performance(self):
        self._refresh_table("sales_performance_gold_table", "gold_queries/history_gold_sales_performance.sql")

    def refresh_regional_performance(self):
        self._refresh_table("regional_performance_gold_table", "gold_queries/history_gold_regional_performance.sql")
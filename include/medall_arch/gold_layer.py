from datetime import datetime
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from include.helpers.sql_helper import load_sql


class GoldTableManager:
    
    def __init__(self, LOCAL_DUCKDB_CONN_ID, GOLD_SCHEMA_NAME, SILVER_SCHEMA_NAME):
        self.my_duck_hook = DuckDBHook.get_hook(LOCAL_DUCKDB_CONN_ID)
        self.conn = self.my_duck_hook.get_conn()
        self.LOCAL_DUCKDB_CONN_ID = LOCAL_DUCKDB_CONN_ID
        self.GOLD_SCHEMA_NAME = GOLD_SCHEMA_NAME
        self.SILVER_SCHEMA_NAME = SILVER_SCHEMA_NAME

    
    def create_customer_360_table(self):
        pass

    def create_monthly_trend_table(self):
        pass

    def create_product_performance_table(self):
        pass

    def create_sales_performance_table(self):
        pass

    def create_regional_performance_table(self):
        pass
        
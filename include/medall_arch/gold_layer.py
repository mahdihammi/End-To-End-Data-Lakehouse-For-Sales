import logging

from include.helpers.sql_helper import load_sql
from include.medall_arch.base import BaseLayerManager


class GoldTableManager(BaseLayerManager):

    def __init__(self, LOCAL_DUCKDB_CONN_ID, GOLD_SCHEMA_NAME, DUCKLAKE_NAME):
        super().__init__(LOCAL_DUCKDB_CONN_ID, DUCKLAKE_NAME)
        self.GOLD_SCHEMA_NAME = GOLD_SCHEMA_NAME
        self.DUCKLAKE_NAME = DUCKLAKE_NAME

    def _refresh_table(self, table_name: str, history_sql: str):
        query = load_sql(history_sql)

        try:
            with self.ducklake_connection() as conn:
                logging.info("Refreshing %s...", table_name)

                conn.execute("BEGIN;")
                try:
                    conn.execute(f"""
                        CREATE OR REPLACE TABLE {self.DUCKLAKE_NAME}.{self.GOLD_SCHEMA_NAME}.{table_name} AS
                        {query}
                    """)
                    conn.execute("COMMIT;")
                except Exception:
                    conn.execute("ROLLBACK;")
                    raise

                logging.info("%s refreshed successfully", table_name)

        except Exception as e:
            logging.error("Error refreshing %s: %s", table_name, e)
            raise

    def refresh_customer_kpis(self):
        self._refresh_table("customer_kpis", "gold_queries/history_gold_customer_360.sql")

    def refresh_sales_monthly_kpis(self):
        self._refresh_table("sales_monthly_kpis", "gold_queries/history_gold_monthly_trend.sql")

    def refresh_product_kpis(self):
        self._refresh_table("product_kpis", "gold_queries/history_gold_product_performance.sql")

    def refresh_sales_summary_kpis(self):
        self._refresh_table("sales_summary_kpis", "gold_queries/history_gold_sales_performance.sql")

    def refresh_region_kpis(self):
        self._refresh_table("region_kpis", "gold_queries/history_gold_regional_performance.sql")

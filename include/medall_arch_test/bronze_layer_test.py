from airflow.providers.postgres.hooks.postgres import PostgresHook
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from airflow.models import Variable
from datetime import datetime
import logging
import pandas as pd
from sqlalchemy import create_engine
from include.helpers.helper import upload_parquet
from include.helpers.ducklake_init import attach_ducklake_and_set_secrets
from dotenv import load_dotenv
from include.medall_arch.base import BaseLayerManager
import os


from include.helpers.sql_helper import load_sql

# Load .env file
load_dotenv()
# ----------------------------
# CONFIG
# ----------------------------

BUCKET_NAME = "lakehouse-project"
BRONZE_PREFIX = "lakehouse-raw/sales"
WATERMARK_VAR = "sales_last_updated_at"

class BronzeLayerManager(BaseLayerManager):
    def __init__(self, LOCAL_DUCKDB_CONN_ID, POSTGRES_CONN_ID, DUCKLAKE_NAME, BRONZE_SCHEMA,BRONZE_TABLE_NAME):
        super().__init__(LOCAL_DUCKDB_CONN_ID)
        self.pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        self.BRONZE_TABLE_NAME = BRONZE_TABLE_NAME
        self.BRONZE_SCHEMA = BRONZE_SCHEMA
        self.DUCKLAKE_NAME = DUCKLAKE_NAME
    

    @property
    def full_table_name(self):
        return f"{self.DUCKLAKE_NAME}.{self.BRONZE_SCHEMA}.{self.BRONZE_TABLE_NAME}"
    
    def _table_exists(self, conn) -> bool:
        """
        Lightweight check — just probes reachability, no full scan.
        """
        try:
            conn.execute(f"SELECT 1 FROM {self.full_table_name} LIMIT 1")
            return True
        except Exception:
            return False

    def increment_load_from_pg_to_minio(self):

        last_ts = Variable.get(WATERMARK_VAR, default_var="1970-01-01 00:00:00")

        request = f"""
            SELECT * 
            FROM orders 
            WHERE updated_at > '{last_ts}'
        """
        connection = self.pg_hook.get_conn()
        cursor = connection.cursor()
        cursor.execute(request)
        rows = cursor.fetchall()

        if not rows:
            return "no new data to return"
        
        columns = [desc[0] for desc in cursor.description]

        # Convert to DataFrame
        df = pd.DataFrame(rows, columns=columns)

        print(df.shape)

        load_date = datetime.utcnow().strftime("%Y-%m-%d")
        object_name = f"{BRONZE_PREFIX}/load_date={load_date}/sales_{datetime.utcnow().strftime('%H%M%S')}.parquet"

        upload_parquet(df, BUCKET_NAME, object_name)

        new_ts = df["updated_at"].max().strftime("%Y-%m-%d %H:%M:%S")
        Variable.set(WATERMARK_VAR, new_ts)
        print(f"Updated watermark to {new_ts}")


    def setup_bronze_table(self, conn):
        """
        DDL step — only runs when the table is missing.
        Creates the table and sets partitioning by (order_year, order_month).
        No-op if the table already exists.
        """
        if self._table_exists(conn):
            logging.info(f"{self.full_table_name} already exists, skipping DDL")
            return

        logging.info(f"{self.full_table_name} not found, running DDL setup")

        ddl_query = load_sql('ddl/bronze_ddl.sql', full_table_name=self.full_table_name)

        # DuckDB doesn't support multi-statement execute,
        # so split on ';' and run each statement individually
        statements = [s.strip() for s in ddl_query.split(';') if s.strip()]
        for stmt in statements:
            conn.execute(stmt)

        logging.info(f"DDL setup complete for {self.full_table_name}")


    def merge_bronze_table(self, conn):
        """
        Incremental MERGE step.
        On a freshly created table this acts as a full initial load.
        On an existing table it only processes new/updated records.
        """
        logging.info(f"Running MERGE into {self.full_table_name}")

        query = load_sql('bronze_table.sql')
        conn.execute(query)

        logging.info(f"MERGE complete on {self.full_table_name}")

    def create_or_update_bronze_table(self):
        """
        Main entry point called by the Airflow task.

        Flow:
            1. setup_silver_table  — DDL only if table is missing
            2. merge_silver_table  — incremental MERGE always runs
        """
        conn = self.conn
        self.attach_ducklake()

        try:
            self.setup_bronze_table(conn)
            self.merge_bronze_table(conn)

        except Exception as e:
            logging.error(f"Error in bronze layer pipeline: {e}")
            raise


        
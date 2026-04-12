from datetime import datetime
from json import load
import pandas as pd
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from include.helpers.sql_helper import load_sql
from include.medall_arch.base import BaseLayerManager
from dotenv import load_dotenv

import logging
import os

load_dotenv()

class SilverLayerManager(BaseLayerManager):
    def __init__(self, LOCAL_DUCKDB_CONN_ID, SILVER_TABLE_NAME, DUCKLAKE_NAME, SCHEMA):
        super().__init__(LOCAL_DUCKDB_CONN_ID)
        self.SILVER_TABLE_NAME = SILVER_TABLE_NAME
        self.SCHEMA = SCHEMA
        self.DUCKLAKE_NAME = DUCKLAKE_NAME

    @property
    def full_table_name(self):
        return f"{self.DUCKLAKE_NAME}.{self.SCHEMA}.{self.SILVER_TABLE_NAME}"

    def _table_exists(self, conn) -> bool:
        """
        Lightweight check — just probes reachability, no full scan.
        """
        try:
            conn.execute(f"SELECT 1 FROM {self.full_table_name} LIMIT 1")
            return True
        except Exception:
            return False

    def setup_silver_table(self, conn):
        """
        DDL step — only runs when the table is missing.
        Creates the table and sets partitioning by (order_year, order_month).
        No-op if the table already exists.
        """
        if self._table_exists(conn):
            logging.info(f"{self.full_table_name} already exists, skipping DDL")
            return

        logging.info(f"{self.full_table_name} not found, running DDL setup")

        ddl_query = load_sql('ddl/silver_ddl.sql', full_table_name=self.full_table_name)

        # DuckDB doesn't support multi-statement execute,
        # so split on ';' and run each statement individually
        statements = [s.strip() for s in ddl_query.split(';') if s.strip()]
        for stmt in statements:
            conn.execute(stmt)

        logging.info(f"DDL setup complete for {self.full_table_name}")

    def merge_silver_table(self, conn):
        """
        Incremental MERGE step.
        On a freshly created table this acts as a full initial load.
        On an existing table it only processes new/updated records.
        """
        logging.info(f"Running MERGE into {self.full_table_name}")

        query = load_sql('silver_transformation.sql')
        conn.execute(query)
        count = conn.fetchone()[0]

        logging.info(f"MERGE complete on {self.full_table_name}")
        logging.info(f"Upsert on silver table succeded, number of rows : {count}")

    def create_or_update_silver_table(self):
        """
        Main entry point called by the Airflow task.

        Flow:
            1. setup_silver_table  — DDL only if table is missing
            2. merge_silver_table  — incremental MERGE always runs
        """
        conn = self.conn
        self.attach_ducklake()

        try:
            self.setup_silver_table(conn)
            self.merge_silver_table(conn)

        except Exception as e:
            logging.error(f"Error in silver layer pipeline: {e}")
            raise
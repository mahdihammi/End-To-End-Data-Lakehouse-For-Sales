import os
from contextlib import contextmanager

import duckdb

from include.helpers.ducklake_init import attach_ducklake_and_set_secrets


class BaseLayerManager:
    def __init__(self, LOCAL_DUCKDB_CONN_ID: str | None = None, DUCKLAKE_NAME: str | None = None):
        # Kept only for backward compatibility with the current constructors.
        # Do not open DuckDBHook here; Airflow parallel tasks must not share a local .duckdb file.
        self.LOCAL_DUCKDB_CONN_ID = LOCAL_DUCKDB_CONN_ID

        self.minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        self.minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")

        self.supabase_host = os.getenv("SUPABASE_HOST")
        self.supabase_port = os.getenv("SUPABASE_PORT")
        self.supabase_user = os.getenv("SUPABASE_USER")
        self.supabase_pwd = os.getenv("SUPABASE_PWD")

        self.duckdb_secret = os.getenv("DUCKDB_SECRET", "__default_postgres")
        self.dbname = os.getenv("DBNAME", "postgres")
        self.ducklake_name = DUCKLAKE_NAME or os.getenv("DUCKLAKE_NAME") or "mahdi_ducklake"
        self.data_path = os.getenv("DATA_PATH", "s3://lakehouse-project/")

    @contextmanager
    def ducklake_connection(self):
        """
        Open a fresh in-memory DuckDB connection for a single Airflow task.

        DuckLake state stays in Supabase/Postgres + MinIO, so the local DuckDB
        engine can be temporary. This avoids IO lock errors on a shared .duckdb file.
        """
        conn = duckdb.connect(database=":memory:")
        try:
            attach_ducklake_and_set_secrets(
                conn=conn,
                dbname=self.dbname,
                supabase_host=self.supabase_host,
                supabase_port=self.supabase_port,
                supabase_user=self.supabase_user,
                supabase_pwd=self.supabase_pwd,
                minio_endpoint=self.minio_endpoint,
                minio_access_key=self.minio_access_key,
                minio_secret_key=self.minio_secret_key,
                ducklake_name=self.ducklake_name,
                data_path=self.data_path,
                postgres_secret_name=self.duckdb_secret,
            )
            yield conn
        finally:
            conn.close()

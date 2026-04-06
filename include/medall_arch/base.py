import os
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from include.helpers.ducklake_init import attach_ducklake_and_set_secrets


class BaseLayerManager:
    def __init__(self, LOCAL_DUCKDB_CONN_ID: str):
        self.LOCAL_DUCKDB_CONN_ID = LOCAL_DUCKDB_CONN_ID
        self.my_duck_hook = DuckDBHook.get_hook(LOCAL_DUCKDB_CONN_ID)
        self.conn = self.my_duck_hook.get_conn()

        self.minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        self.minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
        self.supabase_host = os.getenv("SUPABASE_HOST")
        self.supabase_port = os.getenv("SUPABASE_PORT")
        self.supabase_user = os.getenv("SUPABASE_USER")
        self.supabase_pwd = os.getenv("SUPABASE_PWD")
        self.duckdb_secret = os.getenv("DUCKDB_SECRET")
        self.dbname = os.getenv('DBNAME')

    def attach_ducklake(self):
            attach_ducklake_and_set_secrets(
                self.dbname,
                self.supabase_host,
                self.supabase_port,
                self.supabase_user,
                self.supabase_pwd,
                self.minio_endpoint,
                self.minio_access_key,
                self.minio_secret_key,
                self.conn,
                self.duckdb_secret
            )
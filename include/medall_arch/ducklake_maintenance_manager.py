import logging
import os
import re
from collections.abc import Iterable

import duckdb

from include.helpers.ducklake_init import attach_ducklake_and_set_secrets


class DuckLakeMaintenanceManager:
    _VALID_IDENTIFIER = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")

    def __init__(
        self,
        ducklake_name: str,
        snapshot_retention: str = "1 day",
        file_retention: str = "1 hour",
    ) -> None:
        self.ducklake_name = ducklake_name
        self.snapshot_retention = snapshot_retention
        self.file_retention = file_retention

        self.minio_endpoint = os.getenv("MINIO_ENDPOINT", "minio:9000")
        self.minio_access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
        self.minio_secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin")
        self.supabase_host = os.getenv("SUPABASE_HOST")
        self.supabase_port = os.getenv("SUPABASE_PORT")
        self.supabase_user = os.getenv("SUPABASE_USER")
        self.supabase_pwd = os.getenv("SUPABASE_PWD")
        self.duckdb_secret = os.getenv("DUCKDB_SECRET")
        self.dbname = os.getenv("DBNAME")

        self._validate_identifier(self.ducklake_name)

        self.conn = duckdb.connect(database=":memory:")

    def _validate_identifier(self, value: str) -> None:
        if not self._VALID_IDENTIFIER.match(value):
            raise ValueError(f"Invalid SQL identifier: {value}")

    def attach_ducklake(self) -> None:
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
            self.duckdb_secret,
        )

    def close(self) -> None:
        if self.conn is not None:
            self.conn.close()
            self.conn = None

    def _execute_and_log(self, sql: str, operation_name: str) -> None:
        logging.info("Starting DuckLake maintenance operation: %s", operation_name)

        rows = self.conn.execute(sql).fetchall()

        if not rows:
            logging.info("%s completed with no returned rows", operation_name)
            return

        logging.info("%s returned %s row(s)", operation_name, len(rows))

        for row in rows[:50]:
            logging.info("%s result row: %s", operation_name, row)

        if len(rows) > 50:
            logging.info("%s result truncated after 50 rows", operation_name)

    def configure_retention(self) -> None:
        self.attach_ducklake()

        logging.info(
            "Configuring DuckLake retention: snapshots=%s, files=%s",
            self.snapshot_retention,
            self.file_retention,
        )

        self.conn.execute(
            f"CALL {self.ducklake_name}.set_option("
            f"'expire_older_than', '{self.snapshot_retention}'"
            f")"
        )

        self.conn.execute(
            f"CALL {self.ducklake_name}.set_option("
            f"'delete_older_than', '{self.file_retention}'"
            f")"
        )

    def drop_tables(self, schema_name: str, table_names: Iterable[str]) -> None:
        self.attach_ducklake()
        self._validate_identifier(schema_name)

        for table_name in table_names:
            self._validate_identifier(table_name)

            full_table_name = f"{self.ducklake_name}.{schema_name}.{table_name}"
            logging.info("Dropping table if exists: %s", full_table_name)
            self.conn.execute(f"DROP TABLE IF EXISTS {full_table_name}")

    def expire_snapshots(self, dry_run: bool = True) -> None:
        self.attach_ducklake()

        sql = f"""
            CALL ducklake_expire_snapshots(
                '{self.ducklake_name}',
                dry_run => {str(dry_run).lower()},
                older_than => now() - INTERVAL '{self.snapshot_retention}'
            )
        """

        self._execute_and_log(
            sql=sql,
            operation_name=f"ducklake_expire_snapshots(dry_run={dry_run})",
        )

    def cleanup_old_files(self, dry_run: bool = True) -> None:
        self.attach_ducklake()

        sql = f"""
            CALL ducklake_cleanup_old_files(
                '{self.ducklake_name}',
                dry_run => {str(dry_run).lower()},
                older_than => now() - INTERVAL '{self.file_retention}'
            )
        """

        self._execute_and_log(
            sql=sql,
            operation_name=f"ducklake_cleanup_old_files(dry_run={dry_run})",
        )

    def delete_orphaned_files(self, dry_run: bool = True) -> None:
        self.attach_ducklake()

        sql = f"""
            CALL ducklake_delete_orphaned_files(
                '{self.ducklake_name}',
                dry_run => {str(dry_run).lower()},
                older_than => now() - INTERVAL '{self.file_retention}'
            )
        """

        self._execute_and_log(
            sql=sql,
            operation_name=f"ducklake_delete_orphaned_files(dry_run={dry_run})",
        )

    def checkpoint(self) -> None:
        self.attach_ducklake()

        logging.info("Running DuckLake CHECKPOINT")
        self.conn.execute("CHECKPOINT")
        logging.info("DuckLake CHECKPOINT completed")
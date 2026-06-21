import os
import re
import logging

from include.medall_arch.base import BaseLayerManager


class DuckLakeMaintenanceManager(BaseLayerManager):
    """
    Maintenance manager for DuckLake.

    Uses BaseLayerManager to:
      - get the DuckDB Airflow connection
      - set MinIO/Supabase secrets
      - attach DuckLake
    """

    _INTERVAL_PATTERN = re.compile(
        r"^\d+\s+(second|seconds|minute|minutes|hour|hours|day|days|week|weeks|month|months|year|years)$",
        re.IGNORECASE,
    )

    def __init__(
        self,
        LOCAL_DUCKDB_CONN_ID: str,
        DUCKLAKE_NAME: str | None = None,
        DRY_RUN: bool = True,
    ):
        super().__init__(LOCAL_DUCKDB_CONN_ID=LOCAL_DUCKDB_CONN_ID)

        self.DUCKLAKE_NAME = (
            DUCKLAKE_NAME
            or os.getenv("DUCKLAKE_NAME")
            or os.getenv("DBNAME")
        )

        self.DRY_RUN = DRY_RUN

        if not self.DUCKLAKE_NAME:
            raise ValueError(
                "DUCKLAKE_NAME is required. Set DUCKLAKE_NAME or DBNAME in env."
            )

    def _validate_interval(self, interval_value: str) -> None:
        if not self._INTERVAL_PATTERN.match(interval_value):
            raise ValueError(
                f"Invalid interval: {interval_value!r}. "
                "Use values like '1 week', '30 days', or '1 month'."
            )

    def _sql_literal(self, value: str) -> str:
        return "'" + value.replace("'", "''") + "'"

    def _dry_run_sql(self) -> str:
        return "true" if self.DRY_RUN else "false"

    def _execute_maintenance_sql(self, operation_name: str, sql: str):
        logging.info("Starting DuckLake maintenance operation: %s", operation_name)
        logging.info("DuckLake name: %s", self.DUCKLAKE_NAME)
        logging.info("Dry run: %s", self.DRY_RUN)

        self.attach_ducklake()

        logging.info("Executing SQL:\n%s", sql)

        result = self.conn.execute(sql)
        rows = result.fetchall()

        logging.info("%s returned %s rows", operation_name, len(rows))

        for row in rows:
            logging.info("[%s] %s", operation_name, row)

        logging.info("Finished DuckLake maintenance operation: %s", operation_name)

        return rows

    def expire_snapshots(self, older_than: str = "1 month"):
        self._validate_interval(older_than)

        sql = f"""
        CALL ducklake_expire_snapshots(
            {self._sql_literal(self.DUCKLAKE_NAME)},
            dry_run => {self._dry_run_sql()},
            older_than => now() - INTERVAL {self._sql_literal(older_than)}
        );
        """

        return self._execute_maintenance_sql("expire_snapshots", sql)

    def cleanup_old_files(self, older_than: str = "1 week"):
        self._validate_interval(older_than)

        sql = f"""
        CALL ducklake_cleanup_old_files(
            {self._sql_literal(self.DUCKLAKE_NAME)},
            dry_run => {self._dry_run_sql()},
            older_than => now() - INTERVAL {self._sql_literal(older_than)}
        );
        """

        return self._execute_maintenance_sql("cleanup_old_files", sql)

    # def delete_orphaned_files(self, older_than: str = "1 week"):
    #     self._validate_interval(older_than)

    #     sql = f"""
    #     CALL ducklake_delete_orphaned_files(
    #         {self._sql_literal(self.DUCKLAKE_NAME)},
    #         dry_run => {self._dry_run_sql()},
    #         older_than => now() - INTERVAL {self._sql_literal(older_than)}
    #     );
    #     """

        return self._execute_maintenance_sql("delete_orphaned_files", sql)
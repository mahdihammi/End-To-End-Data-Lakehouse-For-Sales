import os
from datetime import datetime

from dotenv import load_dotenv

from airflow.decorators import dag
from airflow.providers.standard.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup

from include.medall_arch.maintenance_layer import DuckLakeMaintenanceManager

load_dotenv()


LOCAL_DUCKDB_CONN_ID = os.environ.get("LOCAL_DUCKDB_CONN_ID")
DUCKLAKE_NAME = os.getenv("DUCKLAKE_NAME") or os.getenv("DBNAME")

DUCKLAKE_MAINTENANCE_DRY_RUN = os.getenv(
    "DUCKLAKE_MAINTENANCE_DRY_RUN",
    "true",
).lower() in {"1", "true", "yes"}

EXPIRE_SNAPSHOTS_OLDER_THAN = os.getenv(
    "DUCKLAKE_EXPIRE_SNAPSHOTS_OLDER_THAN",
    "1 month",
)

CLEANUP_FILES_OLDER_THAN = os.getenv(
    "DUCKLAKE_CLEANUP_FILES_OLDER_THAN",
    "1 week",
)

DELETE_ORPHANED_FILES_OLDER_THAN = os.getenv(
    "DUCKLAKE_DELETE_ORPHANED_FILES_OLDER_THAN",
    "1 week",
)

DUCKLAKE_MAINTENANCE_SCHEDULE = os.getenv(
    "DUCKLAKE_MAINTENANCE_SCHEDULE",
    "@weekly",
)


def get_maintenance_manager() -> DuckLakeMaintenanceManager:
    return DuckLakeMaintenanceManager(
        LOCAL_DUCKDB_CONN_ID=LOCAL_DUCKDB_CONN_ID,
        DUCKLAKE_NAME=DUCKLAKE_NAME,
        DRY_RUN=DUCKLAKE_MAINTENANCE_DRY_RUN,
    )


def expire_snapshots_task():
    manager = get_maintenance_manager()
    return manager.expire_snapshots(
        older_than=EXPIRE_SNAPSHOTS_OLDER_THAN,
    )


def cleanup_old_files_task():
    manager = get_maintenance_manager()
    return manager.cleanup_old_files(
        older_than=CLEANUP_FILES_OLDER_THAN,
    )


def delete_orphaned_files_task():
    manager = get_maintenance_manager()
    return manager.delete_orphaned_files(
        older_than=DELETE_ORPHANED_FILES_OLDER_THAN,
    )


@dag(
    dag_id="ducklake_maintenance_dag",
    start_date=datetime(2026, 1, 1),
    schedule=DUCKLAKE_MAINTENANCE_SCHEDULE,
    catchup=False,
    tags=["ducklake", "maintenance"],
)
def ducklake_maintenance_dag():

    with TaskGroup(group_id="DuckLake_Maintenance") as maintenance_group:

        t_expire_snapshots = PythonOperator(
            task_id="expire_snapshots",
            python_callable=expire_snapshots_task,
        )

        t_cleanup_old_files = PythonOperator(
            task_id="cleanup_old_files",
            python_callable=cleanup_old_files_task,
        )

        # t_delete_orphaned_files = PythonOperator(
        #     task_id="delete_orphaned_files",
        #     python_callable=delete_orphaned_files_task,
        # )

        (
            t_expire_snapshots
            >> t_cleanup_old_files
            # >> t_delete_orphaned_files
        )

    maintenance_group


ducklake_maintenance_dag()
import logging


def _sql_literal(value: str | int | bool | None) -> str:
    if value is None:
        return "NULL"
    if isinstance(value, bool):
        return "true" if value else "false"
    return "'" + str(value).replace("'", "''") + "'"


def _sql_identifier(value: str) -> str:
    if not value:
        raise ValueError("SQL identifier cannot be empty")
    return '"' + value.replace('"', '""') + '"'


def attach_ducklake_and_set_secrets(
    *,
    conn,
    dbname: str,
    supabase_host: str,
    supabase_port: str | int,
    supabase_user: str,
    supabase_pwd: str,
    minio_endpoint: str,
    minio_access_key: str,
    minio_secret_key: str,
    ducklake_name: str,
    data_path: str,
    postgres_secret_name: str = "__default_postgres",
):
    """
    Attach the DuckLake catalog using the current DuckDB connection.

    This function is intentionally connection-local. It is safe for Airflow
    parallel tasks because every task gets its own in-memory DuckDB connection.
    """
    try:
        # conn.execute("INSTALL ducklake;")
        conn.execute("INSTALL postgres;")
        # conn.execute("INSTALL httpfs;")

        # conn.execute("LOAD ducklake;")
        conn.execute("LOAD postgres;")
        # conn.execute("LOAD httpfs;")

        # Make DuckLake commits more tolerant when Airflow has parallel writers.
        conn.execute("SET ducklake_default_data_inlining_row_limit = 0")
        conn.execute("SET ducklake_max_retry_count = 100;")
        conn.execute("SET ducklake_retry_wait_ms = 100;")
        conn.execute("SET ducklake_retry_backoff = 2;")
        

        conn.execute(
            f"""
            CREATE OR REPLACE SECRET minio_secret (
                TYPE s3,
                PROVIDER config,
                KEY_ID {_sql_literal(minio_access_key)},
                SECRET {_sql_literal(minio_secret_key)},
                ENDPOINT {_sql_literal(minio_endpoint)},
                URL_STYLE 'path',
                USE_SSL false,
                SCOPE {_sql_literal(data_path)}
            );
            """
        )

        conn.execute(
            f"""
            CREATE OR REPLACE SECRET {_sql_identifier(postgres_secret_name)} (
                TYPE postgres,
                HOST {_sql_literal(supabase_host)},
                PORT {_sql_literal(supabase_port)},
                DATABASE {_sql_literal(dbname)},
                USER {_sql_literal(supabase_user)},
                PASSWORD {_sql_literal(supabase_pwd)}
            );
            """
        )

        conn.execute(
            f"""
            ATTACH IF NOT EXISTS 'ducklake:postgres:dbname={dbname}'
            AS {_sql_identifier(ducklake_name)}
            (
            DATA_PATH {_sql_literal(data_path)},
            AUTOMATIC_MIGRATION
            );
            """
        )

        logging.info("DuckLake attached successfully as %s", ducklake_name)

    except Exception:
        logging.exception("Cannot attach the DuckLake instance")
        raise

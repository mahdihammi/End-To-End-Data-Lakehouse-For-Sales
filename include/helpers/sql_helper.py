from pathlib import Path

BASE_DIR = Path(__file__).resolve().parents[1]  # include/
SQL_FOLDER = BASE_DIR / "sql"

def load_sql(file_name: str, **kwargs) -> str:
    """
    Load a SQL file from include/sql and return it as a string.
    Optionally format it with keyword arguments.

    Args:
        file_name (str): file name, e.g. 'silver_ddl.sql'
        **kwargs: optional placeholders to format into the SQL

    Returns:
        str: SQL content
    """
    file_path = SQL_FOLDER / file_name
    if not file_path.exists():
        raise FileNotFoundError(f"SQL file not found: {file_path}")
    sql = file_path.read_text()
    return sql.format(**kwargs) if kwargs else sql
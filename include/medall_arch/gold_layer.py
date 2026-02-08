from datetime import datetime
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from include.helpers.sql_helper import load_sql


class GoldTableManager:
    
    def __init__(self, LOCAL_DUCKDB_CONN_ID, GOLD_SCHEMA_NAME, SILVER_SCHEMA_NAME):
        self.my_duck_hook = DuckDBHook.get_hook(LOCAL_DUCKDB_CONN_ID)
        self.conn = self.my_duck_hook.get_conn()
        self.LOCAL_DUCKDB_CONN_ID = LOCAL_DUCKDB_CONN_ID
        self.GOLD_SCHEMA_NAME = GOLD_SCHEMA_NAME
        self.SILVER_SCHEMA_NAME = SILVER_SCHEMA_NAME

    
    def create_dim_customer(self):
        """
        Create dim_customer table in Gold schema from silver data.
        """
        conn = self.conn

        try:
            # Load SQL query from file
            sql_query = load_sql("dim_customer.sql")

            # Execute the SQL to create the dim_customer table
            conn.execute(sql_query)

            print("✅ dim_customer table created successfully in Gold schema.")

            conn.execute("SELECT COUNT(*) FROM gold.dim_customer")
            count = conn.fetchone()[0]
            print(f"   Records in dim_customer: {count:,}")

        except Exception as e:
            print(f"❌ Error creating dim_customer table: {e}")
            raise
        finally:
            conn.close()

    def create_dim_date(self):
        """
        Create dim_date table in Gold schema from silver data.
        """
        conn = self.conn

        try:
            # Load SQL query from file
            sql_query = load_sql("dim_date.sql")

            # Execute the SQL to create the dim_date table
            conn.execute(sql_query)

            print("✅ dim_date table created successfully in Gold schema.")

            conn.execute("SELECT COUNT(*) FROM gold.dim_date")
            count = conn.fetchone()[0]
            print(f"   Records in dim_date: {count:,}")

        except Exception as e:
            print(f"❌ Error creating dim_date table: {e}")
            raise
        finally:
            conn.close()

    def create_dim_location(self):
        """
        Create dim_location table in Gold schema from silver data.
        """
        conn = self.conn

        try:
            # Load SQL query from file
            sql_query = load_sql("dim_location.sql")

            # Execute the SQL to create the dim_location table
            conn.execute(sql_query)

            print("✅ dim_location table created successfully in Gold schema.")

            conn.execute("SELECT COUNT(*) FROM gold.dim_location")
            count = conn.fetchone()[0]
            print(f"   Records in dim_location: {count:,}")

        except Exception as e:
            print(f"❌ Error creating dim_location table: {e}")
            raise
        finally:
            conn.close()

    def create_dim_product(self):
        """
        Create dim_product table in Gold schema from silver data.
        """
        conn = self.conn

        try:
            # Load SQL query from file
            sql_query = load_sql("dim_product.sql")

            # Execute the SQL to create the dim_product table
            conn.execute(sql_query)

            print("✅ dim_product table created successfully in Gold schema.")

            conn.execute("SELECT COUNT(*) FROM gold.dim_product")
            count = conn.fetchone()[0]
            print(f"   Records in dim_product: {count:,}")

        except Exception as e:
            print(f"❌ Error creating dim_product table: {e}")
            raise
        finally:
            conn.close()


    def create_dim_ship_mode(self):
        """
        Create dim_ship_mode table in Gold schema from silver data.
        """
        conn = self.conn

        try:
            # Load SQL query from file
            sql_query = load_sql("dim_ship_mode.sql")

            # Execute the SQL to create the dim_ship_mode table
            conn.execute(sql_query)

            print("✅ dim_ship_mode table created successfully in Gold schema.")

            conn.execute("SELECT COUNT(*) FROM gold.dim_ship_mode")
            count = conn.fetchone()[0]
            print(f"   Records in dim_ship_mode: {count:,}")

        except Exception as e:
            print(f"❌ Error creating dim_ship_mode table: {e}")
            raise
        finally:
            conn.close()


    def create_fact_sales(self):
        """
        Create fact_sales table in Gold schema from silver data.
        """
        conn = self.conn

        try:
            # Load SQL query from file
            sql_query = load_sql("fact_sales.sql")

            # Execute the SQL to create the fact_sales table
            conn.execute(sql_query)

            print("✅ fact_sales table created successfully in Gold schema.")

            conn.execute("SELECT COUNT(*) FROM gold.fact_sales")
            count = conn.fetchone()[0]
            print(f"   Records in fact_sales: {count:,}")

        except Exception as e:
            print(f"❌ Error creating fact_sales table: {e}")
            raise
        finally:
            conn.close()
        
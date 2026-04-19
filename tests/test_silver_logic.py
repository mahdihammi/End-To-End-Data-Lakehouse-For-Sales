# tests/test_silver_logic.py

import pytest
import duckdb
import logging


@pytest.fixture
def conn():
    """In-memory DuckDB connection"""
    connection = duckdb.connect(database=':memory:')
    connection.execute("""
        CREATE TABLE bronze_orders AS SELECT * FROM (VALUES
            ('R001', 'O001', '2024-01-01'::DATE, '2024-01-05'::DATE, 'Standard Class', 'C001', 'Yassine Trabelsi',  'Consumer',    'Tunisia', 'Tunis',    'Tunis',    '1001', 'North',  'P001', 'Office Supplies', 'Pens',    'Pilot G2',   100.00, 2, 0.10,  20.00, '2024-01-01'::TIMESTAMP, '2024-01-01'::DATE),
            ('R002', 'O002', '2024-02-01'::DATE, '2024-01-31'::DATE, 'First Class',    'C002', 'Amira Ben Salah',   'Corporate',   'Tunisia', 'Sfax',     'Sfax',     '3000', 'South',  'P002', 'Technology',      'Phones',  'Samsung A54', 850.00, 1, 0.00, 120.00, '2024-02-01'::TIMESTAMP, '2024-02-01'::DATE),
            ('R003', 'O003', '2024-03-01'::DATE, '2024-03-05'::DATE, 'Same Day',       'C003', 'Mohamed Chaabane', 'Home Office', 'Tunisia', 'Sousse',   'Sousse',   '4000', 'Center', 'P003', 'Furniture',       'Chairs',  'Chaise Ergo', 320.00, 2, 0.05,  -45.00,'2024-03-01'::TIMESTAMP, '2024-03-01'::DATE),
            ('R004', 'O004', '2024-04-01'::DATE, '2024-04-03'::DATE, 'Second Class',   'C001', 'Yassine Trabelsi', 'Consumer',    'Tunisia', 'Tunis',    'Tunis',    '1001', 'North',  'P001', 'Office Supplies', 'Pens',    'Pilot G2',     0.00, 0, 0.00,   0.00, '2024-04-01'::TIMESTAMP, '2024-04-01'::DATE)
        ) t(row_id, order_id, order_date, ship_date, ship_mode, customer_id, customer_name,
            segment, country, city, governorate, postal_code, region, product_id,
            category, sub_category, product_name, sales, quantity, discount, profit,
            updated_at, load_date)
    """)
    return connection


# ---------------------------------------------------------------
# shipping_days
# ---------------------------------------------------------------

def test_shipping_days_positive(conn):
    result = conn.execute("""
        SELECT DATE_DIFF('day', order_date, ship_date) AS shipping_days
        FROM bronze_orders WHERE row_id = 'R001'
    """).fetchone()[0]
    logging.info(f"shipping days positive check :  {result}")
    assert result == 4


def test_shipping_days_negative_flags_invalid(conn):
    """R002 has ship_date before order_date — should be flagged"""
    result = conn.execute("""
        SELECT DATE_DIFF('day', order_date, ship_date) < 0 AS is_invalid
        FROM bronze_orders WHERE row_id = 'R002'
    """).fetchone()[0]
    logging.info("ship_date before order_date — should be flagged")
    print(result)
    assert result == True


# ---------------------------------------------------------------
# profit_status
# ---------------------------------------------------------------

def test_profit_status_profitable(conn):
    result = conn.execute("""
        SELECT CASE
            WHEN profit > 0 THEN 'Profitable'
            WHEN profit = 0 THEN 'Break-even'
            ELSE 'Loss'
        END AS profit_status
        FROM bronze_orders WHERE row_id = 'R001'
    """).fetchone()[0]
    logging.info(f"test_profit_status_profitable : {result}")
    assert result == "Profitable"


def test_profit_status_loss(conn):
    result = conn.execute("""
        SELECT CASE
            WHEN profit > 0 THEN 'Profitable'
            WHEN profit = 0 THEN 'Break-even'
            ELSE 'Loss'
        END AS profit_status
        FROM bronze_orders WHERE row_id = 'R003'
    """).fetchone()[0]
    assert result == "Loss"


def test_profit_status_breakeven(conn):
    result = conn.execute("""
        SELECT CASE
            WHEN profit > 0 THEN 'Profitable'
            WHEN profit = 0 THEN 'Break-even'
            ELSE 'Loss'
        END AS profit_status
        FROM bronze_orders WHERE row_id = 'R004'
    """).fetchone()[0]
    assert result == "Break-even"


# ---------------------------------------------------------------
# unit_price — NULLIF to prevent division by zero
# ---------------------------------------------------------------

def test_unit_price_normal(conn):
    result = conn.execute("""
        SELECT CAST(sales AS DECIMAL(10,2)) / NULLIF(CAST(quantity AS INTEGER), 0) AS unit_price
        FROM bronze_orders WHERE row_id = 'R001'
    """).fetchone()[0]
    assert result == 50.00


def test_unit_price_zero_quantity_returns_null(conn):
    """quantity=0 must return NULL, not crash"""
    result = conn.execute("""
        SELECT CAST(sales AS DECIMAL(10,2)) / NULLIF(CAST(quantity AS INTEGER), 0) AS unit_price
        FROM bronze_orders WHERE row_id = 'R004'
    """).fetchone()[0]
    assert result is None


# ---------------------------------------------------------------
# order_value_tier
# ---------------------------------------------------------------

def test_order_value_tier_high(conn):
    result = conn.execute("""
        SELECT CASE
            WHEN sales >= 200 THEN 'High Value'
            WHEN sales >= 100 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS tier
        FROM bronze_orders WHERE row_id = 'R002'
    """).fetchone()[0]
    assert result == "High Value"


def test_order_value_tier_medium(conn):
    result = conn.execute("""
        SELECT CASE
            WHEN sales >= 200 THEN 'High Value'
            WHEN sales >= 100 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS tier
        FROM bronze_orders WHERE row_id = 'R001'
    """).fetchone()[0]
    assert result == "Medium Value"


def test_order_value_tier_low(conn):
    result = conn.execute("""
        SELECT CASE
            WHEN sales >= 200 THEN 'High Value'
            WHEN sales >= 100 THEN 'Medium Value'
            ELSE 'Low Value'
        END AS tier
        FROM bronze_orders WHERE row_id = 'R004'
    """).fetchone()[0]
    assert result == "Low Value"


# ---------------------------------------------------------------
# ship_mode_priority
# ---------------------------------------------------------------

def test_ship_mode_priority_same_day(conn):
    result = conn.execute("""
        SELECT CASE
            WHEN ship_mode = 'Same Day'       THEN 0
            WHEN ship_mode = 'First Class'    THEN 1
            WHEN ship_mode = 'Second Class'   THEN 2
            WHEN ship_mode = 'Standard Class' THEN 3
            ELSE 4
        END AS priority
        FROM bronze_orders WHERE row_id = 'R003'
    """).fetchone()[0]
    assert result == 0


def test_ship_mode_priority_standard(conn):
    result = conn.execute("""
        SELECT CASE
            WHEN ship_mode = 'Same Day'       THEN 0
            WHEN ship_mode = 'First Class'    THEN 1
            WHEN ship_mode = 'Second Class'   THEN 2
            WHEN ship_mode = 'Standard Class' THEN 3
            ELSE 4
        END AS priority
        FROM bronze_orders WHERE row_id = 'R001'
    """).fetchone()[0]
    assert result == 3


# ---------------------------------------------------------------
# has_missing_financial_data
# ---------------------------------------------------------------

def test_has_missing_financial_data_false(conn):
    result = conn.execute("""
        SELECT CASE
            WHEN sales IS NULL OR quantity IS NULL OR profit IS NULL THEN TRUE
            ELSE FALSE
        END AS has_missing
        FROM bronze_orders WHERE row_id = 'R001'
    """).fetchone()[0]
    assert result == False
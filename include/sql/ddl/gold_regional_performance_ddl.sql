-- ddl/gold_regional_performance_ddl.sql
CREATE TABLE IF NOT EXISTS {full_table_name} (
    region                      VARCHAR,
    governorate                 VARCHAR,
    order_year                  INTEGER,
    order_month                 INTEGER,
    total_orders                BIGINT,
    unique_customers            BIGINT,
    total_revenue               DECIMAL(10,2),
    total_profit                DECIMAL(10,2),
    avg_profit_margin           DECIMAL(10,4),
    avg_shipping_days           DECIMAL(5,2),
    high_value_orders           BIGINT,
    medium_value_orders         BIGINT,
    low_value_orders            BIGINT,
    consumer_orders             BIGINT,
    corporate_orders            BIGINT,
    home_office_orders          BIGINT,
    load_date                   DATE,
    updated_at                  TIMESTAMP
);
CREATE TABLE IF NOT EXISTS {full_table_name} (
    row_id          UUID,
    order_id        UUID,
    order_date      TIMESTAMP,
    ship_date       TIMESTAMP,
    ship_mode       VARCHAR(20),
    customer_id     VARCHAR(12),
    customer_name   VARCHAR(120),
    segment         VARCHAR(20),
    country         VARCHAR(50),
    city            VARCHAR(80),
    governorate     VARCHAR(80),
    postal_code     CHAR(4),
    region          VARCHAR(30),
    product_id      VARCHAR(16),
    category        VARCHAR(30),
    sub_category    VARCHAR(30),
    product_name    VARCHAR(120),
    sales           NUMERIC(10, 2),
    quantity        SMALLINT,
    discount        NUMERIC(4, 2),
    profit          NUMERIC(10, 2),
    updated_at      TIMESTAMP,
    load_date       DATE
);

ALTER TABLE {full_table_name}
SET PARTITIONED BY (load_date);
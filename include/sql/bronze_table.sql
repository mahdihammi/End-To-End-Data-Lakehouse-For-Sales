MERGE INTO mahdi_ducklake.bronze.orders_bronze AS b
USING (
  select * from read_parquet('s3://lakehouse-project/lakehouse-raw/sales/load_date=*/*.parquet')
) AS s
ON b.row_id = s.row_id

WHEN MATCHED
    AND s.updated_at > b.updated_at
THEN UPDATE SET
    order_id      = s.order_id,
    order_date    = s.order_date,
    ship_date     = s.ship_date,
    ship_mode     = s.ship_mode,
    customer_id   = s.customer_id,
    customer_name = s.customer_name,
    segment       = s.segment,
    country       = s.country,
    city          = s.city,
    governorate   = s.governorate,
    postal_code   = s.postal_code,
    region        = s.region,
    product_id    = s.product_id,
    category      = s.category,
    sub_category  = s.sub_category,
    product_name  = s.product_name,
    sales         = s.sales,
    quantity      = s.quantity,
    discount      = s.discount,
    profit        = s.profit,
    updated_at    = s.updated_at

WHEN NOT MATCHED THEN INSERT (
    row_id,
    order_id,
    order_date,
    ship_date,
    ship_mode,
    customer_id,
    customer_name,
    segment,
    country,
    city,
    governorate,
    postal_code,
    region,
    product_id,
    category,
    sub_category,
    product_name,
    sales,
    quantity,
    discount,
    profit,
    updated_at
)
VALUES (
    s.row_id,
    s.order_id,
    s.order_date,
    s.ship_date,
    s.ship_mode,
    s.customer_id,
    s.customer_name,
    s.segment,
    s.country,
    s.city,
    s.governorate,
    s.postal_code,
    s.region,
    s.product_id,
    s.category,
    s.sub_category,
    s.product_name,
    s.sales,
    s.quantity,
    s.discount,
    s.profit,
    s.updated_at
);

MERGE INTO mahdi_ducklake.gold.customer_360 AS tgt
USING (
    SELECT
        customer_id,
        customer_name,
        segment,
        region,
        governorate,

        COUNT(DISTINCT order_id)                                                AS total_orders,
        MIN(order_date)                                                         AS first_order_date,
        MAX(order_date)                                                         AS last_order_date,
        DATE_DIFF('day', MIN(order_date), MAX(order_date))                      AS customer_lifespan_days,
        CAST(SUM(sales) AS DECIMAL(10,2))                                       AS total_spent,
        CAST(AVG(sales) AS DECIMAL(10,2))                                       AS avg_order_value,
        CAST(SUM(profit) AS DECIMAL(10,2))                                      AS total_profit_generated,
        CAST(AVG(discount) AS DECIMAL(5,2))                                     AS avg_discount_used,
        SUM(quantity)                                                           AS total_units_bought,
        COUNT(DISTINCT sub_category)                                            AS product_variety,

        CASE
            WHEN SUM(sales) >= 10000 THEN 'VIP'
            WHEN SUM(sales) >= 5000  THEN 'High Value'
            WHEN SUM(sales) >= 1000  THEN 'Mid Value'
            ELSE                          'Low Value'
        END                                                                     AS customer_value_tier,

        CASE
            WHEN COUNT(DISTINCT order_id) >= 10 THEN 'Champion'
            WHEN COUNT(DISTINCT order_id) >= 5  THEN 'Loyal'
            WHEN COUNT(DISTINCT order_id) >= 2  THEN 'Returning'
            ELSE                                     'One-time'
        END                                                                     AS customer_loyalty_tier,

        MAX(load_date)                                                          AS load_date,
        MAX(updated_at)                                                         AS updated_at

    FROM mahdi_ducklake.silver.orders_silver
    GROUP BY 1,2,3,4,5

) AS src

ON tgt.customer_id = src.customer_id

WHEN MATCHED AND src.updated_at > tgt.updated_at THEN
    UPDATE SET
        customer_name           = src.customer_name,
        segment                 = src.segment,
        region                  = src.region,
        governorate             = src.governorate,
        total_orders            = src.total_orders,
        first_order_date        = src.first_order_date,
        last_order_date         = src.last_order_date,
        customer_lifespan_days  = src.customer_lifespan_days,
        total_spent             = src.total_spent,
        avg_order_value         = src.avg_order_value,
        total_profit_generated  = src.total_profit_generated,
        avg_discount_used       = src.avg_discount_used,
        total_units_bought      = src.total_units_bought,
        product_variety         = src.product_variety,
        customer_value_tier     = src.customer_value_tier,
        customer_loyalty_tier   = src.customer_loyalty_tier,
        load_date               = src.load_date,
        updated_at              = src.updated_at

WHEN NOT MATCHED THEN
    INSERT (
        customer_id, customer_name, segment, region, governorate,
        total_orders, first_order_date, last_order_date, customer_lifespan_days,
        total_spent, avg_order_value, total_profit_generated, avg_discount_used,
        total_units_bought, product_variety, customer_value_tier,
        customer_loyalty_tier, load_date, updated_at
    )
    VALUES (
        src.customer_id, src.customer_name, src.segment, src.region, src.governorate,
        src.total_orders, src.first_order_date, src.last_order_date, src.customer_lifespan_days,
        src.total_spent, src.avg_order_value, src.total_profit_generated, src.avg_discount_used,
        src.total_units_bought, src.product_variety, src.customer_value_tier,
        src.customer_loyalty_tier, src.load_date, src.updated_at
    );
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
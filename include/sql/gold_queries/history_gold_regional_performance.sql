SELECT
        region,
        governorate,
        order_year,
        order_month,

        COUNT(DISTINCT order_id)                                                AS total_orders,
        COUNT(DISTINCT customer_id)                                             AS unique_customers,
        CAST(SUM(sales) AS DECIMAL(10,2))                                       AS total_revenue,
        CAST(SUM(profit) AS DECIMAL(10,2))                                      AS total_profit,
        CAST(AVG(profit_margin) AS DECIMAL(10,4))                               AS avg_profit_margin,
        CAST(AVG(shipping_days) AS DECIMAL(5,2))                                AS avg_shipping_days,

        SUM(CASE WHEN order_value_tier = 'High Value'   THEN 1 ELSE 0 END)     AS high_value_orders,
        SUM(CASE WHEN order_value_tier = 'Medium Value' THEN 1 ELSE 0 END)     AS medium_value_orders,
        SUM(CASE WHEN order_value_tier = 'Low Value'    THEN 1 ELSE 0 END)     AS low_value_orders,

        SUM(CASE WHEN segment = 'Consumer'    THEN 1 ELSE 0 END)               AS consumer_orders,
        SUM(CASE WHEN segment = 'Corporate'   THEN 1 ELSE 0 END)               AS corporate_orders,
        SUM(CASE WHEN segment = 'Home Office' THEN 1 ELSE 0 END)               AS home_office_orders,

        MAX(load_date)                                                          AS load_date,
        MAX(updated_at)                                                         AS updated_at

    FROM mahdi_ducklake.silver.orders_silver
    GROUP BY 1,2,3,4